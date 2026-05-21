#include "reclaimnet/migration_client.h"
#include "reclaimnet/security.h"
#include "reclaimnet/time.h"
#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <netdb.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <poll.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#ifdef __linux__
#include <pthread.h>
#include <signal.h>
#endif
namespace reclaimnet::agent {
namespace {
constexpr size_t block_size = 65536;
struct SigpipeGuard {
#ifdef __linux__
  sigset_t signals{}, previous{};
  bool already_pending = false;
  SigpipeGuard() {
    sigemptyset(&signals);
    sigaddset(&signals, SIGPIPE);
    sigset_t pending;
    sigpending(&pending);
    already_pending = sigismember(&pending, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &signals, &previous);
  }
  ~SigpipeGuard() {
    if (!already_pending) {
      timespec timeout{};
      sigtimedwait(&signals, nullptr, &timeout);
    }
    pthread_sigmask(SIG_SETMASK, &previous, nullptr);
  }
#endif
};
using Context = std::shared_ptr<SSL_CTX>;
Result<Context> make_context(const MigrationTlsConfig &config, bool server) {
  return make_tls13_context(
      {config.cert_path, config.key_path, config.ca_path, server});
}
VoidResult ready(int fd, bool write, int64_t deadline,
                 const std::atomic<bool> *cancel) {
  while (true) {
    if (cancel && cancel->load())
      return Error{"Migration cancelled"};
    const auto remaining = deadline - now_ns();
    if (remaining <= 0)
      return Error{"Migration deadline exceeded"};
    pollfd event{fd, static_cast<short>(write ? POLLOUT : POLLIN), 0};
    int result = poll(&event, 1,
                      static_cast<int>(std::min<int64_t>(
                          100, (remaining + 999999) / 1000000)));
    if (result > 0)
      return ok_void();
    if (result < 0 && errno != EINTR)
      return Error{"Migration socket poll failed"};
  }
}
struct Channel {
  int fd = -1;
  SSL *ssl = nullptr;
  int64_t deadline = 0;
  const std::atomic<bool> *cancel = nullptr;
  ~Channel() {
    if (ssl)
      SSL_free(ssl);
    if (fd >= 0)
      close(fd);
  }
  VoidResult handshake() {
    while (true) {
      if (cancel && cancel->load())
        return Error{"Migration cancelled"};
      if (now_ns() >= deadline)
        return Error{"Migration TLS deadline exceeded"};
      int result = SSL_do_handshake(ssl);
      if (result == 1)
        return SSL_get_verify_result(ssl) == X509_V_OK
                   ? ok_void()
                   : err_void("Migration peer certificate verification failed");
      int error = SSL_get_error(ssl, result);
      if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE)
        return Error{"Migration mTLS handshake failed"};
      if (auto r = ready(fd, error == SSL_ERROR_WANT_WRITE, deadline, cancel);
          !r)
        return r;
    }
  }
  VoidResult io(char *bytes, size_t length, bool write) {
    size_t position = 0;
    while (position < length) {
      if (cancel && cancel->load())
        return Error{"Migration cancelled"};
      if (now_ns() >= deadline)
        return Error{"Migration deadline exceeded"};
      int count =
          static_cast<int>(std::min<size_t>(block_size, length - position));
      int result = write ? SSL_write(ssl, bytes + position, count)
                         : SSL_read(ssl, bytes + position, count);
      if (result > 0) {
        position += static_cast<size_t>(result);
        continue;
      }
      int error = SSL_get_error(ssl, result);
      if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE)
        return Error{"Migration TLS stream closed before completion"};
      if (auto r = ready(fd, error == SSL_ERROR_WANT_WRITE, deadline, cancel);
          !r)
        return r;
    }
    return ok_void();
  }
  VoidResult write(const std::string &value) {
    return io(const_cast<char *>(value.data()), value.size(), true);
  }
};
struct FlowGuard {
  FiveTuple flow;
  std::function<void(const FiveTuple &)> finished;
  ~FlowGuard() {
    if (finished)
      finished(flow);
  }
};
void socket_options(int fd) {
  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
#ifdef SO_NOSIGPIPE
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
}
Result<sockaddr_in> route_source(const sockaddr_in &remote) {
  Channel route;
  route.fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (route.fd < 0)
    return Error{"Migration route socket failed"};
  if (connect(route.fd, reinterpret_cast<const sockaddr *>(&remote),
              sizeof(remote)) < 0)
    return Error{"Migration source route is unavailable"};
  sockaddr_in local{};
  socklen_t size = sizeof(local);
  if (getsockname(route.fd, reinterpret_cast<sockaddr *>(&local), &size) < 0 ||
      local.sin_addr.s_addr == htonl(INADDR_ANY))
    return Error{"Migration source address is unavailable"};
  local.sin_port = 0;
  return local;
}
struct Encoder {
  std::string data;
  void number(uint64_t value, size_t bytes = 8) {
    for (size_t i = 0; i < bytes; ++i)
      data.push_back(static_cast<char>((value >> (i * 8)) & 255));
  }
  void string(const std::string &value) {
    number(value.size(), 4);
    data += value;
  }
  void strings(const std::vector<std::string> &values) {
    number(values.size(), 4);
    for (const auto &value : values)
      string(value);
  }
};
struct Decoder {
  const std::string &data;
  size_t position = 0;
  uint64_t number(size_t bytes = 8) {
    if (bytes > data.size() - position)
      throw std::runtime_error("Truncated migration metadata");
    uint64_t value = 0;
    for (size_t i = 0; i < bytes; ++i)
      value |= uint64_t(static_cast<unsigned char>(data[position++]))
               << (i * 8);
    return value;
  }
  std::string string() {
    auto length = number(4);
    if (length > data.size() - position)
      throw std::runtime_error("Truncated migration string");
    auto value = data.substr(position, length);
    position += length;
    return value;
  }
  std::vector<std::string> strings() {
    auto count = number(4);
    std::vector<std::string> values;
    for (uint64_t i = 0; i < count; ++i)
      values.push_back(string());
    return values;
  }
};
void encode_spec(Encoder &out, const JobRuntimeSpec &spec) {
  out.string(spec.job_id);
  out.string(spec.state_path);
  out.string(spec.backend);
  out.string(spec.application_module);
  out.string(spec.bridge_directory);
  out.string(spec.container_name);
  out.string(spec.container_image);
  out.string(spec.gpu_devices);
  out.string(spec.seccomp_profile);
  out.number(spec.memory_limit_bytes);
  out.number(spec.state_directory, 1);
  out.number(spec.defer_start, 1);
  out.strings(spec.container_args);
  out.strings(spec.checkpoint_command);
  out.strings(spec.restore_command);
  out.strings(spec.start_command);
  out.strings(spec.pause_command);
  out.strings(spec.resume_command);
  out.strings(spec.stop_command);
}
JobRuntimeSpec decode_spec(Decoder &in) {
  JobRuntimeSpec s;
  s.job_id = in.string();
  s.state_path = in.string();
  s.backend = in.string();
  s.application_module = in.string();
  s.bridge_directory = in.string();
  s.container_name = in.string();
  s.container_image = in.string();
  s.gpu_devices = in.string();
  s.seccomp_profile = in.string();
  s.memory_limit_bytes = in.number();
  s.state_directory = in.number(1);
  s.defer_start = in.number(1);
  s.container_args = in.strings();
  s.checkpoint_command = in.strings();
  s.restore_command = in.strings();
  s.start_command = in.strings();
  s.pause_command = in.strings();
  s.resume_command = in.strings();
  s.stop_command = in.strings();
  return s;
}
VoidResult send_message(Channel &channel, const std::string &data) {
  Encoder size;
  size.number(data.size(), 4);
  if (auto r = channel.write(size.data); !r)
    return r;
  return channel.write(data);
}
Result<std::string> read_message(Channel &channel) {
  std::string header(4, 0);
  if (auto r = channel.io(header.data(), header.size(), false); !r)
    return r.error();
  Decoder decoder{header};
  auto size = decoder.number(4);
  if (size > 16777216)
    return Error{"Migration metadata exceeds protocol frame size"};
  std::string data(size, 0);
  if (auto r = channel.io(data.data(), size, false); !r)
    return r.error();
  return data;
}
VoidResult ack(Channel &channel, const CheckpointMeta &meta,
               const VoidResult &result) {
  Encoder out;
  out.number(static_cast<bool>(result), 1);
  out.string(meta.job_id);
  out.string(meta.sha256);
  out.string(result ? "" : result.error().message);
  return send_message(channel, out.data);
}
}
MigrationClient::MigrationClient(MigrationTlsConfig tls)
    : tls_(std::move(tls)) {}
VoidResult MigrationClient::send(const MigrationParams &params,
                                 const TransferProgressCb &progress,
                                 std::atomic<bool> &cancel) {
  SigpipeGuard sigpipe;
  if (!std::isfinite(params.allocated_bps) || params.allocated_bps <= 0.0)
    return Error{"An admitted positive migration rate is required"};
  int64_t deadline = params.deadline_ns;
  if (deadline == 0 && std::isfinite(params.deadline_s) &&
      params.deadline_s > 0)
    deadline = now_ns() + static_cast<int64_t>(params.deadline_s * 1e9);
  if (deadline <= now_ns())
    return Error{"Migration deadline has expired"};
  auto context = make_context(tls_, false);
  if (!context)
    return context.error();
  CheckpointMeta meta = params.checkpoint;
  if (meta.job_id != params.job_id || !meta.complete ||
      meta.sha256.size() != 64)
    return Error{"Migration requires a committed checkpoint manifest"};
  auto hash = checkpoint_sha256(meta.path);
  if (!hash)
    return hash.error();
  if (hash.value() != meta.sha256 ||
      std::filesystem::file_size(meta.path) != meta.size_bytes)
    return Error{"Source checkpoint changed after commit"};
  addrinfo hints{}, *addresses = nullptr;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo(params.dst_host.c_str(),
                  std::to_string(params.dst_port).c_str(), &hints, &addresses))
    return Error{"Migration destination resolution failed"};
  const auto remote =
      *reinterpret_cast<const sockaddr_in *>(addresses->ai_addr);
  freeaddrinfo(addresses);
  auto source = route_source(remote);
  if (!source)
    return source.error();
  Channel channel;
  channel.deadline = deadline;
  channel.cancel = &cancel;
  channel.fd = socket(AF_INET, SOCK_STREAM, 0);
  if (channel.fd < 0)
    return Error{"Migration socket failed"};
  socket_options(channel.fd);
  auto local = source.value();
  if (bind(channel.fd, reinterpret_cast<const sockaddr *>(&local),
           sizeof(local)) < 0)
    return Error{"Migration source bind failed"};
  socklen_t address_size = sizeof(local);
  if (getsockname(channel.fd, reinterpret_cast<sockaddr *>(&local),
                  &address_size) < 0)
    return Error{"Migration source endpoint is unavailable"};
  FlowGuard flow{{ntohl(local.sin_addr.s_addr), ntohl(remote.sin_addr.s_addr),
                  ntohs(local.sin_port), ntohs(remote.sin_port), IPPROTO_TCP},
                 params.flow_finished};
  if (params.flow_started) {
    if (auto result = params.flow_started(flow.flow); !result)
      return result;
  }
  int connected = connect(
      channel.fd, reinterpret_cast<const sockaddr *>(&remote), sizeof(remote));
  if (connected < 0 && errno != EINPROGRESS)
    return Error{"Migration connect failed"};
  if (connected < 0) {
    if (auto r = ready(channel.fd, true, deadline, &cancel); !r)
      return r;
    int error = 0;
    socklen_t size = sizeof(error);
    getsockopt(channel.fd, SOL_SOCKET, SO_ERROR, &error, &size);
    if (error)
      return Error{"Migration connect failed"};
  }
  channel.ssl = SSL_new(context.value().get());
  SSL_set_fd(channel.ssl, channel.fd);
  SSL_set_connect_state(channel.ssl);
  auto host = tls_.server_name.empty() ? params.dst_host : tls_.server_name;
  in_addr ip{};
  if (inet_pton(AF_INET, host.c_str(), &ip) == 1)
    X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(channel.ssl), host.c_str());
  else {
    X509_VERIFY_PARAM_set1_host(SSL_get0_param(channel.ssl), host.c_str(),
                                host.size());
    SSL_set_tlsext_host_name(channel.ssl, host.c_str());
  }
  if (auto r = channel.handshake(); !r)
    return r;
  Encoder manifest;
  manifest.string("RNCKP002");
  manifest.string(meta.job_id);
  manifest.number(meta.sequence, 4);
  manifest.number(meta.size_bytes);
  manifest.number(meta.timestamp_ns);
  manifest.string(meta.sha256);
  manifest.number(deadline);
  manifest.number(params.restore_after_transfer, 1);
  manifest.string(params.transfer_id);
  manifest.number(params.rank, 4);
  manifest.number(params.destination_runtime.has_value(), 1);
  if (params.destination_runtime)
    encode_spec(manifest, *params.destination_runtime);
  if (auto r = send_message(channel, manifest.data); !r)
    return r;
  auto response = read_message(channel);
  if (!response)
    return response.error();
  uint64_t offset;
  try {
    Decoder resume{response.value()};
    offset = resume.number();
  } catch (const std::exception &e) {
    return Error{e.what()};
  }
  if (offset > meta.size_bytes)
    return Error{"Invalid migration resume offset"};
  std::ifstream input(meta.path, std::ios::binary);
  input.seekg(offset);
  uint64_t sent = offset;
  if (progress)
    progress(sent);
  std::array<char, block_size> buffer;
  auto replenished = std::chrono::steady_clock::now();
  double tokens = 0;
  while (sent < meta.size_bytes) {
    size_t count = static_cast<size_t>(
        std::min<uint64_t>(buffer.size(), meta.size_bytes - sent));
    input.read(buffer.data(), count);
    if (static_cast<size_t>(input.gcount()) != count)
      return Error{"Source checkpoint read failed"};
    while (tokens < static_cast<double>(count)) {
      if (cancel.load())
        return Error{"Migration cancelled"};
      if (now_ns() >= deadline)
        return Error{"Migration deadline exceeded"};
      double rate =
          params.rate_provider ? params.rate_provider() : params.allocated_bps;
      if (!std::isfinite(rate) || rate < 0)
        return Error{"Invalid updated migration rate"};
      auto current = std::chrono::steady_clock::now();
      double elapsed =
          std::chrono::duration<double>(current - replenished).count();
      replenished = current;
      tokens = std::min(tokens + elapsed * rate / 8.0,
                        std::max(static_cast<double>(count), rate / 80.0));
      if (tokens >= static_cast<double>(count))
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    tokens -= static_cast<double>(count);
    if (auto r = channel.io(buffer.data(), count, true); !r)
      return r;
    sent += count;
    if (progress)
      progress(sent);
  }
  auto acknowledgement = read_message(channel);
  if (!acknowledgement)
    return acknowledgement.error();
  try {
    Decoder in{acknowledgement.value()};
    auto success = in.number(1);
    auto job = in.string(), checksum = in.string(), message = in.string();
    if (!success)
      return Error{"Destination restore/commit failed: " + message};
    if (job != meta.job_id || checksum != meta.sha256)
      return Error{"Migration ACK manifest mismatch"};
  } catch (const std::exception &error) {
    return Error{error.what()};
  }
  return ok_void();
}
struct MigrationServer::Impl {
  MigrationTlsConfig tls;
  CheckpointManager &checkpoints;
  MigrationReceiveProgressCb progress;
  MigrationCommittedCb committed;
  MigrationValidateCb validate;
  Context context;
  std::atomic<bool> running{false};
  std::atomic<bool> cancel{false};
  int listener = -1;
  uint16_t bound_port = 0;
  std::thread worker;
  struct Connection {
    std::thread worker;
    std::shared_ptr<std::atomic<bool>> finished;
  };
  std::mutex workers_mutex;
  std::vector<Connection> connections;
  std::mutex transfers_mutex;
  std::unordered_map<JobId, std::shared_ptr<std::mutex>> transfer_locks;
  Impl(MigrationTlsConfig config, CheckpointManager &manager,
       MigrationReceiveProgressCb p, MigrationCommittedCb c,
       MigrationValidateCb v)
      : tls(std::move(config)), checkpoints(manager), progress(std::move(p)),
        committed(std::move(c)), validate(std::move(v)) {}
  VoidResult receive(Channel &channel) {
    auto message = read_message(channel);
    if (!message)
      return message.error();
    CheckpointMeta meta;
    bool restoring;
    std::optional<JobRuntimeSpec> runtime;
    try {
      Decoder in{message.value()};
      if (in.string() != "RNCKP002")
        return Error{"Unknown migration protocol"};
      meta.job_id = in.string();
      meta.sequence = in.number(4);
      meta.size_bytes = in.number();
      meta.timestamp_ns = static_cast<int64_t>(in.number());
      meta.sha256 = in.string();
      meta.complete = true;
      channel.deadline = static_cast<int64_t>(in.number());
      restoring = in.number(1);
      meta.transfer_id = in.string();
      meta.rank = in.number(4);
      if (in.number(1))
        runtime = decode_spec(in);
      if (in.position != message.value().size())
        return Error{"Trailing migration metadata"};
    } catch (const std::exception &error) {
      return Error{error.what()};
    }
    auto path = checkpoints.incoming_path(meta.job_id, meta.sequence);
    if (path.empty() || meta.sha256.size() != 64)
      return Error{"Invalid checkpoint manifest"};
    std::shared_ptr<std::mutex> job_mutex;
    {
      std::lock_guard<std::mutex> lock(transfers_mutex);
      auto &slot = transfer_locks[meta.job_id];
      if (!slot)
        slot = std::make_shared<std::mutex>();
      job_mutex = slot;
    }
    std::lock_guard<std::mutex> transfer_lock(*job_mutex);
    if (validate) {
      X509 *peer = SSL_get_peer_certificate(channel.ssl);
      if (!peer)
        return Error{"Migration peer certificate is required"};
      auto subject = X509_get_subject_name(peer);
      int index = X509_NAME_get_index_by_NID(subject, NID_commonName, -1);
      if (index < 0) {
        X509_free(peer);
        return Error{"Migration peer node identity is required"};
      }
      auto entry = X509_NAME_get_entry(subject, index);
      unsigned char *text = nullptr;
      int length = ASN1_STRING_to_UTF8(&text, X509_NAME_ENTRY_get_data(entry));
      std::string name;
      if (length > 0)
        name.assign(reinterpret_cast<const char *>(text), length);
      OPENSSL_free(text);
      X509_free(peer);
      if (name.empty())
        return Error{"Migration peer node identity is required"};
      if (auto result = validate(meta, restoring, name); !result)
        return result;
    }
    if (restoring && !checkpoints.runtime().spec(meta.job_id)) {
      if (!runtime || runtime->job_id != meta.job_id)
        return Error{"Destination runtime is not prepared"};
      if (auto result = checkpoints.runtime().register_job(*runtime); !result)
        return result;
    }
    uint64_t offset = 0;
    auto existing = checkpoints.latest_meta(meta.job_id);
    bool already_committed = existing &&
                             existing.value().sequence == meta.sequence &&
                             existing.value().sha256 == meta.sha256;
    if (already_committed) {
      auto hash = checkpoint_sha256(existing.value().path);
      if (!hash)
        return hash.error();
      if (hash.value() != meta.sha256 ||
          std::filesystem::file_size(existing.value().path) != meta.size_bytes)
        return Error{"Committed checkpoint integrity failed"};
    }
    if (already_committed)
      offset = meta.size_bytes;
    else {
      std::ifstream identity(path + ".identity");
      std::string previous;
      identity >> previous;
      if (previous == meta.sha256 && std::filesystem::exists(path)) {
        offset = std::filesystem::file_size(path);
        if (offset > meta.size_bytes)
          offset = 0;
      }
      if (offset == 0) {
        std::ofstream truncate(path, std::ios::binary | std::ios::trunc);
      }
      std::ofstream identity_out(path + ".identity", std::ios::trunc);
      identity_out << meta.sha256;
      identity_out.close();
      if (auto r = checkpoint_sync_file(path + ".identity"); !r)
        return r;
    }
    Encoder resume;
    resume.number(offset);
    if (auto r = send_message(channel, resume.data); !r)
      return r;
    if (!already_committed) {
      std::ofstream output(path, std::ios::binary | std::ios::app);
      if (!output)
        return Error{"Cannot create incoming checkpoint"};
      std::array<char, block_size> buffer;
      uint64_t received = offset;
      while (received < meta.size_bytes) {
        auto count = static_cast<size_t>(
            std::min<uint64_t>(buffer.size(), meta.size_bytes - received));
        if (auto r = channel.io(buffer.data(), count, false); !r) {
          output.flush();
          output.close();
          checkpoint_sync_file(path);
          return r;
        }
        output.write(buffer.data(), count);
        if (!output)
          return Error{"Incoming checkpoint disk write failed"};
        received += count;
        if (progress)
          progress(meta.job_id, received);
      }
      output.flush();
      if (!output)
        return Error{"Incoming checkpoint flush failed"};
      output.close();
      if (auto r = checkpoints.commit_received(meta, path); !r) {
        ack(channel, meta, r);
        return r;
      }
      std::filesystem::remove(path + ".identity");
    }
    VoidResult result = ok_void();
    if (restoring)
      result =
          checkpoints.restore_checkpoint(meta.job_id, {}, channel.deadline);
    auto final = checkpoints.latest_meta(meta.job_id);
    if (!final)
      result = final.error();
    if (result && committed) {
      final.value().transfer_id = meta.transfer_id;
      final.value().rank = meta.rank;
      if (now_ns() >= channel.deadline)
        result = Error{"Migration restore deadline exceeded"};
      else
        result = committed(final.value(), restoring);
    }
    auto sent = ack(channel, meta, result);
    return result ? sent : result;
  }
  void loop() {
    SigpipeGuard sigpipe;
    while (running.load()) {
      {
        std::lock_guard<std::mutex> lock(workers_mutex);
        auto connection = connections.begin();
        while (connection != connections.end()) {
          if (connection->finished->load()) {
            connection->worker.join();
            connection = connections.erase(connection);
          } else
            ++connection;
        }
      }
      pollfd event{listener, POLLIN, 0};
      if (poll(&event, 1, 100) <= 0)
        continue;
      int fd = accept(listener, nullptr, nullptr);
      if (fd < 0)
        continue;
      std::lock_guard<std::mutex> lock(workers_mutex);
      auto finished = std::make_shared<std::atomic<bool>>(false);
      std::thread connection([this, fd, finished] {
        SigpipeGuard sigpipe;
        Channel channel;
        channel.fd = fd;
        channel.cancel = &cancel;
        channel.deadline =
            now_ns() + static_cast<int64_t>(tls.handshake_timeout_s * 1e9);
        socket_options(fd);
        channel.ssl = SSL_new(context.get());
        SSL_set_fd(channel.ssl, fd);
        SSL_set_accept_state(channel.ssl);
        try {
          if (channel.handshake())
            receive(channel);
        } catch (const std::exception &) {
        }
        finished->store(true);
      });
      connections.push_back({std::move(connection), std::move(finished)});
    }
  }
};
MigrationServer::MigrationServer(MigrationTlsConfig tls,
                                 CheckpointManager &checkpoints,
                                 MigrationReceiveProgressCb progress,
                                 MigrationCommittedCb committed,
                                 MigrationValidateCb validate)
    : impl_(std::make_unique<Impl>(std::move(tls), checkpoints,
                                   std::move(progress), std::move(committed),
                                   std::move(validate))) {}
MigrationServer::~MigrationServer() { stop(); }
VoidResult MigrationServer::start(uint16_t port,
                                  const std::string &bind_address) {
  if (impl_->running)
    return Error{"Migration receiver is already running"};
  auto context = make_context(impl_->tls, true);
  if (!context)
    return context.error();
  impl_->context = context.value();
  impl_->listener = socket(AF_INET, SOCK_STREAM, 0);
  if (impl_->listener < 0)
    return Error{"Migration listener socket failed"};
  int one = 1;
  setsockopt(impl_->listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (inet_pton(AF_INET, bind_address.c_str(), &address.sin_addr) != 1 ||
      bind(impl_->listener, reinterpret_cast<sockaddr *>(&address),
           sizeof(address)) < 0 ||
      listen(impl_->listener, 16) < 0) {
    close(impl_->listener);
    impl_->listener = -1;
    return Error{"Migration listener bind failed"};
  }
  socklen_t size = sizeof(address);
  getsockname(impl_->listener, reinterpret_cast<sockaddr *>(&address), &size);
  impl_->bound_port = ntohs(address.sin_port);
  impl_->cancel = false;
  impl_->running = true;
  impl_->worker = std::thread([this] { impl_->loop(); });
  return ok_void();
}
void MigrationServer::stop() {
  impl_->running = false;
  impl_->cancel = true;
  if (impl_->worker.joinable())
    impl_->worker.join();
  for (auto &connection : impl_->connections)
    if (connection.worker.joinable())
      connection.worker.join();
  impl_->connections.clear();
  if (impl_->listener >= 0) {
    close(impl_->listener);
    impl_->listener = -1;
  }
}
uint16_t MigrationServer::port() const { return impl_->bound_port; }
}
