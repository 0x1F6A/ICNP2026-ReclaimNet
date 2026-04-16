


#include "reclaimnet/migration_client.h"
#include "reclaimnet/log.h"
#include "reclaimnet/time.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <fstream>
#include <vector>
#include <thread>
#include <chrono>

namespace reclaimnet {
namespace agent {



VoidResult MigrationClient::send_all(int fd, const char* buf, size_t n) {
  size_t sent = 0;
  while (sent < n) {
    ssize_t rc = ::send(fd, buf + sent, n - sent, MSG_NOSIGNAL);
    if (rc <= 0) {
      return Error{std::string("send() failed: ") + strerror(errno)};
    }
    sent += (size_t)rc;
  }
  return ok_void();
}

VoidResult MigrationClient::recv_all(int fd, char* buf, size_t n) {
  size_t got = 0;
  while (got < n) {
    ssize_t rc = ::recv(fd, buf + got, n - got, 0);
    if (rc <= 0) {
      return Error{std::string("recv() failed: ") + strerror(errno)};
    }
    got += (size_t)rc;
  }
  return ok_void();
}




VoidResult MigrationClient::send_block(int fd, uint32_t seq,
                                        const char* data, size_t len) {

  char hdr[16];
  uint32_t magic_le = htole32(MAGIC);
  uint32_t seq_le   = htole32(seq);
  uint64_t len_le   = htole64((uint64_t)len);
  memcpy(hdr,     &magic_le, 4);
  memcpy(hdr + 4, &seq_le,   4);
  memcpy(hdr + 8, &len_le,   8);

  if (auto r = send_all(fd, hdr, 16); !r) return r;
  if (auto r = send_all(fd, data, len); !r) return r;
  return ok_void();
}



VoidResult MigrationClient::send(const MigrationParams& params,
                                  const TransferProgressCb& progress_cb,
                                  std::atomic<bool>& cancel_flag) {

  std::ifstream ifs(params.checkpoint_path, std::ios::binary);
  if (!ifs.is_open())
    return Error{"Cannot open checkpoint: " + params.checkpoint_path};


  struct addrinfo hints{}, *res = nullptr;
  hints.ai_family   = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  std::string port_str = std::to_string(params.dst_port);
  int gai = getaddrinfo(params.dst_host.c_str(), port_str.c_str(), &hints, &res);
  if (gai != 0)
    return Error{std::string("getaddrinfo: ") + gai_strerror(gai)};

  int fd = ::socket(res->ai_family, res->ai_socktype, 0);
  if (fd < 0) { freeaddrinfo(res); return Error{"socket() failed"}; }
  if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
    ::close(fd);
    freeaddrinfo(res);
    return Error{std::string("connect() failed: ") + strerror(errno)};
  }
  freeaddrinfo(res);

  LOG_INFO("MigrationClient: connected to %s:%d for job %s",
           params.dst_host.c_str(), params.dst_port, params.job_id.c_str());


  double tokens    = 0.0;
  auto   tb_last   = std::chrono::steady_clock::now();
  double rate_Bps  = (params.allocated_bps > 0.0)
                     ? params.allocated_bps / 8.0 : 1e12;

  std::vector<char> buf(BLOCK);
  uint32_t seq = params.resume_seq;
  uint64_t total_sent = 0;


  if (seq > 0) ifs.seekg((std::streamoff)seq * BLOCK);

  while (!cancel_flag.load()) {
    ifs.read(buf.data(), BLOCK);
    std::streamsize n = ifs.gcount();
    if (n <= 0) break;


    if (params.allocated_bps > 0.0) {
      auto now = std::chrono::steady_clock::now();
      double dt = std::chrono::duration<double>(now - tb_last).count();
      tb_last  = now;
      tokens  += dt * rate_Bps;
      if (tokens > rate_Bps * 0.1) tokens = rate_Bps * 0.1;
      while (tokens < (double)n && !cancel_flag.load()) {
        std::this_thread::sleep_for(std::chrono::microseconds(500));
        auto now2 = std::chrono::steady_clock::now();
        double dt2 = std::chrono::duration<double>(now2 - tb_last).count();
        tb_last = now2;
        tokens += dt2 * rate_Bps;
      }
      tokens -= (double)n;
    }

    if (auto r = send_block(fd, seq++, buf.data(), (size_t)n); !r) {
      ::close(fd);
      return r;
    }
    total_sent += (uint64_t)n;
    if (progress_cb) progress_cb(total_sent);
  }

  ::close(fd);
  LOG_INFO("MigrationClient: sent %llu bytes for job %s",
           (unsigned long long)total_sent, params.job_id.c_str());
  return ok_void();
}



VoidResult MigrationClient::receive(uint16_t port,
                                     const std::string& dst_path,
                                     uint64_t expected_bytes,
                                     const TransferProgressCb& progress_cb) {
  int srv = ::socket(AF_INET, SOCK_STREAM, 0);
  if (srv < 0) return Error{"socket() failed"};

  int yes = 1;
  setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  struct sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port        = htons(port);
  if (::bind(srv, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    ::close(srv);
    return Error{std::string("bind() failed: ") + strerror(errno)};
  }
  ::listen(srv, 1);
  LOG_INFO("MigrationClient::receive: listening on port %d", port);

  int cli = ::accept(srv, nullptr, nullptr);
  ::close(srv);
  if (cli < 0) return Error{std::string("accept() failed: ") + strerror(errno)};

  std::ofstream ofs(dst_path, std::ios::binary | std::ios::trunc);
  if (!ofs.is_open()) { ::close(cli); return Error{"Cannot create " + dst_path}; }

  uint64_t total_rx = 0;
  char hdr[16];
  std::vector<char> buf;

  while (total_rx < expected_bytes || expected_bytes == 0) {
    auto r = recv_all(cli, hdr, 16);
    if (!r) break;

    uint32_t magic; memcpy(&magic, hdr,     4); magic = le32toh(magic);
    uint32_t seq;   memcpy(&seq,   hdr + 4, 4); seq   = le32toh(seq);
    uint64_t len;   memcpy(&len,   hdr + 8, 8); len   = le64toh(len);
    (void)seq;

    if (magic != MAGIC) { ::close(cli); return Error{"Bad magic in migration frame"}; }
    buf.resize(len);
    if (auto rr = recv_all(cli, buf.data(), len); !rr) { ::close(cli); return rr; }
    ofs.write(buf.data(), (std::streamsize)len);
    total_rx += len;
    if (progress_cb) progress_cb(total_rx);
    if (expected_bytes > 0 && total_rx >= expected_bytes) break;
  }

  ::close(cli);
  LOG_INFO("MigrationClient::receive: wrote %llu bytes to %s",
           (unsigned long long)total_rx, dst_path.c_str());
  return ok_void();
}

}
}
