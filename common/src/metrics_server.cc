#include "reclaimnet/metrics_server.h"
#include "reclaimnet/prometheus.h"
#include <arpa/inet.h>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
namespace reclaimnet {
MetricsServer::~MetricsServer() { stop(); }
VoidResult MetricsServer::start(uint16_t port) {
  socket_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socket_ < 0)
    return err_void("metrics socket failed");
  int reuse = 1;
  setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(port);
  if (bind(socket_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) <
          0 ||
      listen(socket_, 16) < 0) {
    ::close(socket_);
    socket_ = -1;
    return err_void("metrics listen failed");
  }
  socklen_t length = sizeof(address);
  getsockname(socket_, reinterpret_cast<sockaddr *>(&address), &length);
  port_ = ntohs(address.sin_port);
  running_ = true;
  thread_ = std::thread([this] {
    while (running_) {
      pollfd descriptor{socket_, POLLIN, 0};
      if (poll(&descriptor, 1, 100) <= 0)
        continue;
      int client = accept(socket_, nullptr, nullptr);
      if (client < 0)
        continue;
      pollfd incoming{client, POLLIN, 0};
      if (poll(&incoming, 1, 1000) <= 0) {
        ::close(client);
        continue;
      }
      char request[4096];
      ssize_t size = recv(client, request, sizeof(request), 0);
      std::string input(request, size > 0 ? static_cast<size_t>(size) : 0);
      bool metrics = input.rfind("GET /metrics ", 0) == 0;
      std::string body =
          metrics ? MetricRegistry::instance().render() : "not found\n";
      std::string response =
          std::string(metrics ? "HTTP/1.1 200 OK\r\n"
                              : "HTTP/1.1 404 Not Found\r\n") +
          "Content-Type: text/plain; version=0.0.4\r\nConnection: "
          "close\r\nContent-Length: " +
          std::to_string(body.size()) + "\r\n\r\n" + body;
      size_t written = 0;
      while (written < response.size()) {
#ifdef MSG_NOSIGNAL
        ssize_t count = send(client, response.data() + written,
                             response.size() - written, MSG_NOSIGNAL);
#else
        int no_sigpipe = 1;
        setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe,
                   sizeof(no_sigpipe));
        ssize_t count = send(client, response.data() + written,
                             response.size() - written, 0);
#endif
        if (count <= 0)
          break;
        written += static_cast<size_t>(count);
      }
      ::close(client);
    }
  });
  return ok_void();
}
void MetricsServer::stop() {
  running_ = false;
  if (socket_ >= 0)
    shutdown(socket_, SHUT_RDWR);
  if (thread_.joinable())
    thread_.join();
  if (socket_ >= 0)
    ::close(socket_);
  socket_ = -1;
}
}
