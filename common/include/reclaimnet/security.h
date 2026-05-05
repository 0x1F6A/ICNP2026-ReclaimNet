#pragma once
#include "reclaimnet/result.h"
#include <cstdint>
#include <memory>
#include <string>
typedef struct ssl_ctx_st SSL_CTX;
namespace reclaimnet {
struct JwtClaims {
  std::string subject;
  std::string node_id;
  int64_t issued_at_s = 0;
  int64_t expires_at_s = 0;
};
struct Tls13Config {
  std::string cert_path;
  std::string key_path;
  std::string ca_path;
  bool server = false;
};
Result<std::string> issue_jwt_rs256(const JwtClaims &claims,
                                    const std::string &private_pem);
Result<JwtClaims> verify_jwt_rs256(const std::string &token,
                                   const std::string &public_pem,
                                   int64_t now_s);
Result<std::string> sha256_file(const std::string &path);
Result<std::shared_ptr<SSL_CTX>> make_tls13_context(const Tls13Config &config);
}
