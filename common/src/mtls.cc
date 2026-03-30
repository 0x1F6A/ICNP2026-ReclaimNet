


#include "reclaimnet/mtls.h"
#include "reclaimnet/log.h"

#include <fstream>
#include <sstream>
#include <stdexcept>


#if __has_include(<grpcpp/grpcpp.h>)
#  include <grpcpp/grpcpp.h>
#  include <grpcpp/security/credentials.h>
#  define HAVE_GRPC 1
#else
#  define HAVE_GRPC 0

   namespace grpc {
     class ChannelCredentials {};
     class ServerCredentials {};
   }
#endif

namespace reclaimnet {



Result<std::string> read_pem_file(const std::string& path) {
  std::ifstream ifs(path, std::ios::in | std::ios::binary);
  if (!ifs.is_open()) {
    return Error{"Cannot open PEM file: " + path};
  }
  std::ostringstream oss;
  oss << ifs.rdbuf();
  if (ifs.fail()) {
    return Error{"Read error on PEM file: " + path};
  }
  return oss.str();
}



Result<TlsBundle> load_tls_bundle(
    const std::string& cert_path,
    const std::string& key_path,
    const std::string& ca_path) {
  auto cert = read_pem_file(cert_path);
  if (!cert) return cert.error();

  auto key = read_pem_file(key_path);
  if (!key) return key.error();

  auto ca = read_pem_file(ca_path);
  if (!ca) return ca.error();

  TlsBundle b;
  b.cert_pem = cert.value();
  b.key_pem  = key.value();
  b.ca_pem   = ca.value();
  return b;
}



Result<std::shared_ptr<grpc::ChannelCredentials>>
make_client_credentials(const TlsBundle& bundle) {
#if HAVE_GRPC
  grpc::SslCredentialsOptions opts;
  opts.pem_root_certs    = bundle.ca_pem;
  opts.pem_cert_chain    = bundle.cert_pem;
  opts.pem_private_key   = bundle.key_pem;
  return grpc::SslCredentials(opts);
#else
  (void)bundle;
  return Error{"gRPC not available; cannot create mTLS credentials"};
#endif
}



Result<std::shared_ptr<grpc::ServerCredentials>>
make_server_credentials(const TlsBundle& bundle) {
#if HAVE_GRPC
  grpc::SslServerCredentialsOptions opts(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
  grpc::SslServerCredentialsOptions::PemKeyCertPair kcp;
  kcp.cert_chain  = bundle.cert_pem;
  kcp.private_key = bundle.key_pem;
  opts.pem_key_cert_pairs.push_back(kcp);
  opts.pem_root_certs = bundle.ca_pem;
  return grpc::SslServerCredentials(opts);
#else
  (void)bundle;
  return Error{"gRPC not available; cannot create mTLS server credentials"};
#endif
}



std::shared_ptr<grpc::ChannelCredentials> insecure_channel_credentials() {
#if HAVE_GRPC
  return grpc::InsecureChannelCredentials();
#else
  return nullptr;
#endif
}

std::shared_ptr<grpc::ServerCredentials> insecure_server_credentials() {
#if HAVE_GRPC
  return grpc::InsecureServerCredentials();
#else
  return nullptr;
#endif
}

}
