

#include "reclaimnet/mtls.h"
#include "reclaimnet/log.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#if RECLAIMNET_HAVE_GRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/tls_certificate_provider.h>
#include <grpcpp/security/tls_credentials_options.h>
#define HAVE_GRPC 1
#else
#define HAVE_GRPC 0

namespace grpc {
class ChannelCredentials {};
class ServerCredentials {};
}
#endif

namespace reclaimnet {

Result<std::string> read_pem_file(const std::string &path) {
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

Result<TlsBundle> load_tls_bundle(const std::string &cert_path,
                                  const std::string &key_path,
                                  const std::string &ca_path) {
  auto cert = read_pem_file(cert_path);
  if (!cert)
    return cert.error();

  auto key = read_pem_file(key_path);
  if (!key)
    return key.error();

  auto ca = read_pem_file(ca_path);
  if (!ca)
    return ca.error();

  TlsBundle b;
  b.cert_pem = cert.value();
  b.key_pem = key.value();
  b.ca_pem = ca.value();
  return b;
}

Result<std::shared_ptr<grpc::ChannelCredentials>>
make_client_credentials(const TlsBundle &bundle) {
#if HAVE_GRPC
  auto provider =
      std::make_shared<grpc::experimental::InMemoryCertificateProvider>();
  auto root = provider->UpdateRoot(bundle.ca_pem);
  auto identity = provider->UpdateIdentityKeyCertPair(
      std::vector<grpc::experimental::IdentityKeyOrSignerCertPair>{
          {bundle.key_pem, bundle.cert_pem}});
  if (!root.ok())
    return Error{root.ToString()};
  if (!identity.ok())
    return Error{identity.ToString()};
  grpc::experimental::TlsChannelCredentialsOptions opts;
  opts.set_root_certificate_provider(provider);
  opts.set_identity_certificate_provider(provider);
  opts.set_min_tls_version(TLS1_3);
  opts.set_max_tls_version(TLS1_3);
  return grpc::experimental::TlsCredentials(opts);
#else
  (void)bundle;
  return Error{"gRPC not available; cannot create mTLS credentials"};
#endif
}

Result<std::shared_ptr<grpc::ServerCredentials>>
make_server_credentials(const TlsBundle &bundle) {
#if HAVE_GRPC
  auto provider =
      std::make_shared<grpc::experimental::InMemoryCertificateProvider>();
  auto root = provider->UpdateRoot(bundle.ca_pem);
  auto identity = provider->UpdateIdentityKeyCertPair(
      std::vector<grpc::experimental::IdentityKeyOrSignerCertPair>{
          {bundle.key_pem, bundle.cert_pem}});
  if (!root.ok())
    return Error{root.ToString()};
  if (!identity.ok())
    return Error{identity.ToString()};
  auto options =
      grpc::experimental::TlsServerCredentialsOptions::Create(provider);
  if (!options.ok())
    return Error{options.status().ToString()};
  auto opts = std::move(options.value());
  opts.set_root_certificate_provider(provider);
  opts.set_cert_request_type(
      GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
  opts.set_min_tls_version(TLS1_3);
  opts.set_max_tls_version(TLS1_3);
  return grpc::experimental::TlsServerCredentials(opts);
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
