

#pragma once

#include "reclaimnet/result.h"
#include <memory>
#include <string>

namespace grpc {
class ChannelCredentials;
class ServerCredentials;
}

namespace reclaimnet {

struct TlsBundle {
  std::string cert_pem;
  std::string key_pem;
  std::string ca_pem;
};

Result<TlsBundle> load_tls_bundle(const std::string &cert_path,
                                  const std::string &key_path,
                                  const std::string &ca_path);

Result<std::shared_ptr<grpc::ChannelCredentials>>
make_client_credentials(const TlsBundle &bundle);

Result<std::shared_ptr<grpc::ServerCredentials>>
make_server_credentials(const TlsBundle &bundle);

std::shared_ptr<grpc::ChannelCredentials> insecure_channel_credentials();

std::shared_ptr<grpc::ServerCredentials> insecure_server_credentials();

Result<std::string> read_pem_file(const std::string &path);

}
