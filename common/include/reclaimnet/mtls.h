/// @file mtls.h
/// @brief mTLS credential helpers for gRPC channels and servers.
///
/// Wraps grpc::SslCredentials and grpc::SslServerCredentials to load
/// PEM-encoded certificates from disk.  All connections between coordinator
/// and agents use mutual TLS; the shared CA signs both sides.

#pragma once

#include <string>
#include <memory>
#include "reclaimnet/result.h"

// Forward-declare gRPC credential types to avoid pulling in grpc headers.
namespace grpc {
  class ChannelCredentials;
  class ServerCredentials;
}

namespace reclaimnet {

/// Bundle of PEM strings for a single entity (cert + key + CA).
struct TlsBundle {
  std::string cert_pem;  ///< PEM-encoded entity certificate.
  std::string key_pem;   ///< PEM-encoded private key.
  std::string ca_pem;    ///< PEM-encoded CA certificate (trust anchor).
};

/// Load PEM files from disk into a TlsBundle.
/// @param cert_path  Path to entity certificate file.
/// @param key_path   Path to private key file.
/// @param ca_path    Path to CA certificate file.
Result<TlsBundle> load_tls_bundle(
    const std::string& cert_path,
    const std::string& key_path,
    const std::string& ca_path);

/// Build mTLS channel credentials (client-side) from a TlsBundle.
/// @returns shared_ptr<grpc::ChannelCredentials> on success.
Result<std::shared_ptr<grpc::ChannelCredentials>>
make_client_credentials(const TlsBundle& bundle);

/// Build mTLS server credentials from a TlsBundle.
/// @returns shared_ptr<grpc::ServerCredentials> on success.
Result<std::shared_ptr<grpc::ServerCredentials>>
make_server_credentials(const TlsBundle& bundle);

/// Return insecure channel credentials (for testing / local dev).
std::shared_ptr<grpc::ChannelCredentials> insecure_channel_credentials();

/// Return insecure server credentials (for testing / local dev).
std::shared_ptr<grpc::ServerCredentials> insecure_server_credentials();

/// Read the entire contents of a PEM file into a string.
Result<std::string> read_pem_file(const std::string& path);

}  // namespace reclaimnet
