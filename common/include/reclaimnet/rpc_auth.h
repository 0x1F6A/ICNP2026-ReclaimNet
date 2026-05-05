#pragma once
#include "reclaimnet/security.h"
#include "reclaimnet/time.h"
#if RECLAIMNET_HAVE_GRPC
#include <grpcpp/grpcpp.h>
namespace reclaimnet {
inline bool rpc_peer_identity(grpc::ServerContext *ctx,
                              const std::string &node) {
  auto auth = ctx->auth_context();
  if (!auth || !auth->IsPeerAuthenticated())
    return false;
  for (const auto &identity : auth->GetPeerIdentity())
    if (std::string(identity.data(), identity.size()) == node)
      return true;
  auto names = auth->FindPropertyValues("x509_common_name");
  for (const auto &name : names)
    if (std::string(name.data(), name.size()) == node)
      return true;
  return false;
}
inline Result<JwtClaims> rpc_claims(grpc::ServerContext *ctx,
                                    const std::string &key) {
  auto metadata = ctx->client_metadata().find("authorization");
  if (metadata == ctx->client_metadata().end())
    return Error{"Authentication token is required"};
  std::string token(metadata->second.data(), metadata->second.size());
  if (token.rfind("Bearer ", 0) != 0)
    return Error{"Invalid authentication scheme"};
  return verify_jwt_rs256(token.substr(7), key, now_ns() / 1000000000);
}
inline grpc::Status rpc_authorize(grpc::ServerContext *ctx,
                                  const std::string &key,
                                  const std::string &node = {}) {
  auto claims = rpc_claims(ctx, key);
  if (!claims)
    return {grpc::StatusCode::UNAUTHENTICATED, claims.error().message};
  if ((!node.empty() && claims.value().node_id != node) ||
      !rpc_peer_identity(ctx, claims.value().node_id))
    return {grpc::StatusCode::PERMISSION_DENIED,
            "Authenticated node identity does not match request"};
  return grpc::Status::OK;
}
}
#endif
