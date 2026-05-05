#include "reclaimnet/security.h"
#include <array>
#include <fstream>
#include <iomanip>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <sstream>
#include <vector>
#if RECLAIMNET_HAVE_JSON
#include <nlohmann/json.hpp>
#endif
namespace reclaimnet {
namespace {
#if RECLAIMNET_HAVE_JSON
std::string encode64(const unsigned char *data, size_t size) {
  std::string out(4 * ((size + 2) / 3), '\0');
  EVP_EncodeBlock(reinterpret_cast<unsigned char *>(out.data()), data,
                  static_cast<int>(size));
  for (auto &c : out) {
    if (c == '+')
      c = '-';
    else if (c == '/')
      c = '_';
  }
  while (!out.empty() && out.back() == '=')
    out.pop_back();
  return out;
}
std::string encode64(const std::string &text) {
  return encode64(reinterpret_cast<const unsigned char *>(text.data()),
                  text.size());
}
Result<std::vector<unsigned char>> decode64(std::string input) {
  for (auto &c : input) {
    if (c == '-')
      c = '+';
    else if (c == '_')
      c = '/';
  }
  while (input.size() % 4)
    input.push_back('=');
  std::vector<unsigned char> out(input.size() / 4 * 3);
  int size = EVP_DecodeBlock(
      out.data(), reinterpret_cast<const unsigned char *>(input.data()),
      static_cast<int>(input.size()));
  if (size < 0)
    return Error{"Invalid JWT base64url"};
  if (!input.empty() && input.back() == '=')
    --size;
  if (input.size() > 1 && input[input.size() - 2] == '=')
    --size;
  out.resize(static_cast<size_t>(size));
  return out;
}
std::shared_ptr<EVP_PKEY> key_from_pem(const std::string &pem,
                                       bool private_key) {
  BIO *bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
  EVP_PKEY *key = private_key
                      ? PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr)
                      : PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
  BIO_free(bio);
  return {key, EVP_PKEY_free};
}
#endif
}
Result<std::string> issue_jwt_rs256(const JwtClaims &claims,
                                    const std::string &private_pem) {
#if RECLAIMNET_HAVE_JSON
  if (claims.subject.empty() || claims.node_id.empty() ||
      claims.expires_at_s <= claims.issued_at_s)
    return Error{"JWT requires subject, node and a positive lifetime"};
  auto key = key_from_pem(private_pem, true);
  if (!key || EVP_PKEY_base_id(key.get()) != EVP_PKEY_RSA)
    return Error{"RS256 requires an RSA private key"};
  const std::string header =
      encode64(nlohmann::json{{"alg", "RS256"}, {"typ", "JWT"}}.dump());
  const std::string payload = encode64(nlohmann::json{
      {"sub", claims.subject},
      {"node", claims.node_id},
      {"iat", claims.issued_at_s},
      {"exp", claims.expires_at_s}}.dump());
  const std::string message = header + "." + payload;
  auto context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>(
      EVP_MD_CTX_new(), EVP_MD_CTX_free);
  EVP_PKEY_CTX *signing = nullptr;
  if (!context ||
      EVP_DigestSignInit(context.get(), &signing, EVP_sha256(), nullptr,
                         key.get()) != 1 ||
      EVP_PKEY_CTX_set_rsa_padding(signing, RSA_PKCS1_PADDING) != 1)
    return Error{"Cannot initialize RS256 signing"};
  size_t size = 0;
  if (EVP_DigestSign(context.get(), nullptr, &size,
                     reinterpret_cast<const unsigned char *>(message.data()),
                     message.size()) != 1)
    return Error{"Cannot size RS256 signature"};
  std::vector<unsigned char> signature(size);
  if (EVP_DigestSign(context.get(), signature.data(), &size,
                     reinterpret_cast<const unsigned char *>(message.data()),
                     message.size()) != 1)
    return Error{"Cannot sign JWT"};
  return message + "." + encode64(signature.data(), size);
#else
  (void)claims;
  (void)private_pem;
  return Error{"JWT requires nlohmann_json"};
#endif
}
Result<JwtClaims> verify_jwt_rs256(const std::string &token,
                                   const std::string &public_pem,
                                   int64_t now_s) {
#if RECLAIMNET_HAVE_JSON
  const size_t first = token.find('.');
  const size_t second =
      first == std::string::npos ? first : token.find('.', first + 1);
  if (first == std::string::npos || second == std::string::npos ||
      token.find('.', second + 1) != std::string::npos)
    return Error{"JWT must contain three segments"};
  auto header = decode64(token.substr(0, first));
  auto payload = decode64(token.substr(first + 1, second - first - 1));
  auto signature = decode64(token.substr(second + 1));
  if (!header)
    return header.error();
  if (!payload)
    return payload.error();
  if (!signature)
    return signature.error();
  auto key = key_from_pem(public_pem, false);
  if (!key || EVP_PKEY_base_id(key.get()) != EVP_PKEY_RSA)
    return Error{"RS256 requires an RSA public key"};
  auto context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>(
      EVP_MD_CTX_new(), EVP_MD_CTX_free);
  EVP_PKEY_CTX *verifying = nullptr;
  if (!context ||
      EVP_DigestVerifyInit(context.get(), &verifying, EVP_sha256(), nullptr,
                           key.get()) != 1 ||
      EVP_PKEY_CTX_set_rsa_padding(verifying, RSA_PKCS1_PADDING) != 1 ||
      EVP_DigestVerify(
          context.get(), signature.value().data(), signature.value().size(),
          reinterpret_cast<const unsigned char *>(token.data()), second) != 1)
    return Error{"JWT signature verification failed"};
  try {
    const auto h =
        nlohmann::json::parse(header.value().begin(), header.value().end());
    const auto p =
        nlohmann::json::parse(payload.value().begin(), payload.value().end());
    if (h.at("alg") != "RS256" || h.at("typ") != "JWT" ||
        !p.at("iat").is_number_integer() || !p.at("exp").is_number_integer())
      return Error{"JWT algorithm or claims are invalid"};
    JwtClaims claims{p.at("sub").get<std::string>(),
                     p.at("node").get<std::string>(),
                     p.at("iat").get<int64_t>(), p.at("exp").get<int64_t>()};
    if (claims.subject.empty() || claims.node_id.empty() ||
        claims.issued_at_s > now_s || claims.expires_at_s <= now_s)
      return Error{"JWT is outside its validity period"};
    return claims;
  } catch (const std::exception &e) {
    return Error{e.what()};
  }
#else
  (void)token;
  (void)public_pem;
  (void)now_s;
  return Error{"JWT requires nlohmann_json"};
#endif
}
Result<std::string> sha256_file(const std::string &path) {
  std::ifstream input(path, std::ios::binary);
  if (!input)
    return Error{"Cannot open SHA256 input " + path};
  auto context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>(
      EVP_MD_CTX_new(), EVP_MD_CTX_free);
  if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1)
    return Error{"Cannot initialize SHA256"};
  std::array<char, 65536> buffer{};
  while (input.read(buffer.data(), buffer.size()) || input.gcount())
    if (EVP_DigestUpdate(context.get(), buffer.data(),
                         static_cast<size_t>(input.gcount())) != 1)
      return Error{"Cannot update SHA256"};
  if (input.bad())
    return Error{"Cannot read SHA256 input"};
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int size = 0;
  if (EVP_DigestFinal_ex(context.get(), digest.data(), &size) != 1)
    return Error{"Cannot finish SHA256"};
  std::ostringstream hex;
  for (unsigned int i = 0; i < size; ++i)
    hex << std::hex << std::setfill('0') << std::setw(2)
        << static_cast<unsigned int>(digest[i]);
  return hex.str();
}
Result<std::shared_ptr<SSL_CTX>> make_tls13_context(const Tls13Config &config) {
  std::shared_ptr<SSL_CTX> context(
      SSL_CTX_new(config.server ? TLS_server_method() : TLS_client_method()),
      SSL_CTX_free);
  if (!context ||
      SSL_CTX_set_min_proto_version(context.get(), TLS1_3_VERSION) != 1 ||
      SSL_CTX_set_max_proto_version(context.get(), TLS1_3_VERSION) != 1 ||
      SSL_CTX_use_certificate_chain_file(context.get(),
                                         config.cert_path.c_str()) != 1 ||
      SSL_CTX_use_PrivateKey_file(context.get(), config.key_path.c_str(),
                                  SSL_FILETYPE_PEM) != 1 ||
      SSL_CTX_check_private_key(context.get()) != 1 ||
      SSL_CTX_load_verify_locations(context.get(), config.ca_path.c_str(),
                                    nullptr) != 1)
    return Error{"Cannot configure TLS 1.3 certificate, key or CA"};
  SSL_CTX_set_verify(context.get(),
                     SSL_VERIFY_PEER |
                         (config.server ? SSL_VERIFY_FAIL_IF_NO_PEER_CERT : 0),
                     nullptr);
  return context;
}
}
