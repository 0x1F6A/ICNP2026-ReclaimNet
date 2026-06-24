#include "../data_plane/bpf_shared.h"
#include "reclaimnet/ebpf_loader.h"
#include "reclaimnet/gpu_isolation.h"
#include "reclaimnet/security.h"
#include "reclaimnet/telemetry.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <unistd.h>
using namespace reclaimnet;
using namespace reclaimnet::agent;
namespace {
struct PemPair {
  std::string private_key, public_key, certificate;
};
PemPair rsa_pem() {
  EVP_PKEY_CTX *context = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
  EVP_PKEY_keygen_init(context);
  EVP_PKEY_CTX_set_rsa_keygen_bits(context, 2048);
  EVP_PKEY *key = nullptr;
  EVP_PKEY_keygen(context, &key);
  EVP_PKEY_CTX_free(context);
  BIO *private_bio = BIO_new(BIO_s_mem());
  BIO *public_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PrivateKey(private_bio, key, nullptr, nullptr, 0, nullptr,
                           nullptr);
  PEM_write_bio_PUBKEY(public_bio, key);
  auto text = [](BIO *bio) {
    char *value = nullptr;
    long size = BIO_get_mem_data(bio, &value);
    return std::string(value, size);
  };
  PemPair pair{text(private_bio), text(public_bio), {}};
  X509 *certificate = X509_new();
  X509_set_version(certificate, 2);
  ASN1_INTEGER_set(X509_get_serialNumber(certificate), 1);
  X509_gmtime_adj(X509_get_notBefore(certificate), 0);
  X509_gmtime_adj(X509_get_notAfter(certificate), 3600);
  X509_set_pubkey(certificate, key);
  X509_NAME *name = X509_NAME_new();
  X509_NAME_add_entry_by_txt(
      name, "CN", MBSTRING_ASC,
      reinterpret_cast<const unsigned char *>("localhost"), -1, -1, 0);
  X509_set_subject_name(certificate, name);
  X509_set_issuer_name(certificate, name);
  X509_NAME_free(name);
  X509_sign(certificate, key, EVP_sha256());
  BIO *certificate_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_X509(certificate_bio, certificate);
  pair.certificate = text(certificate_bio);
  BIO_free(private_bio);
  BIO_free(public_bio);
  BIO_free(certificate_bio);
  X509_free(certificate);
  EVP_PKEY_free(key);
  return pair;
}
struct TempFiles {
  std::filesystem::path directory;
  TempFiles() {
    char name[] = "/tmp/reclaimnet-security-XXXXXX";
    directory = mkdtemp(name);
  }
  ~TempFiles() { std::filesystem::remove_all(directory); }
  std::string write(const std::string &name, const std::string &data) {
    auto file = directory / name;
    std::ofstream output(file, std::ios::binary);
    output << data;
    return file.string();
  }
};
class TestGpu final : public GpuBackend {
public:
  std::vector<GpuDevice> devices{
      {0, "GPU-test", "RTX-test", 24000, 24000, 0, 8.6, false, false}};
  int created = 0, destroyed = 0, zeroed = 0;
  bool zero_error = false;
  Result<std::vector<GpuDevice>> inventory() override { return devices; }
  Result<std::string> create_mig(unsigned, unsigned, unsigned,
                                 uint64_t) override {
    ++created;
    return std::string("MIG-test");
  }
  VoidResult destroy_mig(const std::string &) override {
    ++destroyed;
    return ok_void();
  }
  Result<uint64_t> zero_free_vram(const std::string &) override {
    if (zero_error)
      return Error{"zero failed"};
    ++zeroed;
    return uint64_t{24000};
  }
};
}
#if RECLAIMNET_HAVE_JSON
TEST(Security, Rs256VerifiesClaimsAndRejectsTamperingAndWrongKey) {
  auto pair = rsa_pem();
  JwtClaims claims{"provider-alice", "node-a", 100, 130};
  auto token = issue_jwt_rs256(claims, pair.private_key);
  ASSERT_TRUE(token) << token.error().message;
  auto verified = verify_jwt_rs256(token.value(), pair.public_key, 110);
  ASSERT_TRUE(verified) << verified.error().message;
  EXPECT_EQ(verified.value().subject, claims.subject);
  EXPECT_EQ(verified.value().node_id, claims.node_id);
  EXPECT_EQ(verified.value().issued_at_s, 100);
  EXPECT_EQ(verified.value().expires_at_s, 130);
  auto changed = token.value();
  changed[changed.find('.') + 2] =
      changed[changed.find('.') + 2] == 'a' ? 'b' : 'a';
  EXPECT_FALSE(verify_jwt_rs256(changed, pair.public_key, 110));
  EXPECT_FALSE(verify_jwt_rs256(token.value(), rsa_pem().public_key, 110));
  EXPECT_FALSE(verify_jwt_rs256(token.value(), pair.public_key, 130));
  EXPECT_FALSE(verify_jwt_rs256(token.value(), pair.public_key, 99));
}
#else
TEST(Security, JwtWithoutJsonReportsUnavailable) {
  EXPECT_FALSE(issue_jwt_rs256({"subject", "node", 100, 130}, "key"));
  EXPECT_FALSE(verify_jwt_rs256("token", "key", 110));
}
#endif
TEST(Security, Sha256UsesFileContents) {
  TempFiles files;
  auto digest = sha256_file(files.write("data", "abc"));
  ASSERT_TRUE(digest) << digest.error().message;
  EXPECT_EQ(digest.value(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  EXPECT_FALSE(sha256_file((files.directory / "missing").string()));
}
TEST(Security, ContextRequiresMtlsAndOnlyTls13) {
  TempFiles files;
  auto pair = rsa_pem();
  Tls13Config configuration{files.write("cert", pair.certificate),
                            files.write("key", pair.private_key),
                            files.write("ca", pair.certificate), true};
  auto context = make_tls13_context(configuration);
  ASSERT_TRUE(context) << context.error().message;
  EXPECT_EQ(SSL_CTX_get_min_proto_version(context.value().get()),
            TLS1_3_VERSION);
  EXPECT_EQ(SSL_CTX_get_max_proto_version(context.value().get()),
            TLS1_3_VERSION);
  EXPECT_EQ(SSL_CTX_get_verify_mode(context.value().get()),
            SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT);
  configuration.key_path = files.write("wrong-key", rsa_pem().private_key);
  EXPECT_FALSE(make_tls13_context(configuration));
}
TEST(GpuIsolation, ReservationUsesInventoryAndReleasesOnlyAfterZero) {
  auto backend = std::make_shared<TestGpu>();
  GpuIsolation isolation(backend);
  auto reserved = isolation.reserve("job", 0, 16000);
  ASSERT_TRUE(reserved) << reserved.error().message;
  EXPECT_EQ(reserved.value().device_uuid, "GPU-test");
  EXPECT_TRUE(isolation.reserve("job", 0, 16000));
  EXPECT_EQ(isolation.reservations().size(), 1u);
  EXPECT_FALSE(isolation.reserve("other", 0, 16000));
  backend->zero_error = true;
  EXPECT_FALSE(isolation.release("job"));
  EXPECT_EQ(isolation.reservations().size(), 1u);
  backend->zero_error = false;
  EXPECT_TRUE(isolation.release("job"));
  EXPECT_EQ(backend->zeroed, 1);
  EXPECT_TRUE(isolation.reservations().empty());
  EXPECT_TRUE(isolation.reserve("other", 0, 16000));
}
TEST(GpuIsolation, MigUsesReportedHardwareCapabilityAndLifecycle) {
  auto backend = std::make_shared<TestGpu>();
  GpuIsolation isolation(backend);
  EXPECT_FALSE(isolation.reserve("unsupported", 0, 8000, 0));
  EXPECT_EQ(backend->created, 0);
  backend->devices.front().mig_supported = true;
  auto reserved = isolation.reserve("mig-job", 0, 8000, 0);
  ASSERT_TRUE(reserved) << reserved.error().message;
  EXPECT_TRUE(reserved.value().mig);
  EXPECT_EQ(reserved.value().device_uuid, "MIG-test");
  EXPECT_TRUE(isolation.release("mig-job"));
  EXPECT_EQ(backend->zeroed, 1);
  EXPECT_EQ(backend->destroyed, 1);
}
TEST(Telemetry, GpuCountersComeFromDeviceInventory) {
  auto backend = std::make_shared<TestGpu>();
  backend->devices.front().free_vram_bytes = 12000;
  backend->devices.front().utilization_pct = 50;
  TelemetryCollector telemetry("lo", 0.1, backend);
  auto value = telemetry.sample_now();
  EXPECT_EQ(value.gpu_count, 1u);
  EXPECT_EQ(value.total_vram_bytes, 24000u);
  EXPECT_EQ(value.vram_free_bytes, 12000u);
  EXPECT_DOUBLE_EQ(value.gpu_util_pct, 50);
  EXPECT_DOUBLE_EQ(value.vram_used_pct, 50);
  EXPECT_DOUBLE_EQ(value.cuda_capability, 8.6);
  backend->devices.clear();
  value = telemetry.sample_now();
  EXPECT_EQ(value.gpu_count, 0u);
  EXPECT_EQ(value.vram_free_bytes, 0u);
}
TEST(BpfPolicy, CanonicalRatesAreConvertedFromBitsToBytes) {
  EXPECT_EQ(ebpf_rate_bytes_per_second(8000), 1000u);
  flow_state bucket{};
  bucket.rate_bytes_per_sec = 1000;
  bucket.tokens_bytes = 1500;
  EXPECT_TRUE(rn_take_tokens(&bucket, 0, 1500));
  EXPECT_FALSE(rn_take_tokens(&bucket, 50000000, 1500));
  EXPECT_EQ(bucket.tokens_bytes, 50u);
  EXPECT_TRUE(rn_take_tokens(&bucket, 1500000000, 1500));
  EXPECT_EQ(bucket.tokens_bytes, 0u);
}
TEST(BpfPolicy, SubByteRefillsAreNotLostBetweenPackets) {
  flow_state bucket{};
  bucket.rate_bytes_per_sec = 10;
  for (uint64_t tick = 1; tick < 100; ++tick)
    EXPECT_FALSE(rn_take_tokens(&bucket, tick * 1000000, 1));
  EXPECT_TRUE(rn_take_tokens(&bucket, 100000000, 1));
}
TEST(BpfPolicy, KillSwitchDropsNewWorkAndHonorsMigrationGrace) {
  rn_reclaim_state state{};
  state.active = 1;
  state.control_port = 50052;
  state.migration_port = 7600;
  EXPECT_TRUE(rn_reclaim_drop(&state, 100, 1, 1, 0, 20000, 7600, 1));
  EXPECT_TRUE(rn_reclaim_drop(&state, 100, 1, 0, 1, 20000, 7600, 1));
  state.scheduled = 1;
  state.grace_deadline_mono_ns = 200;
  EXPECT_FALSE(rn_reclaim_drop(&state, 100, 1, 0, 1, 20000, 7600, 1));
  EXPECT_TRUE(rn_reclaim_drop(&state, 200, 1, 0, 1, 20000, 7600, 1));
  EXPECT_TRUE(rn_reclaim_drop(&state, 100, 1, 0, 1, 20000, 8888, 0));
  EXPECT_FALSE(rn_reclaim_drop(&state, 100, 1, 1, 0, 20000, 50051, 0));
  EXPECT_FALSE(rn_reclaim_drop(&state, 100, 1, 1, 0, 20000, 50052, 0));
}
TEST(BpfPolicy, HostTupleHashMatchesKernelKeyLayout) {
  FiveTuple tuple{0x0a010101, 0x0a010102, 20000, 7600, 6};
  flow_key key{};
  key.src_ip = tuple.src_ip;
  key.dst_ip = tuple.dst_ip;
  key.src_port = tuple.src_port;
  key.dst_port = tuple.dst_port;
  key.protocol = tuple.protocol;
  EXPECT_EQ(ebpf_flow_key(tuple), rn_flow_hash(&key));
}
TEST(BpfPolicy, ControlAndMigrationHandshakeReceiveTheirDscpClasses) {
  traffic_ports ports{50051, 50052, 7600, 0};
  flow_key key{};
  key.src_port = 20000;
  key.dst_port = 50051;
  EXPECT_EQ(rn_default_class(&key, &ports), RN_CLASS_EMERGENCY);
  key.dst_port = 50052;
  EXPECT_EQ(rn_default_class(&key, &ports), RN_CLASS_EMERGENCY);
  key.dst_port = 7600;
  EXPECT_EQ(rn_default_class(&key, &ports), RN_CLASS_MIGRATION);
  key.dst_port = 8888;
  EXPECT_EQ(rn_default_class(&key, &ports), RN_CLASS_RESEARCH);
}
#if !defined(__linux__) || !RECLAIMNET_HAVE_LIBBPF
TEST(BpfPolicy, UnsupportedPlatformsReturnErrorsForMapMutations) {
  EbpfHandle handle;
  EXPECT_FALSE(ebpf_load_tc_ratelimit("missing", "eth0"));
  EXPECT_FALSE(ebpf_set_flow_rate(handle, 1, 8000));
  EXPECT_FALSE(
      ebpf_set_flow_policy(handle, {}, 8000, TrafficClass::P1_CHECKPOINT));
  EXPECT_FALSE(ebpf_set_kill_switch(-1, true));
  EXPECT_FALSE(ebpf_read_flow_stats(handle));
}
#endif
