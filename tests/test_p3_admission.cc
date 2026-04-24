


#include <gtest/gtest.h>
#include "reclaimnet/p3_admission.h"
#include "reclaimnet/time.h"

using namespace reclaimnet;
using namespace reclaimnet::control;



static MigrationEntry make_entry(const std::string& job,
                                  const std::string& src,
                                  const std::string& dst,
                                  double deadline_s = 1e9) {
  MigrationEntry e{};
  e.job_id               = job;
  e.src                  = src;
  e.dst                  = dst;
  e.data_remaining_bytes = 1000000000ULL;
  e.deadline_s           = deadline_s;
  e.start_ns             = now_ns();
  return e;
}



TEST(P3Admission, AdmitAndRelease) {
  P3Admission p3(0.40);
  p3.add_link("A", "B", 10e9);

  auto r = p3.admit(make_entry("job-1", "A", "B"));
  EXPECT_TRUE(r.has_value());
  EXPECT_EQ(p3.active_count(), 1u);

  p3.release("job-1");
  EXPECT_EQ(p3.active_count(), 0u);
}



TEST(P3Admission, DeniedWhenNoCapacity) {
  P3Admission p3(0.40);
  p3.add_link("A", "B", 10e9);


  ASSERT_TRUE(p3.admit(make_entry("job-1", "A", "B")).has_value());


  auto r2 = p3.admit(make_entry("job-2", "A", "B"));
  EXPECT_FALSE(r2.has_value());
}



TEST(P3Admission, DeniedUnknownLink) {
  P3Admission p3(0.40);
  auto r = p3.admit(make_entry("job-1", "X", "Y"));
  EXPECT_FALSE(r.has_value());
}



TEST(P3Admission, UpdateProgress) {
  P3Admission p3(0.40);
  p3.add_link("A", "B", 10e9);
  ASSERT_TRUE(p3.admit(make_entry("job-1", "A", "B")));

  p3.update_progress("job-1", 500000000ULL);
  auto active = p3.active_migrations();
  ASSERT_EQ(active.size(), 1u);
  EXPECT_EQ(active[0].data_remaining_bytes, 500000000ULL);
}



TEST(P3Admission, ExpiredMigrations) {
  P3Admission p3(0.40);
  p3.add_link("A", "B", 10e9);
  auto e = make_entry("job-exp", "A", "B", 1.0);
  ASSERT_TRUE(p3.admit(e));

  auto expired = p3.expired_migrations(1e12);
  ASSERT_EQ(expired.size(), 1u);
  EXPECT_EQ(expired[0].job_id, "job-exp");
}



TEST(P3Admission, EmergencyDrain) {
  P3Admission p3(0.40);
  p3.add_link("A", "B", 10e9);
  ASSERT_TRUE(p3.admit(make_entry("job-1", "A", "B")));

  p3.emergency_drain("A");
  auto active = p3.active_migrations();
  ASSERT_EQ(active.size(), 1u);
  EXPECT_TRUE(active[0].emergency);
}



TEST(P3Admission, LinkStates) {
  P3Admission p3(0.40);
  p3.add_link("A", "B", 10e9);
  p3.add_link("C", "D", 5e9);

  auto ls = p3.link_states();
  EXPECT_EQ(ls.size(), 2u);
}
