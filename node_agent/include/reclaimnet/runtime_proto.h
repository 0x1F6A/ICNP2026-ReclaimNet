#pragma once
#include "reclaimnet/runtime.h"
#if RECLAIMNET_HAVE_PROTO
#include <reclaimnet.pb.h>
namespace reclaimnet::agent {
inline JobRuntimeSpec from_proto(const proto::RuntimeSpec &p) {
  JobRuntimeSpec s;
  s.job_id = p.job_id();
  s.state_path = p.state_path();
  s.backend = p.backend().empty() ? "file" : p.backend();
  s.application_module = p.application_module();
  s.bridge_directory = p.bridge_directory();
  s.container_name = p.container_name();
  s.container_image = p.container_image();
  s.gpu_devices = p.gpu_devices();
  s.memory_limit_bytes = p.memory_limit_bytes();
  s.state_directory = p.state_directory();
  s.defer_start = p.defer_start();
  if (!p.seccomp_profile().empty())
    s.seccomp_profile = p.seccomp_profile();
  s.container_args.assign(p.container_args().begin(), p.container_args().end());
  s.checkpoint_command.assign(p.checkpoint_command().begin(),
                              p.checkpoint_command().end());
  s.restore_command.assign(p.restore_command().begin(),
                           p.restore_command().end());
  s.start_command.assign(p.start_command().begin(), p.start_command().end());
  s.pause_command.assign(p.pause_command().begin(), p.pause_command().end());
  s.resume_command.assign(p.resume_command().begin(), p.resume_command().end());
  s.stop_command.assign(p.stop_command().begin(), p.stop_command().end());
  return s;
}
inline void to_proto(const JobRuntimeSpec &s, proto::RuntimeSpec *p) {
  p->Clear();
  p->set_job_id(s.job_id);
  p->set_state_path(s.state_path);
  p->set_backend(s.backend);
  p->set_application_module(s.application_module);
  p->set_bridge_directory(s.bridge_directory);
  p->set_container_name(s.container_name);
  p->set_container_image(s.container_image);
  p->set_gpu_devices(s.gpu_devices);
  p->set_memory_limit_bytes(s.memory_limit_bytes);
  p->set_state_directory(s.state_directory);
  p->set_defer_start(s.defer_start);
  p->set_seccomp_profile(s.seccomp_profile);
  for (const auto &v : s.container_args)
    p->add_container_args(v);
  for (const auto &v : s.checkpoint_command)
    p->add_checkpoint_command(v);
  for (const auto &v : s.restore_command)
    p->add_restore_command(v);
  for (const auto &v : s.start_command)
    p->add_start_command(v);
  for (const auto &v : s.pause_command)
    p->add_pause_command(v);
  for (const auto &v : s.resume_command)
    p->add_resume_command(v);
  for (const auto &v : s.stop_command)
    p->add_stop_command(v);
}
}
#endif
