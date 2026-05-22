#include "reclaimnet/runtime.h"
#include "reclaimnet/time.h"
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
extern char **environ;
namespace reclaimnet::agent {
namespace fs = std::filesystem;
namespace {
std::string expand(std::string value, const JobRuntimeSpec &spec,
                   const std::string &path) {
  for (const auto &pair : std::vector<std::pair<std::string, std::string>>{
           {"{job_id}", spec.job_id},
           {"{state_path}", spec.state_path},
           {"{checkpoint_path}", path},
           {"{container_name}", spec.container_name}}) {
    size_t pos = 0;
    while ((pos = value.find(pair.first, pos)) != std::string::npos) {
      value.replace(pos, pair.first.size(), pair.second);
      pos += pair.second.size();
    }
  }
  return value;
}
}
VoidResult run_runtime_command(const std::vector<std::string> &command,
                               const JobRuntimeSpec &spec,
                               const std::string &path, int64_t deadline_ns) {
  if (deadline_ns > 0 && now_ns() >= deadline_ns)
    return Error{"Runtime deadline exceeded"};
  if (command.empty())
    return ok_void();
  std::vector<std::string> expanded;
  for (const auto &argument : command)
    expanded.push_back(expand(argument, spec, path));
  std::vector<char *> argv;
  for (auto &argument : expanded)
    argv.push_back(argument.data());
  argv.push_back(nullptr);
  pid_t pid;
  posix_spawnattr_t attributes;
  posix_spawnattr_init(&attributes);
  posix_spawnattr_setpgroup(&attributes, 0);
  posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETPGROUP);
  int result =
      posix_spawnp(&pid, argv[0], nullptr, &attributes, argv.data(), environ);
  posix_spawnattr_destroy(&attributes);
  if (result != 0)
    return Error{"Runtime command: " + std::string(strerror(result))};
  int status;
  while (true) {
    int waited = waitpid(pid, &status, deadline_ns > 0 ? WNOHANG : 0);
    if (waited == pid)
      break;
    if (waited < 0 && errno != EINTR)
      return Error{"Runtime wait failed"};
    if (deadline_ns > 0 && now_ns() >= deadline_ns) {
      kill(-pid, SIGKILL);
      while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
      }
      return Error{"Runtime deadline exceeded"};
    }
    if (deadline_ns > 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
    return Error{"Runtime command failed: " + expanded.front()};
  return ok_void();
}
RuntimeManager::RuntimeManager(RuntimeCleanupCb cleanup)
    : cleanup_(std::move(cleanup)) {}
VoidResult RuntimeManager::register_job(JobRuntimeSpec spec) {
  if (spec.job_id.empty() || spec.state_path.empty())
    return Error{"Runtime job id and state path are required"};
  if (spec.backend != "file" && spec.backend != "pytorch" &&
      spec.backend != "huggingface" && spec.backend != "hook")
    return Error{"Unknown application checkpoint backend"};
  if ((spec.backend == "pytorch" || spec.backend == "huggingface") &&
      spec.application_module.empty())
    return Error{"Application runtime module is required"};
  if (spec.backend == "hook" &&
      (spec.checkpoint_command.empty() || spec.restore_command.empty()))
    return Error{"Application checkpoint and restore hooks are required"};
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  stopped_.erase(spec.job_id);
  jobs_[spec.job_id] = std::move(spec);
  return ok_void();
}
Result<JobRuntimeSpec> RuntimeManager::spec(const JobId &job_id) const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  auto found = jobs_.find(job_id);
  if (found == jobs_.end())
    return Error{"No runtime registered for job " + job_id};
  return found->second;
}
Result<uint64_t> RuntimeManager::state_size(const JobId &job_id) const {
  auto runtime = spec(job_id);
  if (!runtime)
    return runtime.error();
  if (runtime.value().backend == "pytorch" ||
      runtime.value().backend == "huggingface")
    return python_application_state_size(runtime.value());
  try {
    const auto &path = runtime.value().state_path;
    if (!fs::exists(path))
      return Error{"Runtime state does not exist: " + path};
    if (!fs::is_directory(path))
      return fs::file_size(path);
    uint64_t size = 10240;
    for (const auto &entry : fs::recursive_directory_iterator(path))
      if (entry.is_regular_file())
        size += ((entry.file_size() + 511) / 512 + 1) * 512;
    return size;
  } catch (const fs::filesystem_error &error) {
    return Error{error.what()};
  }
}
VoidResult RuntimeManager::capture(const JobId &job_id, const std::string &path,
                                   bool final, int64_t deadline_ns) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  if (stopped_.count(job_id))
    return Error{"A stopped application cannot create a new checkpoint"};
  auto runtime = spec(job_id);
  if (!runtime)
    return runtime.error();
  const auto &s = runtime.value();
  if (final) {
    if (auto paused = pause(job_id, deadline_ns); !paused) {
      resume(job_id);
      return paused;
    }
  }
  VoidResult result = ok_void();
  if (!s.checkpoint_command.empty())
    result = run_runtime_command(s.checkpoint_command, s, path, deadline_ns);
  else if (s.backend == "pytorch" || s.backend == "huggingface")
    result = python_application_checkpoint(s, path, false);
  else if (s.state_directory)
    result = run_runtime_command({"tar", "-C", s.state_path, "-cf", path, "."},
                                 s, "", deadline_ns);
  else {
    std::error_code error;
    fs::copy_file(s.state_path, path, fs::copy_options::overwrite_existing,
                  error);
    if (error)
      result = Error{"Capture application state: " + error.message()};
  }
  if (result && deadline_ns > 0 && now_ns() >= deadline_ns)
    result = Error{"Runtime deadline exceeded"};
  if (!result) {
    if (final)
      resume(job_id);
    return result;
  }
  if (!fs::is_regular_file(path)) {
    if (final)
      resume(job_id);
    return Error{"Checkpoint hook did not produce a file"};
  }
  return ok_void();
}
VoidResult RuntimeManager::restore(const JobId &job_id, const std::string &path,
                                   int64_t deadline_ns) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  auto runtime = spec(job_id);
  if (!runtime)
    return runtime.error();
  const auto &s = runtime.value();
  VoidResult result = ok_void();
  if (!s.restore_command.empty())
    result = run_runtime_command(s.restore_command, s, path, deadline_ns);
  else if (s.backend == "pytorch" || s.backend == "huggingface")
    result = python_application_checkpoint(s, path, true);
  else if (s.state_directory) {
    fs::create_directories(s.state_path);
    result = run_runtime_command({"tar", "-C", s.state_path, "-xf", path}, s,
                                 "", deadline_ns);
  } else {
    auto parent = fs::path(s.state_path).parent_path();
    if (!parent.empty())
      fs::create_directories(parent);
    std::error_code error;
    fs::copy_file(path, s.state_path, fs::copy_options::overwrite_existing,
                  error);
    if (error)
      result = Error{"Restore application state: " + error.message()};
  }
  if (result && deadline_ns > 0 && now_ns() >= deadline_ns)
    result = Error{"Runtime deadline exceeded"};
  if (!result)
    return result;
  if (!s.defer_start &&
      (!s.container_name.empty() || !s.start_command.empty() ||
       s.backend == "pytorch" || s.backend == "huggingface"))
    return start(job_id, deadline_ns);
  return ok_void();
}
VoidResult RuntimeManager::control(const JobId &job_id,
                                   const std::string &action,
                                   int64_t deadline_ns) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  auto runtime = spec(job_id);
  if (!runtime)
    return runtime.error();
  const auto &s = runtime.value();
  if (action == "stop" && stopped_.count(job_id))
    return cleanup_ ? cleanup_(s) : ok_void();
  auto command = action == "start"    ? s.start_command
                 : action == "pause"  ? s.pause_command
                 : action == "resume" ? s.resume_command
                                      : s.stop_command;
  const bool python_control =
      command.empty() && (s.backend == "pytorch" || s.backend == "huggingface");
  const bool default_container = command.empty() && !s.container_name.empty();
  if (command.empty() && !s.container_name.empty()) {
    if (action == "start") {
      if (s.container_image.empty())
        return Error{"Container image is required to start a job"};
      command = {"docker", "run", "--detach", "--name", s.container_name};
      if (!s.seccomp_profile.empty())
        command.insert(command.end(),
                       {"--security-opt", "seccomp=" + s.seccomp_profile});
      auto state = fs::absolute(s.state_path);
      auto parent = s.state_directory ? state : state.parent_path();
      if (parent == parent.root_path())
        return Error{"Container state mount requires a dedicated directory"};
      fs::create_directories(parent);
      const auto container_state =
          s.state_directory ? "/reclaimnet/state"
                            : "/reclaimnet/state/" + state.filename().string();
      command.insert(
          command.end(),
          {"--mount",
           "type=bind,source=" + parent.string() + ",target=/reclaimnet/state",
           "--env", "RECLAIMNET_STATE_PATH=" + container_state});
      if (!s.gpu_devices.empty()) {
        auto devices = s.gpu_devices;
        if (devices.find(',') != std::string::npos && devices.front() != '"')
          devices = "\"" + devices + "\"";
        command.insert(command.end(), {"--gpus", devices});
      }
      command.push_back(s.container_image);
      command.insert(command.end(), s.container_args.begin(),
                     s.container_args.end());
    } else if (action == "stop")
      command = {"docker", "stop", "--timeout", "0", s.container_name};
    else
      command = {"docker", action == "resume" ? "unpause" : action,
                 s.container_name};
  }
  if (deadline_ns > 0 && now_ns() >= deadline_ns)
    return Error{"Runtime deadline exceeded"};
  auto result =
      python_control ? python_application_control(s, action) : ok_void();
  if (result)
    result = run_runtime_command(command, s, "", deadline_ns);
  if (result && action == "stop") {
    if (default_container) {
      result = run_runtime_command({"docker", "rm", s.container_name}, s, "",
                                   deadline_ns);
      if (!result)
        return result;
    }
    stopped_.insert(job_id);
    if (cleanup_)
      return cleanup_(s);
  }
  if (result && action == "start")
    stopped_.erase(job_id);
  return result;
}
VoidResult RuntimeManager::start(const JobId &id, int64_t deadline_ns) {
  return control(id, "start", deadline_ns);
}
VoidResult RuntimeManager::pause(const JobId &id, int64_t deadline_ns) {
  return control(id, "pause", deadline_ns);
}
VoidResult RuntimeManager::resume(const JobId &id, int64_t deadline_ns) {
  return control(id, "resume", deadline_ns);
}
VoidResult RuntimeManager::stop(const JobId &id, int64_t deadline_ns) {
  return control(id, "stop", deadline_ns);
}
}
