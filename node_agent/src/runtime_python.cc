#include "reclaimnet/runtime.h"
#if RECLAIMNET_HAVE_PYBIND
#include <mutex>
#include <pybind11/embed.h>
namespace py = pybind11;
#endif
namespace reclaimnet::agent {
#if RECLAIMNET_HAVE_PYBIND
namespace {
void initialize_python() {
  static std::once_flag initialized;
  std::call_once(initialized, [] {
    if (!Py_IsInitialized()) {
      PyConfig config;
      PyConfig_InitPythonConfig(&config);
      config.install_signal_handlers = 0;
#ifdef RECLAIMNET_PYTHON_EXECUTABLE
      PyConfig_SetBytesString(&config, &config.program_name,
                              RECLAIMNET_PYTHON_EXECUTABLE);
#endif
      py::initialize_interpreter(&config);
      PyEval_SaveThread();
    }
  });
}
py::module_ python_bridge(const JobRuntimeSpec &spec) {
  auto sys = py::module_::import("sys");
#ifdef RECLAIMNET_APP_BRIDGE_DIR
  sys.attr("path").attr("insert")(0, RECLAIMNET_APP_BRIDGE_DIR);
#endif
  if (!spec.bridge_directory.empty())
    sys.attr("path").attr("insert")(0, spec.bridge_directory);
  return py::module_::import("reclaimnet_checkpoint_bridge");
}
py::dict runtime_input(const JobRuntimeSpec &spec) {
  py::dict input;
  input["job_id"] = spec.job_id;
  input["state_path"] = spec.state_path;
  input["backend"] = spec.backend;
  input["application_module"] = spec.application_module;
  input["gpu_devices"] = spec.gpu_devices;
  input["memory_limit_bytes"] = spec.memory_limit_bytes;
  return input;
}
}
#endif

VoidResult python_application_checkpoint(const JobRuntimeSpec &spec,
                                         const std::string &path,
                                         bool restoring) {
#if RECLAIMNET_HAVE_PYBIND
  initialize_python();
  py::gil_scoped_acquire gil;
  try {
    auto bridge = python_bridge(spec);
    auto input = runtime_input(spec);
    bridge.attr(restoring ? "restore" : "capture")(input, path);
    return ok_void();
  } catch (const py::error_already_set &error) {
    return Error{error.what()};
  }
#else
  (void)spec;
  (void)path;
  (void)restoring;
  return Error{"Pybind11 application checkpoint support is not built"};
#endif
}
Result<uint64_t> python_application_state_size(const JobRuntimeSpec &spec) {
#if RECLAIMNET_HAVE_PYBIND
  initialize_python();
  py::gil_scoped_acquire gil;
  try {
    return python_bridge(spec)
        .attr("state_size")(runtime_input(spec))
        .cast<uint64_t>();
  } catch (const py::error_already_set &error) {
    return Error{error.what()};
  }
#else
  (void)spec;
  return Error{"Pybind11 application checkpoint support is not built"};
#endif
}
VoidResult python_application_control(const JobRuntimeSpec &spec,
                                      const std::string &action) {
#if RECLAIMNET_HAVE_PYBIND
  initialize_python();
  py::gil_scoped_acquire gil;
  try {
    python_bridge(spec).attr("control")(runtime_input(spec), action);
    return ok_void();
  } catch (const py::error_already_set &error) {
    return Error{error.what()};
  }
#else
  (void)spec;
  (void)action;
  return Error{"Pybind11 application checkpoint support is not built"};
#endif
}

}
