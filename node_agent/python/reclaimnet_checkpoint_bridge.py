import copy
import contextlib
import importlib
import pathlib
import random
import tarfile
import tempfile
import threading

_runtimes = {}


class ApplicationRuntime:
    def __init__(self, model, optimizer=None, training_state=None, extra_files=(), lock=None,
                 training_step=None, autostart=True):
        self.model = model
        self.optimizer = optimizer
        self.training_state = training_state if training_state is not None else {}
        self.extra_files = tuple(extra_files)
        self.lock = lock if lock is not None else threading.RLock()
        self.training_function = training_step
        self.run_event = threading.Event()
        self.stop_event = threading.Event()
        self.training_thread = None
        self.training_threads = []
        self.assigned_devices = tuple(sorted({tensor.device.index for tensor in
                                             tuple(model.parameters()) + tuple(model.buffers())
                                             if tensor.device.type == "cuda"}))
        if autostart:
            self.start()

    @contextlib.contextmanager
    def training_step(self):
        while not self.stop_event.is_set():
            if not self.run_event.wait(0.05):
                continue
            with self.lock:
                if self.stop_event.is_set():
                    break
                if not self.run_event.is_set():
                    continue
                yield True
                return
        yield False

    def attach_training_thread(self, thread):
        self.training_threads.append(thread)

    def _train(self):
        if self.assigned_devices:
            import torch
            torch.cuda.set_device(self.assigned_devices[0])
        while True:
            with self.training_step() as active:
                if not active:
                    return
                self.training_function(self)

    def start(self):
        self.stop_event.clear()
        self.run_event.set()
        if self.training_function is not None and (self.training_thread is None or not self.training_thread.is_alive()):
            self.training_thread = threading.Thread(target=self._train, daemon=True)
            self.attach_training_thread(self.training_thread)
            self.training_thread.start()

    def pause(self):
        self.run_event.clear()
        with self.lock:
            pass

    def resume(self):
        self.start()

    def stop(self):
        self.stop_event.set()
        self.run_event.set()
        for thread in self.training_threads:
            if thread is not threading.current_thread():
                thread.join()
        self.run_event.clear()
        self.release_cuda()

    def release_cuda(self):
        import torch
        with self.lock:
            self.model.cpu()
            if self.optimizer is not None:
                self.optimizer.state.clear()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()


def register(job_id, model, optimizer=None, training_state=None, extra_files=(), lock=None,
             training_step=None, autostart=True):
    runtime = ApplicationRuntime(model, optimizer, training_state, extra_files, lock, training_step, autostart)
    _runtimes[job_id] = runtime
    return runtime


def get_runtime(job_id):
    return _runtimes[job_id]


def control(spec, action):
    runtime = _runtime(spec)
    module = importlib.import_module(spec["application_module"])
    hook = getattr(module, action + "_runtime", None)
    if hook is not None:
        hook(spec["job_id"])
        if action == "stop":
            runtime.release_cuda()
    else:
        getattr(runtime, action)()
    if action == "stop":
        _runtimes.pop(spec["job_id"], None)


def _runtime(spec):
    if spec.get("gpu_devices"):
        import torch
        if not torch.cuda.is_available():
            raise RuntimeError("The registered GPU application requires CUDA")
        requested = spec["gpu_devices"].strip('"').removeprefix("device=").split(",")
        properties = [torch.cuda.get_device_properties(index)
                      for index in range(torch.cuda.device_count())]
        def canonical(value):
            return str(value).casefold().removeprefix("mig-").removeprefix("gpu-")
        devices = []
        if requested == ["all"]:
            devices = list(range(len(properties)))
        else:
            for value in requested:
                if value.isdigit() and int(value) < len(properties):
                    devices.append(int(value))
                    continue
                matched = [index for index, prop in enumerate(properties)
                           if canonical(prop.uuid) == canonical(value)]
                if len(matched) != 1:
                    raise ValueError("Assigned GPU UUID is not visible: " + value)
                devices.append(matched[0])
        budget = spec.get("memory_limit_bytes", 0)
        if budget:
            for device in devices:
                total = properties[device].total_memory
                torch.cuda.set_per_process_memory_fraction(budget / len(devices) / total, device)
        torch.cuda.set_device(devices[0])
    module = importlib.import_module(spec["application_module"])
    runtime = module.get_runtime(spec["job_id"])
    if spec.get("gpu_devices"):
        runtime.assigned_devices = tuple(devices)
    return runtime


def _cpu_snapshot(value):
    import torch
    if torch.is_tensor(value):
        return value.detach().cpu().clone()
    if isinstance(value, dict):
        snapshot = type(value)((key, _cpu_snapshot(item)) for key, item in value.items())
        if hasattr(value, "_metadata"):
            snapshot._metadata = copy.deepcopy(value._metadata)
        return snapshot
    if isinstance(value, list):
        return [_cpu_snapshot(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_cpu_snapshot(item) for item in value)
    return copy.deepcopy(value)


def _cuda_devices(runtime):
    if runtime.assigned_devices:
        return runtime.assigned_devices
    return tuple(sorted({tensor.device.index for tensor in
                         tuple(runtime.model.parameters()) + tuple(runtime.model.buffers())
                         if tensor.device.type == "cuda"}))


def state_size(spec):
    import io
    import torch
    runtime = _runtime(spec)
    with runtime.lock:
        state = {"model": runtime.model.state_dict(),
                 "optimizer": runtime.optimizer.state_dict() if runtime.optimizer else None,
                 "training_state": runtime.training_state}
        output = io.BytesIO()
        torch.save(state, output)
        size = output.tell() + 32768
        for filename in runtime.extra_files:
            size += pathlib.Path(filename).stat().st_size + 512
        return size


def capture(spec, checkpoint_path):
    import numpy
    import torch
    runtime = _runtime(spec)
    with runtime.lock, tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        numpy_state = numpy.random.get_state()
        state = {
            "optimizer": _cpu_snapshot(runtime.optimizer.state_dict()) if runtime.optimizer else None,
            "training_state": _cpu_snapshot(runtime.training_state),
            "rng_cpu": torch.get_rng_state(),
            "rng_cuda": [torch.cuda.get_rng_state(device) for device in _cuda_devices(runtime)],
            "rng_python": random.getstate(),
            "rng_numpy": (numpy_state[0], numpy_state[1].tolist(), numpy_state[2], numpy_state[3], numpy_state[4]),
        }
        if spec["backend"] == "huggingface":
            runtime.model.save_pretrained(root / "model")
        else:
            state["model"] = _cpu_snapshot(runtime.model.state_dict())
        torch.save(state, root / "state.pt")
        with tarfile.open(checkpoint_path, "w") as archive:
            for path in sorted(root.iterdir()):
                archive.add(path, arcname=path.name)
            for filename in runtime.extra_files:
                path = pathlib.Path(filename)
                archive.add(path, arcname="application/" + path.name)


def restore(spec, checkpoint_path):
    import numpy
    import torch
    runtime = _runtime(spec)
    control(spec, "pause")
    with runtime.lock, tempfile.TemporaryDirectory() as directory:
        root = pathlib.Path(directory)
        with tarfile.open(checkpoint_path, "r") as archive:
            archive.extractall(root, filter="data")
        state = torch.load(root / "state.pt", map_location="cpu", weights_only=True)
        devices = _cuda_devices(runtime)
        if len(state["rng_cuda"]) != len(devices):
            raise ValueError("Checkpoint logical GPU count differs from the assigned destination devices")
        if spec["backend"] == "huggingface":
            restored = type(runtime.model).from_pretrained(root / "model", local_files_only=True)
            runtime.model.load_state_dict(restored.state_dict())
        else:
            runtime.model.load_state_dict(state["model"])
        if state["optimizer"] is not None:
            if runtime.optimizer is None:
                raise ValueError("A training optimizer is required to restore this checkpoint")
            runtime.optimizer.load_state_dict(state["optimizer"])
        runtime.training_state.clear()
        runtime.training_state.update(state["training_state"])
        torch.set_rng_state(state["rng_cpu"])
        for device, rng in zip(devices, state["rng_cuda"]):
            torch.cuda.set_rng_state(rng, device)
        random.setstate(state["rng_python"])
        numpy_state = state["rng_numpy"]
        numpy.random.set_state((numpy_state[0], numpy.asarray(numpy_state[1], dtype=numpy.uint32),
                                numpy_state[2], numpy_state[3], numpy_state[4]))
        application = root / "application"
        if application.exists():
            destination = pathlib.Path(spec["state_path"])
            destination.mkdir(parents=True, exist_ok=True)
            for path in application.iterdir():
                import shutil
                shutil.copy2(path, destination / path.name)
