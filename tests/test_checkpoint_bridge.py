import copy
import pathlib
import random
import time
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace
import torch
import numpy
from transformers import BertConfig, BertForSequenceClassification
import reclaimnet_checkpoint_bridge as bridge


class ApplicationCheckpointTest(unittest.TestCase):
    def roundtrip(self, model, backend, key):
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        loss = sum(parameter.square().sum() for parameter in model.parameters())
        loss.backward()
        optimizer.step()
        training = {"step": 7, "epoch": 2}
        runtime = bridge.register(key, model, optimizer, training)
        expected_model = copy.deepcopy(model.state_dict())
        expected_optimizer = copy.deepcopy(optimizer.state_dict())
        with tempfile.TemporaryDirectory() as directory:
            spec = {"job_id": key, "backend": backend, "application_module":
                    "reclaimnet_checkpoint_bridge", "state_path": directory}
            checkpoint = str(pathlib.Path(directory) / "checkpoint.tar")
            bridge.capture(spec, checkpoint)
            with torch.no_grad():
                for parameter in model.parameters():
                    parameter.zero_()
            optimizer.state.clear()
            training["step"] = 99
            bridge.restore(spec, checkpoint)
        for name, actual in model.state_dict().items():
            torch.testing.assert_close(actual, expected_model[name], rtol=0, atol=0)
        restored_optimizer = optimizer.state_dict()
        self.assertEqual(restored_optimizer["param_groups"], expected_optimizer["param_groups"])
        for index, state in expected_optimizer["state"].items():
            for name, expected in state.items():
                actual = restored_optimizer["state"][index][name]
                if torch.is_tensor(expected):
                    torch.testing.assert_close(actual, expected, rtol=0, atol=0)
                else:
                    self.assertEqual(actual, expected)
        self.assertEqual(runtime.training_state, {"step": 7, "epoch": 2})

    def test_pytorch_model_optimizer_and_training_state(self):
        self.roundtrip(torch.nn.Linear(4, 2), "pytorch", "torch-job")

    def test_huggingface_save_pretrained_and_optimizer(self):
        config = BertConfig(vocab_size=16, hidden_size=8, num_hidden_layers=1,
                            num_attention_heads=2, intermediate_size=16)
        self.roundtrip(BertForSequenceClassification(config), "huggingface", "hf-job")

    def test_python_numpy_and_torch_random_state_roundtrip(self):
        bridge.register("random-job", torch.nn.Linear(1, 1))
        random.seed(19);numpy.random.seed(29);torch.manual_seed(39)
        with tempfile.TemporaryDirectory() as directory:
            spec = {"job_id": "random-job", "backend": "pytorch", "application_module":
                    "reclaimnet_checkpoint_bridge", "state_path": directory}
            checkpoint = str(pathlib.Path(directory) / "checkpoint.tar")
            bridge.capture(spec, checkpoint)
            expected = (random.random(), numpy.random.random(), torch.rand(3))
            random.seed(49);numpy.random.seed(59);torch.manual_seed(69)
            bridge.restore(spec, checkpoint)
            self.assertEqual(random.random(), expected[0])
            self.assertEqual(numpy.random.random(), expected[1])
            torch.testing.assert_close(torch.rand(3), expected[2], rtol=0, atol=0)

    def test_gpu_uuids_apply_budget_to_each_assigned_device(self):
        bridge.register("devices", torch.nn.Linear(1, 1))
        properties = [SimpleNamespace(uuid="GPU-a", total_memory=1000),
                      SimpleNamespace(uuid="MIG-b", total_memory=2000)]
        spec = {"job_id": "devices", "application_module": "reclaimnet_checkpoint_bridge",
                "gpu_devices": "device=GPU-a,MIG-b", "memory_limit_bytes": 400}
        with mock.patch.object(torch.cuda, "is_available", return_value=True), \
             mock.patch.object(torch.cuda, "device_count", return_value=2), \
             mock.patch.object(torch.cuda, "get_device_properties", side_effect=properties), \
             mock.patch.object(torch.cuda, "set_device") as current_device, \
             mock.patch.object(torch.cuda, "set_per_process_memory_fraction") as fraction:
            bridge._runtime(spec)
            self.assertEqual(fraction.call_args_list, [mock.call(0.2, 0), mock.call(0.1, 1)])
            current_device.assert_called_once_with(0)
        spec["gpu_devices"] = "device=GPU-unknown"
        with mock.patch.object(torch.cuda, "is_available", return_value=True), \
             mock.patch.object(torch.cuda, "device_count", return_value=2), \
             mock.patch.object(torch.cuda, "get_device_properties", side_effect=properties), \
             mock.patch.object(torch.cuda, "set_device") as current_device, \
             mock.patch.object(torch.cuda, "set_per_process_memory_fraction") as fraction:
            with self.assertRaises(ValueError):
                bridge._runtime(spec)
            fraction.assert_not_called()
            current_device.assert_not_called()

    def test_cuda_rng_is_mapped_by_assigned_logical_device_order(self):
        bridge.register("gpu-rng", torch.nn.Linear(1, 1))
        rng = [torch.tensor([1, 2, 3], dtype=torch.uint8), torch.tensor([4, 5, 6], dtype=torch.uint8)]
        with tempfile.TemporaryDirectory() as directory:
            spec = {"job_id": "gpu-rng", "backend": "pytorch", "application_module":
                    "reclaimnet_checkpoint_bridge", "state_path": directory,
                    "gpu_devices": "device=GPU-source-a,MIG-source-b"}
            source = [SimpleNamespace(uuid="GPU-source-a", total_memory=1000),
                      SimpleNamespace(uuid="GPU-unassigned", total_memory=1000),
                      SimpleNamespace(uuid="MIG-source-b", total_memory=1000)]
            checkpoint = str(pathlib.Path(directory) / "checkpoint.tar")
            with mock.patch.object(torch.cuda, "is_available", return_value=True), \
                 mock.patch.object(torch.cuda, "device_count", return_value=3), \
                 mock.patch.object(torch.cuda, "get_device_properties", side_effect=source), \
                 mock.patch.object(torch.cuda, "set_device"), \
                 mock.patch.object(torch.cuda, "get_rng_state", side_effect=rng) as get_rng:
                bridge.capture(spec, checkpoint)
                self.assertEqual(get_rng.call_args_list, [mock.call(0), mock.call(2)])
            spec["gpu_devices"] = "device=GPU-target-a,MIG-target-b"
            target = [SimpleNamespace(uuid="MIG-target-b", total_memory=1000),
                      SimpleNamespace(uuid="GPU-target-a", total_memory=1000)]
            with mock.patch.object(torch.cuda, "is_available", return_value=True), \
                 mock.patch.object(torch.cuda, "device_count", return_value=2), \
                 mock.patch.object(torch.cuda, "get_device_properties", side_effect=target * 2), \
                 mock.patch.object(torch.cuda, "set_device"), \
                 mock.patch.object(torch.cuda, "set_rng_state") as set_rng:
                bridge.restore(spec, checkpoint)
                self.assertEqual([call.args[1] for call in set_rng.call_args_list], [1, 0])
                for call, expected in zip(set_rng.call_args_list, rng):
                    torch.testing.assert_close(call.args[0], expected, rtol=0, atol=0)

    def test_training_thread_pause_resume_stop_and_release(self):
        model = torch.nn.Linear(2, 1)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        def train(runtime):
            runtime.optimizer.zero_grad()
            runtime.model(torch.ones(1, 2)).square().sum().backward()
            runtime.optimizer.step()
            runtime.training_state["step"] += 1
            time.sleep(0.002)
        runtime = bridge.register("training-thread", model, optimizer, {"step": 0},
                                  training_step=train, autostart=False)
        spec = {"job_id": "training-thread", "application_module": "reclaimnet_checkpoint_bridge"}
        def wait_for_step(minimum):
            until = time.monotonic() + 3
            while runtime.training_state["step"] < minimum and time.monotonic() < until:
                time.sleep(0.005)
            self.assertGreaterEqual(runtime.training_state["step"], minimum)
        bridge.control(spec, "start");wait_for_step(3)
        bridge.control(spec, "pause");paused = runtime.training_state["step"]
        time.sleep(0.03);self.assertEqual(runtime.training_state["step"], paused)
        bridge.control(spec, "resume");wait_for_step(paused + 3)
        bridge.control(spec, "stop");stopped = runtime.training_state["step"]
        time.sleep(0.03);self.assertEqual(runtime.training_state["step"], stopped)
        self.assertFalse(runtime.training_thread.is_alive())
        self.assertEqual(dict(optimizer.state), {})
        self.assertTrue(all(parameter.device.type == "cpu" for parameter in model.parameters()))
        self.assertNotIn("training-thread", bridge._runtimes)


if __name__ == "__main__":
    unittest.main()
