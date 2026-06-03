#!/usr/bin/env python3








import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import zipfile


VALIDATOR = r'''
#include "reclaimnet/trace.h"
#include "reclaimnet/p1_allocator.h"
#include "reclaimnet/log.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
namespace tr = reclaimnet::trace;
using J = nlohmann::json;
void require(bool value, const std::string& why) {
  if (!value) throw std::runtime_error(why);
}
template <typename T> T checked(const J& j) {
  auto r = tr::Parse<T>(j.dump());
  if (!r) throw std::runtime_error(r.error().message);
  auto output = tr::Serialize(r.value());
  if (!output) throw std::runtime_error(output.error().message);
  require(J::parse(output.value()) == j, "reader/writer round-trip mismatch");
  return std::move(r.value());
}
void near(double actual, double expected, const std::string& name) {
  require(std::isfinite(actual) && std::isfinite(expected) &&
      std::abs(actual - expected) <= std::max(1.0, std::abs(expected)) * 1e-9,
      "P1 output mismatch: " + name);
}
int main() {
  reclaimnet::Logger::instance().set_level(reclaimnet::LogLevel::ERR);
  std::unordered_map<std::string, tr::Manifest> manifests;
  std::unordered_map<std::string, uint64_t> counts;
  uint64_t members = 0;
  std::string line, location;
  try {
    while (std::getline(std::cin, line)) {
      auto envelope = J::parse(line);
      auto kind = envelope.at("kind").get<std::string>();
      auto run = envelope.at("run").get<std::string>();
      location = envelope.at("location").get<std::string>();
      const auto& j = envelope.at("data");
      if (kind == "schema") checked<tr::Schema>(j);
      else if (kind == "manifest") manifests[run] = checked<tr::Manifest>(j);
      else if (kind == "nodes") {
        auto nodes = tr::ParseNodes(j.dump());
        if (!nodes) throw std::runtime_error(nodes.error().message);
        auto output = tr::SerializeNodes(nodes.value());
        if (!output) throw std::runtime_error(output.error().message);
        require(J::parse(output.value()) == j, "node inventory round-trip mismatch");
      } else if (kind == "bandwidth") checked<tr::BandwidthSample>(j);
      else if (kind == "hazard") checked<tr::HazardSample>(j);
      else if (kind == "reclaim") checked<tr::ReclaimEvent>(j);
      else if (kind == "telemetry") checked<tr::Telemetry>(j);
      else if (kind == "job") checked<tr::Job>(j);
      else if (kind == "flow") checked<tr::Flow>(j);
      else if (kind == "migration") checked<tr::Migration>(j);
      else if (kind == "p1") {
        auto epoch = checked<tr::P1Epoch>(j);
        const auto input = tr::ToP1Inputs<reclaimnet::control::P1JobInput>(
            epoch, manifests.at(run).p1_loss_budget_s);
        const auto output = reclaimnet::control::P1Allocator(
            epoch.allocation_budget_bps).compute(input);
        require(output.schedules.size() == epoch.members.size(), "P1 member count mismatch");
        near(output.total_bps_used, epoch.total_allocated_bps, "total_allocated_bps");
        for (size_t i = 0; i < epoch.members.size(); ++i) {
          const auto& actual = output.schedules[i];
          const auto& expected = epoch.members[i];
          require(actual.job_id == expected.job_id, "P1 job identity mismatch");
          require(actual.feasible == expected.constraints_feasible, "P1 feasibility mismatch");
          near(actual.b_i_bps, expected.b_i_bps, "b_i_bps");
          near(actual.write_rate_limit_bps, expected.write_rate_limit_bps, "write_rate_limit_bps");
          near(actual.raw_delta_t_s, expected.p1_raw_interval_s, "raw_interval_s");
          near(actual.lower_bound_s, expected.p1_lower_interval_s, "lower_interval_s");
          near(actual.upper_bound_s, expected.p1_upper_interval_s, "upper_interval_s");
          near(actual.delta_t_s, expected.target_interval_s, "target_interval_s");
          ++members;
        }
      } else throw std::runtime_error("unknown trace family");
      ++counts[kind];
    }
    require(!std::cin.bad(), "input pipe read failure");
    std::cout << J{{"runs", manifests.size()}, {"records", counts},
                   {"p1_members_replayed", members}}.dump() << '\n';
    return 0;
  } catch (const std::exception& e) {
    std::cerr << location << ": " << e.what() << '\n';
    return 1;
  }
}
'''

FAMILIES = {
    "bandwidth_samples.jsonl": "bandwidth",
    "hazard_samples.jsonl": "hazard",
    "reclaims.jsonl": "reclaim",
    "telemetry.jsonl": "telemetry",
    "jobs.jsonl": "job",
    "flows.jsonl": "flow",
    "migrations.jsonl": "migration",
    "p1_allocation_epochs.jsonl": "p1",
}


def main():
    code_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Validate trace compatibility with the C++ protocols.")
    parser.add_argument("archive", nargs="?", type=Path, default=code_dir.parent / "trace_all.zip")
    parser.add_argument("--full", action="store_true", help="Validate all rows in every supported JSONL family")
    parser.add_argument("--rows", type=int, default=100, help="Rows per non-P1 family per run (default: 100)")
    parser.add_argument("--cxx", default=os.environ.get("CXX", "c++"))
    parser.add_argument("--json-include", type=Path, help="Directory containing nlohmann/json.hpp")
    args = parser.parse_args()
    if args.rows <= 0:
        parser.error("--rows must be positive; use --full for unbounded streaming")
    if not args.archive.is_file():
        parser.error(f"archive does not exist: {args.archive}")

    with tempfile.TemporaryDirectory(prefix="reclaimnet-trace-validator-") as temp:
        temp_dir = Path(temp)
        source, executable = temp_dir / "validator.cc", temp_dir / "validator"
        source.write_text(VALIDATOR, encoding="utf-8")
        command = [args.cxx, "-std=c++17", "-O2", "-pthread", "-DRECLAIMNET_HAVE_JSON=1",
                   "-I" + str(code_dir / "common/include"),
                   "-I" + str(code_dir / "control_plane/include")]
        include = args.json_include
        if include is None:
            include = next((p for p in (Path("/opt/homebrew/include"), Path("/usr/local/include"))
                            if (p / "nlohmann/json.hpp").is_file()), None)
        if include:
            command.append("-I" + str(include))
        command.extend([str(source), str(code_dir / "common/src/trace.cc"),
                        str(code_dir / "common/src/log.cc"),
                        str(code_dir / "control_plane/src/p1_allocator.cc"), "-o", str(executable)])
        built = subprocess.run(command, capture_output=True, text=True)
        if built.returncode:
            raise SystemExit("C++ trace validator build failed (C++17 and nlohmann_json required):\n" + built.stderr)
        process = subprocess.Popen([str(executable)], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with zipfile.ZipFile(args.archive) as archive:
                runs = sorted(name.rsplit("/", 1)[0] for name in archive.namelist()
                              if name.endswith("/manifest.json"))
                if len(runs) != 5:
                    raise ValueError(f"expected five trace runs, found {len(runs)}")

                def emit(run, kind, data, location):
                    process.stdin.write(json.dumps({"run": run, "kind": kind, "data": data,
                                                    "location": location}, allow_nan=False) + "\n")

                for run in runs:
                    manifest = json.loads(archive.read(run + "/manifest.json"))
                    for name, info in manifest["files"].items():
                        actual = archive.getinfo(run + "/" + name).file_size
                        if actual != info["bytes"]:
                            raise ValueError(f"{run}/{name}: manifest byte count mismatch")
                    for name, kind in (("schema.json", "schema"), ("manifest.json", "manifest"), ("nodes.json", "nodes")):
                        emit(run, kind, json.loads(archive.read(run + "/" + name)), run + "/" + name)
                    for name, kind in FAMILIES.items():
                        with archive.open(run + "/" + name) as stream:
                            for line_no, line in enumerate(stream, 1):
                                if not line.strip():
                                    continue
                                emit(run, kind, json.loads(line), f"{run}/{name}:{line_no}")
                                if kind != "p1" and not args.full and line_no >= args.rows:
                                    break
            process.stdin.close()
            process.stdin = None
            stdout, stderr = process.communicate()
            if process.returncode:
                raise SystemExit("C++ trace compatibility failed:\n" + stderr)
            result = json.loads(stdout)
            result["non_p1_validation"] = "all rows" if args.full else f"first {args.rows} rows per family per run"
            result["p1_validation"] = "all epochs via actual C++ allocator"
            print(json.dumps(result, indent=2, sort_keys=True))
        except BrokenPipeError:
            process.stdin = None
            stdout, stderr = process.communicate()
            raise SystemExit("C++ trace compatibility failed:\n" + stderr)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    main()
