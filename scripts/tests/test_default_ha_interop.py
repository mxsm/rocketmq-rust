# Copyright 2023 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "scripts" / "interop" / "run_default_ha_interop.py"
MATRIX_PATH = ROOT / "scripts" / "interop" / "default-ha-matrix.json"


def load_runner():
    spec = importlib.util.spec_from_file_location("run_default_ha_interop", RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load DefaultHA interop runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class DefaultHaInteropContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = load_runner()
        cls.matrix = cls.runner.load_matrix(MATRIX_PATH)

    def test_matrix_covers_both_directions_and_required_faults(self) -> None:
        scenarios = self.matrix["scenarios"]
        directions = {scenario["direction"] for scenario in scenarios}
        faults = {(scenario["direction"], scenario["fault"]) for scenario in scenarios}

        self.assertEqual(directions, {"java-master-rust-slave", "rust-master-java-slave"})
        for direction in directions:
            self.assertIn((direction, "none"), faults)
            self.assertIn((direction, "reconnect-resume"), faults)
            self.assertIn((direction, "tail-truncate"), faults)
            self.assertIn((direction, "slow-replica"), faults)

    def test_matrix_rejects_out_of_scope_ha_modes(self) -> None:
        mutated = json.loads(json.dumps(self.matrix))
        mutated["haMode"] = "AutoSwitchHA"
        with self.assertRaisesRegex(ValueError, "DefaultHA"):
            self.runner.validate_matrix(mutated)

    def test_result_requires_every_assertion_and_evidence_field(self) -> None:
        scenario = self.matrix["scenarios"][0]
        result = {
            "id": scenario["id"],
            "status": "passed",
            "direction": scenario["direction"],
            "assertions": {name: True for name in scenario["requiredAssertions"]},
            "evidence": {name: f"evidence/{name}.json" for name in scenario["requiredEvidence"]},
        }
        self.runner.validate_scenario_result(scenario, result)

        result["status"] = "skipped"
        with self.assertRaisesRegex(ValueError, "passed"):
            self.runner.validate_scenario_result(scenario, result)

    def test_aggregate_rejects_missing_scenario_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "missing scenario result"):
                self.runner.validate_result_set(self.matrix["scenarios"], Path(directory))

    def test_aggregate_rejects_missing_or_escaping_evidence(self) -> None:
        scenario = self.matrix["scenarios"][0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scenarios = root / "scenarios"
            scenarios.mkdir()
            result = {
                "id": scenario["id"],
                "status": "passed",
                "direction": scenario["direction"],
                "assertions": {name: True for name in scenario["requiredAssertions"]},
                "evidence": {name: f"evidence/{name}.json" for name in scenario["requiredEvidence"]},
            }
            (scenarios / f"{scenario['id']}.json").write_text(json.dumps(result), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing masterLog evidence"):
                self.runner.validate_result_set([scenario], scenarios)

            result["evidence"]["masterLog"] = "../outside.log"
            (scenarios / f"{scenario['id']}.json").write_text(json.dumps(result), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "escapes the output directory"):
                self.runner.validate_result_set([scenario], scenarios)


if __name__ == "__main__":
    unittest.main()
