# Copyright 2026 The RocketMQ Rust Authors
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


SCRIPT = Path(__file__).parents[1] / "summarize.py"
SPEC = importlib.util.spec_from_file_location("remoting_command_baseline_summarize", SCRIPT)
assert SPEC and SPEC.loader
SUMMARIZE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARIZE)


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


class SummarizeTests(unittest.TestCase):
    def test_combines_independent_process_latency_allocation_and_footprint_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for run, times, allocations, rss in [
                ("sample-01", [100.0, 220.0], 3, 16_000_000),
                ("sample-02", [120.0, 240.0], 5, 18_000_000),
            ]:
                criterion = root / "rust" / run / "demo" / "criterion" / "group" / "case" / "new"
                write_json(criterion / "sample.json", {"iters": [1.0, 2.0], "times": times})
                write_json(
                    criterion / "benchmark.json",
                    {"full_id": "group/case", "throughput": {"Bytes": 128}},
                )
                write_json(
                    root / "rust" / run / "demo" / "evidence.json",
                    {
                        "cases": [
                            {
                                "id": "allocation-case",
                                "allocations": allocations,
                                "allocatedBytes": allocations * 100,
                                "outputLen": 64,
                                "outputCapacity": 128,
                            }
                        ],
                        "objectFootprint": {
                            "sizeOfBytes": 160,
                            "objectCount": 100000,
                            "rssDeltaBytes": rss,
                        },
                    },
                )

            cases = SUMMARIZE.criterion_cases(root, 2)
            self.assertEqual(len(cases), 1)
            self.assertEqual(cases[0]["processSamples"], 2)
            self.assertEqual(cases[0]["medianNs"], 112.5)
            self.assertAlmostEqual(cases[0]["throughputPerSecond"], 128 * 1_000_000_000 / 112.5)

            allocations, footprint = SUMMARIZE.allocation_cases(root, 2)
            self.assertEqual(allocations[0]["medianAllocations"], 4.0)
            self.assertEqual(allocations[0]["medianAllocatedBytes"], 400.0)
            self.assertEqual(footprint["sizeOfBytes"], 160)
            self.assertEqual(footprint["medianRssDeltaBytes"], 17_000_000.0)

    def test_rejects_a_case_without_every_required_process_sample(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            criterion = root / "rust" / "sample-01" / "demo" / "criterion" / "group" / "case" / "new"
            write_json(criterion / "sample.json", {"iters": [1.0], "times": [100.0]})
            write_json(criterion / "benchmark.json", {"full_id": "group/case", "throughput": None})

            with self.assertRaisesRegex(ValueError, "1 process samples; expected 2"):
                SUMMARIZE.criterion_cases(root, 2)


if __name__ == "__main__":
    unittest.main()
