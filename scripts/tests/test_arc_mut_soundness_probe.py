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

from pathlib import Path
import subprocess
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from arc_mut_soundness_probe import ProbeFailure  # noqa: E402
from arc_mut_soundness_probe import validate_alias_result  # noqa: E402
from arc_mut_soundness_probe import validate_guarded_result  # noqa: E402


def result(returncode: int, output: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["cargo", "miri"], returncode=returncode, stdout="", stderr=output
    )


class ArcMutSoundnessProbeTests(unittest.TestCase):
    def test_guarded_path_requires_success(self) -> None:
        validate_guarded_result(result(0))
        with self.assertRaises(ProbeFailure):
            validate_guarded_result(result(1, "tool failure"))

    def test_alias_path_requires_a_nonzero_exit(self) -> None:
        with self.assertRaises(ProbeFailure):
            validate_alias_result(result(0))

    def test_alias_path_rejects_unrelated_failures(self) -> None:
        with self.assertRaises(ProbeFailure):
            validate_alias_result(result(1, "dependency download failed"))

    def test_alias_path_accepts_reference_aliasing_undefined_behavior(self) -> None:
        validate_alias_result(
            result(
                1,
                "error: Undefined Behavior: tag does not exist in the borrow stack\n"
                "help: tag was invalidated by a Unique retag",
            )
        )


if __name__ == "__main__":
    unittest.main()
