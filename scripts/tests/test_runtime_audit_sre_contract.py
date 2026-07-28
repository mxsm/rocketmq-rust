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

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNTIME_AUDIT = ROOT / "scripts" / "runtime-audit.ps1"


class RuntimeAuditSreContractTest(unittest.TestCase):
    def test_executor_verification_poll_has_a_narrow_bounded_disposition(self) -> None:
        script = RUNTIME_AUDIT.read_text(encoding="utf-8-sig")

        self.assertIn(
            '$path -eq "rocketmq-sre/crates/rocketmq-sre-executor/src/verifier.rs"',
            script,
        )
        self.assertIn('Disposition = "sre-bounded-verification-poll"', script)
        self.assertIn("descriptor maximum wait and hard observation limit", script)

        broad_allowlist = '$path -match "^rocketmq-sre/crates/rocketmq-sre-executor/'
        self.assertNotIn(broad_allowlist, script)


if __name__ == "__main__":
    unittest.main()
