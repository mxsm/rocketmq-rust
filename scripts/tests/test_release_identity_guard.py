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

import copy
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts.tests.release_test_support import ROOT, read_json, write_json


SCRIPT = ROOT / "scripts" / "release_identity_guard.py"
IDENTITY = ROOT / "distribution" / "release-identity.json"
SCHEMA = ROOT / "distribution" / "release-identity.schema.json"


class ReleaseIdentityGuardTests(unittest.TestCase):
    def run_guard(
        self,
        *,
        identity: dict[str, object] | None = None,
        schema: dict[str, object] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temporary:
            temporary_root = Path(temporary)
            command = [sys.executable, str(SCRIPT)]
            if identity is not None:
                identity_path = temporary_root / "release-identity.json"
                write_json(identity_path, identity)
                command.extend(("--identity", str(identity_path)))
            else:
                command.extend(("--identity", str(IDENTITY)))
            if schema is not None:
                schema_path = temporary_root / "release-identity.schema.json"
                write_json(schema_path, schema)
                command.extend(("--schema", str(schema_path)))
            command.extend(("--stage", "preflight"))
            return subprocess.run(
                command,
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

    def test_approved_community_identity_passes_preflight(self) -> None:
        self.assertTrue(SCRIPT.is_file(), f"missing release identity guard: {SCRIPT}")
        completed = self.run_guard()

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("unofficial-community", completed.stdout)
        self.assertIn("approver=mxsm", completed.stdout)
        self.assertIn("scope=core-release-1.0", completed.stdout)

    def test_preflight_rejects_unset_identity_kind(self) -> None:
        identity = read_json(IDENTITY)
        identity["identity_kind"] = "unset"

        completed = self.run_guard(identity=identity)

        self.assertEqual(1, completed.returncode)
        self.assertIn("identity_kind", completed.stdout + completed.stderr)

    def test_preflight_rejects_blank_approver(self) -> None:
        identity = read_json(IDENTITY)
        identity["approval"]["approver"] = "  "

        completed = self.run_guard(identity=identity)

        self.assertEqual(1, completed.returncode)
        self.assertIn("approval.approver", completed.stdout + completed.stderr)

    def test_preflight_rejects_future_approval_date(self) -> None:
        identity = read_json(IDENTITY)
        identity["approval"]["approved_on"] = "2999-01-01"

        completed = self.run_guard(identity=identity)

        self.assertEqual(1, completed.returncode)
        self.assertIn("approval.approved_on", completed.stdout + completed.stderr)

    def test_community_identity_cannot_claim_apache_release(self) -> None:
        identity = read_json(IDENTITY)
        identity["official_apache_release"] = True

        completed = self.run_guard(identity=identity)

        self.assertEqual(1, completed.returncode)
        self.assertIn("official_apache_release", completed.stdout + completed.stderr)

    def test_preflight_requires_publication_namespace(self) -> None:
        identity = read_json(IDENTITY)
        identity["oci"]["namespace"] = ""

        completed = self.run_guard(identity=identity)

        self.assertEqual(1, completed.returncode)
        self.assertIn("oci.namespace", completed.stdout + completed.stderr)

    def test_preflight_requires_every_release_consumer(self) -> None:
        identity = read_json(IDENTITY)
        identity["required_consumers"].remove("public-staged-metadata")

        completed = self.run_guard(identity=identity)

        self.assertEqual(1, completed.returncode)
        self.assertIn("required_consumers", completed.stdout + completed.stderr)

    def test_preflight_rejects_repository_drift(self) -> None:
        identity = read_json(IDENTITY)
        identity["project"]["repository"] = "https://example.invalid/rocketmq-rust"

        completed = self.run_guard(identity=identity)

        self.assertEqual(1, completed.returncode)
        self.assertIn("project.repository", completed.stdout + completed.stderr)

    def test_schema_cannot_authorize_digest_fields(self) -> None:
        identity = read_json(IDENTITY)
        schema = copy.deepcopy(read_json(SCHEMA))
        identity["artifact_digest"] = "forbidden"
        schema["required"].append("artifact_digest")
        schema["properties"]["artifact_digest"] = {"type": "string"}

        completed = self.run_guard(identity=identity, schema=schema)

        self.assertEqual(1, completed.returncode)
        self.assertIn("artifact_digest", completed.stdout + completed.stderr)

    def test_schema_objects_must_be_closed(self) -> None:
        schema = copy.deepcopy(read_json(SCHEMA))
        schema["properties"]["oci"]["additionalProperties"] = True

        completed = self.run_guard(schema=schema)

        self.assertEqual(1, completed.returncode)
        self.assertIn("additionalProperties", completed.stdout + completed.stderr)


if __name__ == "__main__":
    unittest.main()
