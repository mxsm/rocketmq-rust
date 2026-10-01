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

import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import publish_core_crates as release


COMMIT = "a" * 40


def crate_archive(commit, dirty=False):
    output = io.BytesIO()
    content = json.dumps({"git": {"sha1": commit, "dirty": dirty}}).encode()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        entry = tarfile.TarInfo("rocketmq-model-1.0.0/.cargo_vcs_info.json")
        entry.size = len(content)
        archive.addfile(entry, io.BytesIO(content))
    return output.getvalue()


class CoreCrateReleaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output = self.root / "release.json"
        self.packages = ["rocketmq-model", "rocketmq-common"]

    def test_verify_never_queries_registry_or_uploads(self):
        with patch.object(release, "release_packages", return_value=("1.0.0", self.packages)), \
             patch.object(release, "version_exists") as registry, \
             patch.object(release, "cargo_publish") as cargo:
            result = release.execute(self.root, "v1.0.0", "verify", None, self.output)
        cargo.assert_called_once_with(self.root, self.packages, dry_run=True)
        registry.assert_not_called()
        self.assertEqual(result["verified"], self.packages)

    def test_resume_publishes_only_missing_crates(self):
        with patch.dict(release.os.environ, {"CARGO_REGISTRY_TOKEN": "test-token"}), \
             patch.object(release.subprocess, "check_output", return_value=COMMIT), \
             patch.object(release, "release_packages", return_value=("1.0.0", self.packages)), \
             patch.object(release, "version_exists", side_effect=[True, False]), \
             patch.object(release, "cargo_publish") as cargo:
            result = release.execute(self.root, "v1.0.0", "publish", COMMIT, self.output)
        cargo.assert_called_once_with(self.root, ["rocketmq-common"], dry_run=False)
        self.assertEqual(result["existing"], ["rocketmq-model"])

    def test_registry_failure_prevents_all_uploads(self):
        with patch.dict(release.os.environ, {"CARGO_REGISTRY_TOKEN": "test-token"}), \
             patch.object(release.subprocess, "check_output", return_value=COMMIT), \
             patch.object(release, "release_packages", return_value=("1.0.0", self.packages)), \
             patch.object(release, "version_exists", side_effect=[False, release.ReleaseError("registry unavailable")]), \
             patch.object(release, "cargo_publish") as cargo:
            with self.assertRaises(release.ReleaseError):
                release.execute(self.root, "v1.0.0", "publish", COMMIT, self.output)
        cargo.assert_not_called()

    def test_checkout_mismatch_prevents_verification_and_uploads(self):
        with patch.object(release.subprocess, "check_output", return_value="b" * 40), \
             patch.object(release, "cargo_publish") as cargo:
            with self.assertRaises(release.ReleaseError):
                release.execute(self.root, "v1.0.0", "verify", COMMIT, self.output)
        cargo.assert_not_called()

    def test_existing_archive_requires_same_clean_source(self):
        metadata = json.dumps({"version": {"yanked": False}}).encode()
        for commit, dirty, accepted in [(COMMIT, False, True), ("b" * 40, False, False), (COMMIT, True, False)]:
            with self.subTest(commit=commit, dirty=dirty), \
                 patch.object(release, "fetch", side_effect=[metadata, crate_archive(commit, dirty)]):
                if accepted:
                    self.assertTrue(release.version_exists("rocketmq-model", "1.0.0", COMMIT))
                else:
                    with self.assertRaises(release.ReleaseError):
                        release.version_exists("rocketmq-model", "1.0.0", COMMIT)

    def test_yanked_version_cannot_be_republished(self):
        with patch.object(release, "fetch", return_value=b'{"version":{"yanked":true}}'):
            with self.assertRaises(release.ReleaseError):
                release.version_exists("rocketmq-model", "1.0.0", COMMIT)

    def test_only_http_404_is_missing(self):
        for status, missing in [(404, True), (403, False), (500, False)]:
            error = urllib.error.HTTPError("https://crates.io", status, "test", {}, None)
            with self.subTest(status=status), patch.object(release.urllib.request, "urlopen", side_effect=error), \
                 patch.object(release.time, "sleep"):
                if missing:
                    self.assertIsNone(release.fetch("https://crates.io", missing_ok=True))
                else:
                    with self.assertRaises(release.ReleaseError):
                        release.fetch("https://crates.io", missing_ok=True)

    def test_tag_version_mismatch_is_rejected_before_metadata(self):
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "1.0.0"\n')
        with patch.object(release.core_release_scope, "collect_metadata") as metadata:
            with self.assertRaises(release.ReleaseError):
                release.release_packages(self.root, "v1.0.1")
        metadata.assert_not_called()

    def test_partial_cargo_failure_preserves_pending_plan(self):
        with patch.dict(release.os.environ, {"CARGO_REGISTRY_TOKEN": "test-token"}), \
             patch.object(release.subprocess, "check_output", return_value=COMMIT), \
             patch.object(release, "release_packages", return_value=("1.0.0", self.packages)), \
             patch.object(release, "version_exists", return_value=False), \
             patch.object(release, "cargo_publish", side_effect=subprocess.CalledProcessError(1, "cargo")):
            with self.assertRaises(subprocess.CalledProcessError):
                release.execute(self.root, "v1.0.0", "publish", COMMIT, self.output)
        result = json.loads(self.output.read_text())
        self.assertEqual(result["pending"], self.packages)
        self.assertNotIn("published", result)

    def test_cargo_command_keeps_locked_archive_verification(self):
        with patch.object(release.subprocess, "run") as command:
            release.cargo_publish(self.root, self.packages, dry_run=False)
        args = command.call_args.args[0]
        self.assertIn("--locked", args)
        self.assertNotIn("--no-verify", args)
        self.assertNotIn("--allow-dirty", args)


if __name__ == "__main__":
    unittest.main()
