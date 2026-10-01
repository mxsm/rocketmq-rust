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

import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import publish_dockerhub_images as release


COMMIT = "a" * 40
DIGEST = "sha256:" + "b" * 64
REPOSITORY = "docker.io/example/rocketmq-rust-namesrv"


def state(name="namesrv", digest=DIGEST):
    return {"digest": digest, "labels": release.labels("1.0.0", COMMIT, name)}


class DockerHubReleaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "docker").mkdir()
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "1.0.0"\n')
        self.policy = {"platform": "linux/amd64", "groups": {"core": [
            {"name": "namesrv", "repository": "rocketmq-rust-namesrv", "dockerfile": "Dockerfile"},
            {"name": "broker", "repository": "rocketmq-rust-broker", "dockerfile": "Dockerfile"}]}}
        self.write_policy()

    def write_policy(self):
        (self.root / "docker/release-images.json").write_text(json.dumps(self.policy))

    def test_dry_run_never_inspects_pushes_signs_or_promotes(self):
        with patch.object(release, "ROOT", self.root), \
             patch.object(release, "run", return_value=COMMIT) as commands, \
             patch.object(release, "existing_release") as existing, \
             patch.object(release, "build") as build, \
             patch.object(release, "qualify", return_value=self.root / "sbom.json") as qualify, \
             patch.object(release, "sign") as sign, patch.object(release, "promote") as promote:
            result = release.execute("core", "1.0.0", COMMIT, "example", False, self.root / "out")
        self.assertEqual(build.call_count, 2)
        self.assertEqual(qualify.call_count, 2)
        existing.assert_not_called()
        sign.assert_not_called()
        promote.assert_not_called()
        self.assertEqual(commands.call_args_list[0].args[0], ["git", "rev-parse", "HEAD"])
        self.assertEqual(commands.call_count, 1)
        self.assertTrue(result["dry_run"])

    def test_scan_failure_prevents_every_registry_write(self):
        with patch.object(release, "ROOT", self.root), \
             patch.object(release, "run", return_value=COMMIT) as commands, \
             patch.object(release, "existing_release", return_value=None), \
             patch.object(release, "build"), \
             patch.object(release, "qualify", side_effect=[self.root / "sbom.json", subprocess.CalledProcessError(1, "trivy")]), \
             patch.object(release, "sign") as sign, patch.object(release, "promote") as promote:
            with self.assertRaises(subprocess.CalledProcessError):
                release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out")
        self.assertEqual(commands.call_count, 1)
        sign.assert_not_called()
        promote.assert_not_called()

    def test_late_source_conflict_prevents_builds_and_writes(self):
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT), \
             patch.object(release, "existing_release", side_effect=[None, release.ReleaseError("source conflict")]), \
             patch.object(release, "build") as build:
            with self.assertRaises(release.ReleaseError):
                release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out")
        build.assert_not_called()

    def test_existing_digest_is_rescanned_and_reused_without_rebuilding(self):
        self.policy["groups"]["core"] = self.policy["groups"]["core"][:1]
        self.write_policy()
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT) as commands, \
             patch.object(release, "existing_release", return_value=state()), \
             patch.object(release, "build") as build, \
             patch.object(release, "qualify", return_value=self.root / "sbom.json") as qualify, \
             patch.object(release, "sign"), patch.object(release, "promote") as promote:
            result = release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out")
        build.assert_not_called()
        self.assertEqual(commands.call_count, 1)
        qualify.assert_called_once_with(f"{REPOSITORY}@{DIGEST}", self.root / "out/namesrv", remote=True)
        promote.assert_called_once_with(REPOSITORY, DIGEST, "1.0.0", COMMIT, "namesrv")
        self.assertEqual(result["images"][0]["digest"], DIGEST)

    def test_only_explicit_manifest_absence_is_missing(self):
        reference = f"{REPOSITORY}:1.0.0"
        for message, absent in [(f"ERROR: {reference}: not found", True), ("manifest unknown", True),
                                ("unauthorized: authentication required", False),
                                ("lookup registry-1.docker.io: host not found", False),
                                ("unexpected status: 429 Too Many Requests", False)]:
            with self.subTest(message=message), patch.object(release.subprocess, "run", return_value=
                    subprocess.CompletedProcess([], 1, stdout="", stderr=message)):
                if absent:
                    self.assertIsNone(release.inspect(reference, missing_ok=True))
                else:
                    with self.assertRaises(release.ReleaseError):
                        release.inspect(reference, missing_ok=True)

    def test_partial_publication_record_does_not_claim_complete_group(self):
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT), \
             patch.object(release, "existing_release", side_effect=[state(), state("broker")]), \
             patch.object(release, "qualify", return_value=self.root / "sbom.json"), \
             patch.object(release, "sign", side_effect=[None, release.ReleaseError("signing unavailable")]), \
             patch.object(release, "promote"):
            with self.assertRaises(release.ReleaseError):
                release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out")
        result = json.loads((self.root / "out/publication.json").read_text())
        self.assertFalse(result["complete"])
        self.assertEqual(result["expected_components"], ["namesrv", "broker"])
        self.assertEqual([record["component"] for record in result["images"]], ["namesrv"])

    def test_inspection_reads_labels_from_resolved_digest(self):
        config = {"os": "linux", "architecture": "amd64", "config": {"Labels": state()["labels"]}}
        with patch.object(release.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, stdout=json.dumps({"digest": DIGEST}), stderr="")), \
             patch.object(release, "run", return_value=json.dumps(config)) as command:
            self.assertEqual(release.inspect(f"{REPOSITORY}:1.0.0"), state())
        self.assertIn(f"{REPOSITORY}@{DIGEST}", command.call_args.args[0])

    def test_conflicting_existing_tags_abort(self):
        with patch.object(release, "inspect", side_effect=[state(), state(digest="sha256:" + "c" * 64)]):
            with self.assertRaises(release.ReleaseError):
                release.existing_release(REPOSITORY, "1.0.0", COMMIT, "namesrv")

    def test_promotion_preserves_digest_and_never_overwrites(self):
        with patch.object(release, "inspect", side_effect=[None, state(), None, state()]), \
             patch.object(release, "run") as command:
            release.promote(REPOSITORY, DIGEST, "1.0.0", COMMIT, "namesrv")
        self.assertEqual(command.call_count, 2)
        for call in command.call_args_list:
            self.assertIn("--prefer-index=false", call.args[0])
            self.assertEqual(call.args[0][-1], f"{REPOSITORY}@{DIGEST}")
        with patch.object(release, "inspect", return_value=state(digest="sha256:" + "c" * 64)), \
             patch.object(release, "run") as command:
            with self.assertRaises(release.ReleaseError):
                release.promote(REPOSITORY, DIGEST, "1.0.0", COMMIT, "namesrv")
        command.assert_not_called()

    def test_digest_change_during_promotion_is_reported(self):
        with patch.object(release, "inspect", side_effect=[None, state(digest="sha256:" + "c" * 64)]), \
             patch.object(release, "run"):
            with self.assertRaises(release.ReleaseError):
                release.promote(REPOSITORY, DIGEST, "1.0.0", COMMIT, "namesrv")

    def test_sbom_attestation_must_match_scanned_predicate_and_digest(self):
        sbom = self.root / "sbom.json"
        sbom.write_text('{"bomFormat":"CycloneDX"}')
        for actual_digest, matches in [("b" * 64, True), ("c" * 64, False)]:
            statement = {"predicateType": "https://cyclonedx.org/bom", "predicate": {"bomFormat": "CycloneDX"},
                         "subject": [{"digest": {"sha256": actual_digest}}]}
            payload = json.dumps({"payload": base64.b64encode(json.dumps(statement).encode()).decode()})
            with self.subTest(digest=actual_digest), patch.object(release, "run", side_effect=["", "{}", "", payload]):
                if matches:
                    release.sign(f"{REPOSITORY}@{DIGEST}", sbom, COMMIT, "namesrv", self.root / "sign")
                else:
                    with self.assertRaises(release.ReleaseError):
                        release.sign(f"{REPOSITORY}@{DIGEST}", sbom, COMMIT, "namesrv", self.root / "sign")

    def test_sre_ui_publication_accepts_deployment_time_oidc_settings(self):
        image = json.loads((release.ROOT / "docker/release-images.json").read_text())["groups"]["sre-ui"][0]
        self.policy["groups"] = {"sre-ui": [image]}
        self.write_policy()
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT), \
             patch.dict(release.os.environ, {}, clear=True), patch.object(release, "existing_release", return_value=None), \
             patch.object(release, "build") as build, patch.object(release, "inspect", return_value=state("sre-ui")), \
             patch.object(release, "qualify", return_value=self.root / "sbom.json"), \
             patch.object(release, "sign"), patch.object(release, "promote"):
            result = release.execute("sre-ui", "1.0.0", COMMIT, "example", True, self.root / "out")
        build.assert_called_once()
        self.assertTrue(result["complete"])
        self.assertTrue(result["images"][0]["published"])

    def test_sre_ui_build_does_not_embed_deployment_identity_or_development_tokens(self):
        image = json.loads((release.ROOT / "docker/release-images.json").read_text())["groups"]["sre-ui"][0]
        with patch.object(release, "run") as command, patch.dict(release.os.environ, {
                "VITE_SRE_OIDC_AUTHORITY": "https://id.example.com", "VITE_SRE_OIDC_CLIENT_ID": "ui",
                "VITE_SRE_DEV_TOKEN": "development-fixture"}):
            release.build(image, "local/ui:test", "1.0.0", COMMIT, "linux/amd64")
        arguments = command.call_args.args[0]
        self.assertIn("VITE_SRE_AUTH_MODE=oidc", arguments)
        for value in arguments:
            self.assertNotIn("VITE_SRE_OIDC_", value)
            self.assertNotIn("VITE_SRE_DEV_", value)


if __name__ == "__main__":
    unittest.main()
