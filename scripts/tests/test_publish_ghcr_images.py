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
from contextlib import ExitStack
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import publish_ghcr_images as release
from publish_dockerhub_images import labels

COMMIT = "a" * 40
DIGEST = "sha256:" + "b" * 64
SOURCE = "docker.io/example/rocketmq-rust-namesrv"


def state(name="namesrv", digest=DIGEST):
    return {"digest": digest, "labels": labels("1.0.0", COMMIT, name)}


def attestation(repository=SOURCE, digest=DIGEST, statement_type="https://in-toto.io/Statement/v0.1"):
    statement = {"_type": statement_type, "predicateType": "https://cyclonedx.org/bom",
                 "subject": [{"name": repository, "digest": {"sha256": digest[7:]}}],
                 "predicate": {"bomFormat": "CycloneDX", "components": []}}
    return json.dumps({"payload": base64.b64encode(json.dumps(statement).encode()).decode()})


class GhcrReleaseTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.images = [{"name": name, "repository": f"rocketmq-rust-{name}"} for name in ("namesrv", "broker")]
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(release, "validate_source"))
        self.stack.enter_context(patch.object(release, "selected_images", return_value=self.images))
        self.commands = self.stack.enter_context(patch.object(release, "run", return_value="1.0.0\n"))
        self.inspect = self.stack.enter_context(patch.object(release, "inspect"))
        self.destination = self.stack.enter_context(patch.object(release, "destination_state", return_value=None))
        self.verify = self.stack.enter_context(patch.object(release, "verify_source"))
        self.scan = self.stack.enter_context(patch.object(release, "scan"))

    def execute(self, publish=True):
        return release.execute("all", "1.0.0", COMMIT, "example", "mxsm", publish, self.root)

    def copies(self):
        return [call.args[0] for call in self.commands.call_args_list if call.args[0][:2] == ["crane", "copy"]]

    def test_dry_run_qualifies_sources_without_any_destination_write_or_signing(self):
        self.inspect.side_effect = [state(), state("broker")]
        result = self.execute(False)
        self.assertTrue(result["complete"])
        self.assertTrue(result["dry_run"])
        self.assertEqual(self.verify.call_count, 2)
        self.assertEqual(self.scan.call_count, 2)
        self.assertEqual(self.copies(), [])
        self.assertTrue(all("docker.io/" in call.args[0] for call in self.inspect.call_args_list))
        self.assertFalse(any(call.args[0][0] == "cosign" for call in self.commands.call_args_list))

    def test_late_destination_conflict_prevents_all_copies_and_scans(self):
        self.inspect.side_effect = [state(), state("broker")]
        self.destination.side_effect = [None, state("broker", "sha256:" + "c" * 64)]
        with self.assertRaises(release.ReleaseError):
            self.execute()
        self.assertEqual(self.copies(), [])
        self.scan.assert_not_called()

    def test_late_scan_failure_prevents_all_registry_writes(self):
        self.inspect.side_effect = [state(), state("broker")]
        self.scan.side_effect = [None, subprocess.CalledProcessError(1, ["trivy"])]
        with self.assertRaises(subprocess.CalledProcessError):
            self.execute()
        self.assertEqual(self.copies(), [])
        self.assertFalse(json.loads((self.root / "publication.json").read_text())["complete"])

    def test_signature_failure_prevents_scan_and_copy(self):
        self.inspect.side_effect = [state(), state("broker")]
        self.verify.side_effect = release.ReleaseError("signature mismatch")
        with self.assertRaises(release.ReleaseError):
            self.execute()
        self.scan.assert_not_called()
        self.assertEqual(self.copies(), [])

    def test_copy_preserves_digest_and_writes_only_version_tags(self):
        self.inspect.side_effect = [state(), state("broker"), state(), state("broker")]
        (self.root / "namesrv.trivy.json").write_text('{"Results":[]}')
        result = self.execute()
        self.assertEqual(self.copies(), [
            ["crane", "copy", f"{SOURCE}@{DIGEST}", "ghcr.io/mxsm/rocketmq-rust/namesrv:1.0.0"],
            ["crane", "copy", f"docker.io/example/rocketmq-rust-broker@{DIGEST}", "ghcr.io/mxsm/rocketmq-rust/broker:1.0.0"]])
        self.assertEqual(len(result["images"]), 2)
        self.assertEqual(len(result["artifacts"]["namesrv.trivy.json"]), 64)
        self.assertTrue(all(image["tags"] == ["1.0.0"] for image in result["images"]))

    def test_matching_destination_is_verified_and_skipped(self):
        self.inspect.side_effect = [state(), state("broker"), state(), state("broker")]
        self.destination.side_effect = [state(), state("broker")]
        self.execute()
        self.assertEqual(self.copies(), [])
        self.assertEqual(self.scan.call_count, 2)

    def test_wrong_readback_digest_does_not_claim_complete_release(self):
        self.inspect.side_effect = [state(), state("broker"), state(digest="sha256:" + "c" * 64)]
        with self.assertRaises(release.ReleaseError):
            self.execute()
        self.assertFalse(json.loads((self.root / "publication.json").read_text())["complete"])

    def test_only_explicit_registry_absence_is_treated_as_missing(self):
        # Exercise the real inspector, outside the orchestration mock.
        self.stack.close()
        for message, absent in [("MANIFEST_UNKNOWN: manifest unknown", True),
                                ("NAME_UNKNOWN: repository not found", True),
                                ("UNAUTHORIZED: authentication required", False),
                                ("TOOMANYREQUESTS", False), ("lookup ghcr.io: host not found", False)]:
            with self.subTest(message=message), patch.object(release.subprocess, "run", return_value=
                    subprocess.CompletedProcess([], 1, stdout="", stderr=message)):
                if absent:
                    self.assertIsNone(release.inspect("ghcr.io/mxsm/rocketmq-rust/namesrv:1.0.0", missing_ok=True))
                else:
                    with self.assertRaises(release.ReleaseError):
                        release.inspect("ghcr.io/mxsm/rocketmq-rust/namesrv:1.0.0", missing_ok=True)

    def test_image_index_is_rejected(self):
        self.stack.close()
        with patch.object(release.subprocess, "run", return_value=
                subprocess.CompletedProcess([], 0, stdout=DIGEST, stderr="")), \
             patch.object(release, "run", return_value=json.dumps({"mediaType": "application/vnd.oci.image.index.v1+json", "manifests": []})):
            with self.assertRaises(release.ReleaseError):
                release.inspect(f"{SOURCE}:1.0.0")

    def test_new_package_absence_requires_api_404_not_registry_denial(self):
        self.stack.close()
        for status in ("HTTP 404", "HTTP 403", "HTTP 429", "connection failed"):
            with self.subTest(status=status), patch.object(release.subprocess, "run", return_value=
                    subprocess.CompletedProcess([], 1, stdout="", stderr=status)), \
                 patch.object(release, "inspect") as inspector:
                if status == "HTTP 404":
                    self.assertIsNone(release.destination_state("mxsm", "namesrv", "1.0.0"))
                else:
                    with self.assertRaises(release.ReleaseError):
                        release.destination_state("mxsm", "namesrv", "1.0.0")
                inspector.assert_not_called()

    def test_sbom_accepts_both_statement_versions_and_canonical_docker_name(self):
        for version in ("v0.1", "v1"):
            release.verify_sbom_subject(attestation(SOURCE.replace("docker.io", "index.docker.io"),
                statement_type=f"https://in-toto.io/Statement/{version}"), SOURCE, DIGEST)

    def test_sbom_rejects_unknown_type_wrong_repository_or_wrong_digest(self):
        for envelope in (attestation(statement_type="https://in-toto.io/Statement/v2"),
                         attestation(repository="docker.io/attacker/namesrv"),
                         attestation(digest="sha256:" + "c" * 64)):
            with self.assertRaises(release.ReleaseError):
                release.verify_sbom_subject(envelope, SOURCE, DIGEST)

    def test_shared_catalog_selects_the_same_components_for_all_and_each_group(self):
        self.stack.close()
        policy = json.loads((release.ROOT / "docker/release-images.json").read_text())
        all_images = release.selected_images("all")
        self.assertEqual(all_images, [image for group in policy["groups"] for image in release.selected_images(group)])
        self.assertEqual(len({image["name"] for image in all_images}), len(all_images))
        with self.assertRaises(KeyError):
            release.selected_images("unknown")


if __name__ == "__main__":
    unittest.main()
