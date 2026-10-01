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
import io
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
REAL_CLEANUP = release.cleanup_aliases


def state(name="namesrv", digest=DIGEST):
    return {"digest": digest, "labels": release.labels("1.0.0", COMMIT, name)}


def attestation(predicate, digest=DIGEST):
    statement = {"predicateType": "https://cyclonedx.org/bom", "predicate": predicate,
                 "subject": [{"digest": {"sha256": digest.removeprefix("sha256:")}}]}
    return json.dumps({"payload": base64.b64encode(json.dumps(statement).encode()).decode()})


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
        hub_patch = patch.object(release, "DockerHubTags")
        self.hub = hub_patch.start().return_value
        self.addCleanup(hub_patch.stop)
        cleanup_patch = patch.object(release, "cleanup_aliases", return_value=[])
        self.cleanup = cleanup_patch.start()
        self.addCleanup(cleanup_patch.stop)

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

    def test_existing_release_rejects_wrong_source_and_validates_legacy_alias(self):
        wrong = state()
        wrong["labels"]["org.opencontainers.image.revision"] = "c" * 40
        with patch.object(release, "inspect", return_value=wrong) as inspect:
            with self.assertRaises(release.ReleaseError):
                release.existing_release(REPOSITORY, "1.0.0", COMMIT, "namesrv")
        self.assertEqual(inspect.call_count, 2)
        inspect.assert_any_call(f"{REPOSITORY}:1.0.0", missing_ok=True)
        with patch.object(release, "inspect", side_effect=[None, state()]):
            self.assertEqual(release.existing_release(REPOSITORY, "1.0.0", COMMIT, "namesrv"), state())
        with patch.object(release, "inspect", side_effect=[state(), state(digest="sha256:" + "c" * 64)]):
            with self.assertRaises(release.ReleaseError):
                release.existing_release(REPOSITORY, "1.0.0", COMMIT, "namesrv")

    def test_promotion_preserves_digest_and_never_overwrites(self):
        with patch.object(release, "inspect", side_effect=[None, state()]), \
             patch.object(release, "run") as command:
            release.promote(REPOSITORY, DIGEST, "1.0.0", COMMIT, "namesrv")
        self.assertEqual(command.call_count, 1)
        for call in command.call_args_list:
            self.assertIn("--prefer-index=false", call.args[0])
            self.assertEqual(call.args[0][-1], f"{REPOSITORY}@{DIGEST}")
            self.assertEqual(call.args[0][call.args[0].index("--tag") + 1], f"{REPOSITORY}:1.0.0")
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
        expected = {"bomFormat": "CycloneDX"}
        for predicate, actual_digest, matches in [(expected, DIGEST, True),
                (expected, "sha256:" + "c" * 64, False), ({"bomFormat": "different"}, DIGEST, False)]:
            payload = attestation(predicate, actual_digest)
            verified = subprocess.CompletedProcess([], 0, payload, "")
            prefix = self.root / "sign"
            with self.subTest(predicate=predicate, digest=actual_digest), \
                 patch.object(release, "upload_cosign_evidence"), \
                 patch.object(release.subprocess, "run", side_effect=[
                     subprocess.CompletedProcess([], 0, "{}", ""), *[verified] * 10,
                 ]) as commands, patch.object(release.time, "sleep") as sleep, \
                 patch("sys.stderr", new_callable=io.StringIO):
                if matches:
                    release.sign(f"{REPOSITORY}@{DIGEST}", sbom, COMMIT, "namesrv", prefix)
                    self.assertEqual(payload, prefix.with_suffix(".attestation.jsonl").read_text())
                    sleep.assert_not_called()
                else:
                    prefix.with_suffix(".attestation.jsonl").unlink(missing_ok=True)
                    with self.assertRaises(release.ReleaseError):
                        release.sign(f"{REPOSITORY}@{DIGEST}", sbom, COMMIT, "namesrv", prefix)
                    self.assertEqual(11, commands.call_count)
                    self.assertEqual(9, sleep.call_count)
                    self.assertFalse(prefix.with_suffix(".attestation.jsonl").exists())

    def test_scanning_uses_registry_endpoint_without_changing_local_or_ghcr_sources(self):
        references = [(f"{REPOSITORY}@{DIGEST}", True,
                       f"registry-1.docker.io/example/rocketmq-rust-namesrv@{DIGEST}"),
                      ("rocketmq-release/namesrv:1.0.0", False, "rocketmq-release/namesrv:1.0.0"),
                      (f"ghcr.io/example/namesrv@{DIGEST}", True, f"ghcr.io/example/namesrv@{DIGEST}")]
        for reference, remote, expected in references:
            with self.subTest(reference=reference), patch.object(release, "run") as run:
                release.qualify(reference, self.root / "scan", remote=remote)
            commands = [call.args[0] for call in run.call_args_list]
            self.assertEqual(f"{'registry' if remote else 'docker'}:{expected}", commands[0][1])
            self.assertEqual(expected, commands[1][-1])

    def test_resigning_waits_for_the_fresh_sbom_after_an_older_valid_attestation(self):
        expected = {"bomFormat": "CycloneDX", "serialNumber": "new-scan"}
        sbom = self.root / "sbom.json"
        sbom.write_text(json.dumps(expected))
        old = attestation({"bomFormat": "CycloneDX", "serialNumber": "old-scan"})
        fresh = attestation(expected)
        with patch.object(release, "upload_cosign_evidence"), patch.object(release.subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], 0, "{}", ""),
                subprocess.CompletedProcess([], 0, old, ""),
                subprocess.CompletedProcess([], 0, old + "\n" + fresh, ""),
             ]) as commands, patch.object(release.time, "sleep") as sleep, \
             patch("sys.stderr", new_callable=io.StringIO):
            release.sign(f"{REPOSITORY}@{DIGEST}", sbom, COMMIT, "namesrv", self.root / "sign")
        self.assertEqual(commands.call_args_list[1], commands.call_args_list[2])
        sleep.assert_called_once_with(5)
        self.assertEqual(old + "\n" + fresh, (self.root / "sign.attestation.jsonl").read_text())

    def test_malformed_verified_attestation_fails_without_retry(self):
        sbom = self.root / "sbom.json"
        sbom.write_text('{"bomFormat":"CycloneDX"}')
        with patch.object(release, "upload_cosign_evidence"), patch.object(release.subprocess, "run", side_effect=[
                subprocess.CompletedProcess([], 0, "{}", ""),
                subprocess.CompletedProcess([], 0, "malformed-json", ""),
             ]), patch.object(release.time, "sleep") as sleep:
            with self.assertRaises(json.JSONDecodeError):
                release.sign(f"{REPOSITORY}@{DIGEST}", sbom, COMMIT, "namesrv", self.root / "sign")
        sleep.assert_not_called()

    def test_evidence_upload_retries_registry_throttling_and_server_errors(self):
        command = ["cosign", "attest", "--type", "cyclonedx", f"{REPOSITORY}@{DIGEST}"]
        with patch.object(release.subprocess, "run", side_effect=[
                subprocess.CompletedProcess(command, 1, "", "unexpected status code 429 Too Many Requests"),
                subprocess.CompletedProcess(command, 1, "", "unexpected status code 503 Service Unavailable"),
                subprocess.CompletedProcess(command, 0, "", ""),
             ]) as run, patch.object(release.time, "sleep") as sleep, \
             patch("sys.stderr", new_callable=io.StringIO):
            release.upload_cosign_evidence(command)
        self.assertEqual([command] * 3, [call.args[0] for call in run.call_args_list])
        self.assertEqual([15, 30], [call.args[0] for call in sleep.call_args_list])

    def test_evidence_upload_permanent_failures_stop_immediately(self):
        command = ["cosign", "sign", f"{REPOSITORY}@{DIGEST}"]
        for error in ("unexpected status code 401 Unauthorized", "unexpected status code 403 Forbidden",
                      "unexpected status code 404 Not Found", "invalid signature", "OIDC token expired"):
            with self.subTest(error=error), patch.object(release.subprocess, "run", return_value=
                    subprocess.CompletedProcess(command, 1, "", error)) as run, \
                 patch.object(release.time, "sleep") as sleep, patch("sys.stderr", new_callable=io.StringIO):
                with self.assertRaises(subprocess.CalledProcessError):
                    release.upload_cosign_evidence(command)
                run.assert_called_once()
                sleep.assert_not_called()

    def test_evidence_upload_retry_exhaustion_remains_a_failure(self):
        command = ["cosign", "attest", "--type", "cyclonedx", f"{REPOSITORY}@{DIGEST}"]
        with patch.object(release.subprocess, "run", return_value=
                subprocess.CompletedProcess(command, 1, "", "unexpected status code 429 Too Many Requests")) as run, \
             patch.object(release.time, "sleep") as sleep, patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(subprocess.CalledProcessError):
                release.upload_cosign_evidence(command)
        self.assertEqual(5, run.call_count)
        self.assertEqual(165, sum(call.args[0] for call in sleep.call_args_list))

    def test_registry_signature_discovery_retries_without_changing_verification(self):
        command = ["cosign", "verify", "--certificate-identity-regexp", release.IDENTITY,
                   "--certificate-oidc-issuer", release.ISSUER,
                   "--annotations", f"source_commit={COMMIT}", f"{REPOSITORY}@{DIGEST}"]
        pending = subprocess.CompletedProcess(command, 10, "", "Error: no signatures found\n")
        available = subprocess.CompletedProcess(command, 0, '{"verified":true}', "")
        with patch.object(release.subprocess, "run", side_effect=[pending, available]) as run, \
             patch.object(release.time, "sleep") as sleep, patch("sys.stderr", new_callable=io.StringIO):
            self.assertEqual('{"verified":true}', release.verify_registry_evidence(command))
        self.assertEqual([command, command], [call.args[0] for call in run.call_args_list])
        sleep.assert_called_once_with(5)

    def test_registry_attestation_predicate_discovery_can_lag_behind_signature(self):
        command = ["cosign", "verify-attestation", "--type", "cyclonedx", f"{REPOSITORY}@{DIGEST}"]
        error = ("error during command execution: none of the attestations matched the predicate type: "
                 "cyclonedx, found: https://sigstore.dev/cosign/sign/v1\n")
        with patch.object(release.subprocess, "run", side_effect=[
                subprocess.CompletedProcess(command, 1, "", error),
                subprocess.CompletedProcess(command, 0, "verified attestation", ""),
             ]), patch.object(release.time, "sleep") as sleep, patch("sys.stderr", new_callable=io.StringIO):
            self.assertEqual("verified attestation", release.verify_registry_evidence(command))
        sleep.assert_called_once_with(5)

    def test_registry_evidence_permanent_failures_stop_immediately(self):
        command = ["cosign", "verify", f"{REPOSITORY}@{DIGEST}"]
        def reject_payload(_):
            raise AssertionError("unverified evidence must not reach payload validation")
        for error in ("unauthorized: authentication required", "certificate identity mismatch",
                      "no matching signatures: invalid signature", "certificate issuer mismatch"):
            with self.subTest(error=error), patch.object(release.subprocess, "run", return_value=
                    subprocess.CompletedProcess(command, 1, "", f"error during command execution: {error}\n")) as run, \
                 patch.object(release.time, "sleep") as sleep, patch("sys.stderr", new_callable=io.StringIO):
                with self.assertRaises(subprocess.CalledProcessError):
                    release.verify_registry_evidence(command, validate=reject_payload)
                run.assert_called_once()
                sleep.assert_not_called()

    def test_registry_evidence_reads_retry_throttling_without_weakening_verification(self):
        command = ["cosign", "verify-attestation", "--certificate-identity-regexp", release.IDENTITY,
                   "--certificate-oidc-issuer", release.ISSUER, "--type", "cyclonedx",
                   f"{REPOSITORY}@{DIGEST}"]
        for error in ("unexpected status code 429 Too Many Requests", "unexpected status code 503",
                      "GET registry/manifests/sha256-example.att: TOOMANYREQUESTS: pull rate limit"):
            with self.subTest(error=error), patch.object(release.subprocess, "run", side_effect=[
                    subprocess.CompletedProcess(command, 1, "", f"Error: {error}\n"),
                    subprocess.CompletedProcess(command, 0, "verified", ""),
                 ]) as run, patch.object(release.time, "sleep") as sleep, \
                 patch("sys.stderr", new_callable=io.StringIO):
                self.assertEqual("verified", release.verify_registry_evidence(command))
            self.assertEqual([command, command], [call.args[0] for call in run.call_args_list])
            sleep.assert_called_once_with(5)

    def test_registry_evidence_retry_exhaustion_remains_a_failure(self):
        command = ["cosign", "verify", f"{REPOSITORY}@{DIGEST}"]
        with patch.object(release.subprocess, "run", return_value=
                subprocess.CompletedProcess(command, 10, "", "Error: no signatures found\n")) as run, \
             patch.object(release.time, "sleep") as sleep, patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(subprocess.CalledProcessError):
                release.verify_registry_evidence(command)
        self.assertEqual(10, run.call_count)
        self.assertEqual(375, sum(call.args[0] for call in sleep.call_args_list))

    def test_failed_run_staging_is_rescanned_signed_and_promoted_without_rebuilding(self):
        self.policy["groups"]["core"] = self.policy["groups"]["core"][:1]
        self.write_policy()
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT) as commands, \
             patch.object(release, "existing_release", return_value=None), \
             patch.object(release, "inspect", return_value=state()) as inspect, \
             patch.object(release, "build") as build, \
             patch.object(release, "qualify", return_value=self.root / "sbom.json") as qualify, \
             patch.object(release, "sign") as sign, patch.object(release, "promote") as promote:
            result = release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out",
                                     staging_run="12345-1")
        inspect.assert_called_once_with(f"{REPOSITORY}:staging-{COMMIT[:12]}-12345-1", missing_ok=True)
        build.assert_not_called()
        self.assertEqual(1, commands.call_count)
        qualify.assert_called_once_with(f"{REPOSITORY}@{DIGEST}", self.root / "out/namesrv", remote=True)
        sign.assert_called_once()
        promote.assert_called_once_with(REPOSITORY, DIGEST, "1.0.0", COMMIT, "namesrv")
        self.assertTrue(result["complete"])

    def test_staging_from_a_different_source_prevents_all_builds_and_writes(self):
        wrong_source = state()
        wrong_source["labels"]["org.opencontainers.image.revision"] = "c" * 40
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT), \
             patch.object(release, "existing_release", return_value=None), \
             patch.object(release, "inspect", return_value=wrong_source), \
             patch.object(release, "build") as build, patch.object(release, "sign") as sign, \
             patch.object(release, "promote") as promote:
            with self.assertRaises(release.ReleaseError):
                release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out",
                                staging_run="12345-1")
        build.assert_not_called()
        sign.assert_not_called()
        promote.assert_not_called()

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

    def test_cleanup_deletes_only_matching_release_aliases_and_preserves_stable_digest(self):
        aliases = [f"1.0.0-{COMMIT[:12]}", f"staging-{COMMIT[:12]}-12345-1"]
        self.hub.tags.return_value = [{"name": tag, "digest": DIGEST} for tag in aliases + [
            "1.0.0", "0.9.0", "sha256-" + "b" * 64, "staging-cccccccccccc-12345-1"]]
        with patch.object(release, "inspect", return_value=state()):
            removed = REAL_CLEANUP(self.hub, "example", self.policy["groups"]["core"][0], "1.0.0", COMMIT, DIGEST)
        self.assertEqual(removed, aliases)
        self.assertEqual([call.args for call in self.hub.delete.call_args_list],
                         [("example", "rocketmq-rust-namesrv", tag) for tag in aliases])

    def test_late_conflicting_cleanup_alias_prevents_every_delete(self):
        self.hub.tags.return_value = [{"name": f"1.0.0-{COMMIT[:12]}", "digest": DIGEST},
            {"name": f"staging-{COMMIT[:12]}-12345-1", "digest": "sha256:" + "c" * 64}]
        with patch.object(release, "inspect", return_value=state()), self.assertRaises(release.ReleaseError):
            REAL_CLEANUP(self.hub, "example", self.policy["groups"]["core"][0], "1.0.0", COMMIT, DIGEST)
        self.hub.delete.assert_not_called()

    def test_failed_new_image_signature_cleans_its_temporary_tag_without_promotion(self):
        self.policy["groups"]["core"] = self.policy["groups"]["core"][:1]
        self.write_policy()
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT), \
             patch.dict(release.os.environ, {"GITHUB_RUN_ID": "12345", "GITHUB_RUN_ATTEMPT": "1"}), \
             patch.object(release, "existing_release", return_value=None), patch.object(release, "build"), \
             patch.object(release, "inspect", return_value=state()), \
             patch.object(release, "qualify", return_value=self.root / "sbom.json"), \
             patch.object(release, "sign", side_effect=release.ReleaseError("signature failure")), \
             patch.object(release, "promote") as promote:
            with self.assertRaisesRegex(release.ReleaseError, "signature failure"):
                release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out")
        promote.assert_not_called()
        self.assertEqual(self.hub.delete.call_count, 2)
        self.hub.delete.assert_called_with("example", "rocketmq-rust-namesrv", f"staging-{COMMIT[:12]}-12345-1")
        self.assertFalse(json.loads((self.root / "out/publication.json").read_text())["complete"])

    def test_successful_new_image_removes_staging_and_records_only_version_tag(self):
        self.policy["groups"]["core"] = self.policy["groups"]["core"][:1]
        self.write_policy()
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT), \
             patch.dict(release.os.environ, {"GITHUB_RUN_ID": "12345", "GITHUB_RUN_ATTEMPT": "1"}), \
             patch.object(release, "existing_release", return_value=None), patch.object(release, "build"), \
             patch.object(release, "inspect", return_value=state()), \
             patch.object(release, "qualify", return_value=self.root / "sbom.json"), \
             patch.object(release, "sign"), patch.object(release, "promote"):
            result = release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out")
        self.assertEqual(self.hub.delete.call_count, 2)
        self.hub.delete.assert_called_with("example", "rocketmq-rust-namesrv", f"staging-{COMMIT[:12]}-12345-1")
        self.assertEqual(result["images"][0]["tags"], ["1.0.0"])
        self.assertEqual(result["images"][0]["removed_tags"], [f"staging-{COMMIT[:12]}-12345-1"])

    def test_cleanup_failure_does_not_report_a_complete_publication(self):
        self.cleanup.side_effect = release.HubError("HTTP 403")
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT), \
             patch.object(release, "existing_release", side_effect=[state(), state("broker")]), \
             patch.object(release, "qualify", return_value=self.root / "sbom.json"), \
             patch.object(release, "sign"), patch.object(release, "promote"):
            with self.assertRaises(release.HubError):
                release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out")
        self.assertFalse(json.loads((self.root / "out/publication.json").read_text())["complete"])

    def test_always_cleanup_deletes_only_the_exact_run_tags_in_selected_group(self):
        with patch.object(release, "ROOT", self.root):
            release.cleanup_run_tags("core", COMMIT, "example", "12345-2")
        self.assertEqual([call.args for call in self.hub.delete.call_args_list], [
            ("example", "rocketmq-rust-namesrv", f"staging-{COMMIT[:12]}-12345-2"),
            ("example", "rocketmq-rust-broker", f"staging-{COMMIT[:12]}-12345-2")])
        self.hub.delete.reset_mock()
        with self.assertRaises(release.ReleaseError):
            release.cleanup_run_tags("core", COMMIT, "example", "../other")
        self.hub.delete.assert_not_called()

    def test_missing_delete_permission_fails_before_building_or_pushing(self):
        self.hub.delete.side_effect = release.HubError("HTTP 403")
        with patch.object(release, "ROOT", self.root), patch.object(release, "run", return_value=COMMIT), \
             patch.object(release, "build") as build, patch.object(release, "existing_release") as existing:
            with self.assertRaises(release.HubError):
                release.execute("core", "1.0.0", COMMIT, "example", True, self.root / "out")
        build.assert_not_called()
        existing.assert_not_called()


if __name__ == "__main__":
    unittest.main()
