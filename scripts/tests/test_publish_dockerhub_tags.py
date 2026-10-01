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
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dockerhub_tags as hub


class DockerHubTagApiTests(unittest.TestCase):
    def test_delete_uses_tag_endpoint_and_accepts_empty_success_response(self):
        client = hub.DockerHubTags(bearer="fixture")
        with patch.object(hub, "urlopen", return_value=io.BytesIO(b"")) as send:
            client.delete("example", "rocketmq-rust-broker", "staging-aaaaaaaaaaaa-12345-1")
        request = send.call_args.args[0]
        self.assertEqual(request.get_method(), "DELETE")
        self.assertEqual(request.full_url,
            "https://hub.docker.com/v2/repositories/example/rocketmq-rust-broker/tags/staging-aaaaaaaaaaaa-12345-1/")
        self.assertNotIn("/manifests/", request.full_url)

    def test_only_explicit_404_is_an_already_deleted_tag(self):
        for status in (404, 401, 403):
            with self.subTest(status=status), patch.object(hub, "urlopen", side_effect=
                    HTTPError("https://hub.docker.com", status, "test", {}, None)):
                if status == 404:
                    hub.DockerHubTags(bearer="fixture").delete("example", "repo", "temporary")
                else:
                    with self.assertRaises(hub.HubError):
                        hub.DockerHubTags(bearer="fixture").delete("example", "repo", "temporary")

    def test_tag_listing_consumes_all_pages_and_rejects_foreign_pagination(self):
        root = "/v2/namespaces/example/repositories/repo/tags"
        for target, valid in [(f"https://hub.docker.com{root}?page=2", True),
                              ("https://attacker.example/tags", False),
                              ("https://hub.docker.com/v2/namespaces/other/repositories/repo/tags?page=2", False)]:
            first = io.BytesIO(json.dumps({"results": [{"name": "1.0.0"}], "next": target}).encode())
            second = io.BytesIO(b'{"results":[{"name":"temporary"}],"next":null}')
            with self.subTest(target=target), patch.object(hub, "urlopen", side_effect=[first, second]) as send:
                client = hub.DockerHubTags(bearer="fixture")
                if valid:
                    self.assertEqual([t["name"] for t in client.tags("example", "repo")], ["1.0.0", "temporary"])
                else:
                    with self.assertRaises(hub.HubError):
                        client.tags("example", "repo")
                    self.assertEqual(send.call_count, 1)

    def test_authentication_failure_never_reports_credentials_or_response_body(self):
        with patch.dict(hub.os.environ, {"DOCKERHUB_USERNAME": "example", "DOCKERHUB_TOKEN": "fixture-secret"}), \
             patch.object(hub, "urlopen", side_effect=HTTPError(
                 "https://hub.docker.com", 401, "fixture-secret", {}, io.BytesIO(b"fixture-secret"))):
            with self.assertRaises(hub.HubError) as caught:
                hub.DockerHubTags()
        self.assertNotIn("fixture-secret", str(caught.exception))

    def test_expired_bearer_is_refreshed_once_after_long_build(self):
        client = hub.DockerHubTags(bearer="old-fixture")
        client.can_refresh = True
        with patch.dict(hub.os.environ, {"DOCKERHUB_USERNAME": "example", "DOCKERHUB_TOKEN": "fixture-secret"}), \
             patch.object(hub, "urlopen", side_effect=[
                 HTTPError("https://hub.docker.com", 401, "expired", {}, None),
                 io.BytesIO(b'{"access_token":"new-fixture"}'), io.BytesIO(b'')]) as send:
            client.delete("example", "repo", "temporary")
        self.assertEqual(send.call_count, 3)
        auth = send.call_args_list[1].args[0]
        self.assertEqual(auth.full_url, "https://hub.docker.com/v2/auth/token")
        self.assertNotIn("Authorization", auth.headers)
        self.assertEqual(send.call_args_list[2].args[0].headers["Authorization"], "Bearer new-fixture")

    def test_refresh_does_not_loop_or_retry_permission_denial(self):
        for status in (401, 403):
            client = hub.DockerHubTags(bearer="old-fixture")
            client.can_refresh = True
            failures = ([HTTPError("https://hub.docker.com", 401, "expired", {}, None),
                         io.BytesIO(b'{"access_token":"new-fixture"}')]
                        if status == 401 else [])
            failures.append(HTTPError("https://hub.docker.com", status, "denied", {}, None))
            with self.subTest(status=status), patch.dict(hub.os.environ,
                    {"DOCKERHUB_USERNAME": "example", "DOCKERHUB_TOKEN": "fixture-secret"}), \
                 patch.object(hub, "urlopen", side_effect=failures) as send, \
                 self.assertRaises(hub.HubError):
                client.delete("example", "repo", "temporary")
            self.assertEqual(send.call_count, 3 if status == 401 else 1)


if __name__ == "__main__":
    unittest.main()
