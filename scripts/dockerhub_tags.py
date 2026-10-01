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

"""Docker Hub tag operations; deleting a tag must not delete its shared manifest."""

import json
import os
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


class HubError(ValueError):
    """Docker Hub tag maintenance failed."""


class DockerHubTags:
    def __init__(self, *, bearer: str | None = None):
        self.token = bearer
        if bearer is None:
            username = os.environ.get("DOCKERHUB_USERNAME")
            secret = os.environ.get("DOCKERHUB_TOKEN")
            if not username or not secret:
                raise HubError("Set DOCKERHUB_USERNAME and DOCKERHUB_TOKEN for tag cleanup")
            result = self.request("/v2/auth/token", method="POST", payload={"identifier": username, "secret": secret})
            self.token = result.get("access_token")
            if not isinstance(self.token, str) or not self.token:
                raise HubError("Docker Hub did not return an access token")

    def request(self, path: str, *, method: str = "GET", payload: dict | None = None,
                missing_ok: bool = False) -> dict | None:
        headers = {"Accept": "application/json", "User-Agent": "rocketmq-rust-release"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        data = json.dumps(payload).encode() if payload is not None else None
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = Request("https://hub.docker.com" + path, headers=headers, method=method, data=data)
        for attempt in range(4):
            try:
                with urlopen(request, timeout=30) as response:
                    body = response.read()
                    return json.loads(body) if body else None
            except HTTPError as error:
                status = error.code
                error.close()
                if missing_ok and status == 404:
                    return None
                if status in {429, 500, 502, 503, 504} and attempt < 3 and method != "POST":
                    time.sleep(2 ** attempt)
                    continue
                permission = " (DOCKERHUB_TOKEN needs Read, Write, Delete permission)" if status == 403 and method == "DELETE" else ""
                # Response bodies and authentication payloads may contain credentials; never report them.
                raise HubError(f"Docker Hub {method} {path} failed: HTTP {status}{permission}") from None
            except (URLError, TimeoutError):
                if attempt < 3 and method != "POST":
                    time.sleep(2 ** attempt)
                    continue
                raise HubError(f"Docker Hub {method} {path} failed: transport error") from None
        raise HubError("Docker Hub request did not complete")

    @staticmethod
    def repository_path(namespace: str, repository: str) -> str:
        if any(not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", value) for value in (namespace, repository)):
            raise HubError("Invalid Docker Hub namespace/repository")
        return f"/v2/namespaces/{namespace}/repositories/{repository}/tags"

    def tags(self, namespace: str, repository: str) -> list[dict]:
        root = self.repository_path(namespace, repository)
        path = root + "?page_size=100"
        tags = []
        seen = set()
        while path:
            if path in seen:
                raise HubError("Repeated Docker Hub tag pagination link")
            seen.add(path)
            page = self.request(path)
            tags.extend(page["results"])
            next_page = page.get("next")
            if not next_page:
                break
            url = urlparse(next_page)
            if url.scheme != "https" or url.netloc != "hub.docker.com" or url.path.rstrip("/") != root:
                raise HubError("Untrusted Docker Hub pagination link")
            path = url.path + ("?" + url.query if url.query else "")
        return tags

    def delete(self, namespace: str, repository: str, tag: str) -> None:
        self.repository_path(namespace, repository)
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
            raise HubError("Invalid Docker Hub tag")
        # This tag-only API is also used by docker/hub-tool. Registry manifest DELETE would
        # remove the digest shared by the stable tag, temporary aliases, and signature subjects.
        self.request(f"/v2/repositories/{namespace}/{repository}/tags/{quote(tag, safe='')}/",
                     method="DELETE", missing_ok=True)
