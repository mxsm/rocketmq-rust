#!/usr/bin/env python3
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

"""Validate the machine-readable RocketMQ core release package scope."""

from __future__ import annotations

import argparse
import sys

import core_release_scope


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", choices=("core-release", "repo-global", "all"), default="core-release")
    args = parser.parse_args()
    try:
        scope, findings = core_release_scope.validate_repository()
    except core_release_scope.ScopeInputError as error:
        print(f"CORE_RELEASE_SCOPE_INPUT_FAILED detail={error}")
        return 1

    core_findings = [item for item in findings if item.scope == "core"]
    repo_findings = [item for item in findings if item.scope == "repo-global"]
    if core_findings:
        print(f"CORE_RELEASE_SCOPE_FAILED findings={len(core_findings)}")
        for finding in core_findings:
            print(finding.render())
    else:
        print(
            "CORE_RELEASE_SCOPE_OK "
            f"packages={len(core_release_scope.core_packages(scope))} "
            f"workspace_exclusions={len(scope['workspace_exclusions'])}"
        )
    if repo_findings:
        print(f"CORE_RELEASE_REPO_GLOBAL_FAILED findings={len(repo_findings)}")
        for finding in repo_findings:
            print(finding.render())
    else:
        print(f"CORE_RELEASE_REPO_GLOBAL_OK exclusions={len(scope['repository_exclusions'])}")

    if args.scope == "core-release":
        return int(bool(core_findings))
    if args.scope == "repo-global":
        return int(bool(repo_findings))
    return int(bool(findings))


if __name__ == "__main__":
    sys.exit(main())
