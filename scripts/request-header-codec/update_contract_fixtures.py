#!/usr/bin/env python3
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

"""Promote deterministic Java contract output into checked-in Rust fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path


JAVA_COMMIT = "2daf0e2ca91a1592d18235d43e5d709d1c35d15f"
RUST_HISTORICAL_COMMIT = "0c4722568a74987f7be51df12ec87dbfdc05fbba"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_json(document: object) -> bytes:
    return (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def normalized_newlines(payload: bytes) -> bytes:
    return payload.replace(b"\r\n", b"\n")


def build_fixture_tree(source: Path, destination: Path) -> dict[Path, bytes]:
    schema_path = source / "java-schema.json"
    index_path = source / "golden" / "index.json"
    evidence_path = source / "extractor-evidence.json"
    for required in (schema_path, index_path, evidence_path):
        if not required.is_file():
            raise ValueError(f"missing extractor output: {required}")

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    index = json.loads(index_path.read_text(encoding="utf-8"))
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    if schema.get("javaCommit") != JAVA_COMMIT or index.get("javaCommit") != JAVA_COMMIT:
        raise ValueError("extractor output does not match the pinned Java commit")
    if evidence.get("releasable") is not True or evidence.get("dirty") is not False:
        raise ValueError("only clean, releasable extractor output may be promoted")

    files: dict[Path, bytes] = {
        destination / "java-schema.json": schema_path.read_bytes(),
        destination / "golden" / "index.json": index_path.read_bytes(),
    }
    golden_files: list[dict[str, object]] = []
    for entry in index["fixtures"]:
        fixture_path = source / "golden" / entry["file"]
        payload = fixture_path.read_bytes()
        files[destination / "golden" / entry["file"]] = payload
        golden_files.append(
            {
                "id": entry["id"],
                "file": f"golden/{entry['file']}",
                "sha256": hashlib.sha256(payload).hexdigest(),
                "frameLength": entry["frameLength"],
                "fnv1a64": entry["fnv1a64"],
            }
        )

    empty_allowlist_path = Path(__file__).resolve().parent / "legacy-empty-header-allowlist.json"
    empty_allowlist = json.loads(empty_allowlist_path.read_text(encoding="utf-8"))
    empty_headers: list[dict[str, object]] = []
    for header in empty_allowlist["headers"]:
        fixture = {
            "schemaVersion": 1,
            "id": header["golden"],
            "rustTypeId": header["rustTypeId"],
            "classification": "intentional-empty-header",
            "logicalMap": {},
            "jsonObject": {},
            "owner": header["owner"],
            "reason": header["reason"],
        }
        payload = canonical_json(fixture)
        relative = Path("rust-only") / f"{header['golden']}.json"
        files[destination / relative] = payload
        empty_headers.append(
            {
                "id": header["golden"],
                "rustTypeId": header["rustTypeId"],
                "file": relative.as_posix(),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )

    manifest = {
        "schemaVersion": 1,
        "contractVersion": "request-header-codec-v1",
        "javaCommit": JAVA_COMMIT,
        "rustHistoricalCommit": RUST_HISTORICAL_COMMIT,
        "schema": {
            "file": "java-schema.json",
            "sha256": digest(schema_path),
            "mappedHeaderCount": schema["mappedHeaderCount"],
        },
        "goldenIndex": {
            "file": "golden/index.json",
            "sha256": digest(index_path),
            "fixtureCount": index["fixtureCount"],
        },
        "goldenFiles": golden_files,
        "legacyEmptyHeaders": empty_headers,
        "wirePolicies": {
            "canonicalRpcKeys": ["ns", "nsd", "bname", "oway"],
            "legacyRpcDecodeAliases": ["namespace", "namespaced", "brokerName", "oneway"],
            "logicalMapEmpty": "preserve",
            "jsonEmpty": "preserve",
            "rocketmqBinaryEmpty": "normalize-to-absent",
            "unknownFields": "ignore-after-envelope-limits",
            "malformedInteger": "reject",
            "malformedBoolean": "reject",
            "requiredStringEmpty": "reject-before-encode",
            "aliasConflict": "reviewed-prefer-canonical-only",
        },
    }
    files[destination / "manifest.json"] = canonical_json(manifest)
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    script = Path(__file__).resolve()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=script.parents[2] / "rocketmq-protocol" / "tests" / "fixtures" / "request_header_codec",
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    try:
        source = args.input.resolve()
        destination = args.output.resolve()
        expected = build_fixture_tree(source, destination)
        if args.check:
            stale = [
                path
                for path, payload in expected.items()
                if not path.is_file() or normalized_newlines(path.read_bytes()) != normalized_newlines(payload)
            ]
            extra = []
            if destination.is_dir():
                expected_paths = set(expected)
                extra = [path for path in destination.rglob("*.json") if path not in expected_paths]
            if stale or extra:
                for path in stale:
                    print(f"stale fixture: {path}", file=sys.stderr)
                for path in extra:
                    print(f"unexpected fixture: {path}", file=sys.stderr)
                return 1
            print(f"contract fixtures are current: {len(expected) - 3} generated contract files")
            return 0

        destination.mkdir(parents=True, exist_ok=True)
        golden_directory = destination / "golden"
        if golden_directory.exists():
            shutil.rmtree(golden_directory)
        for path, payload in expected.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        print(f"promoted {len(expected) - 3} generated contract files to {destination}")
        return 0
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
