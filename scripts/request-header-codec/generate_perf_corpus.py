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

"""Generate the frozen, stratified request-header performance corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path


FAST_HEADERS = {
    "pull-fast-inherited",
    "send-v1-fast",
    "send-v2-fast",
    "send-response-fast",
}


def digest_map(fields: dict[str, str]) -> str:
    payload = json.dumps(fields, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    fixture_root = root.parents[1] / "rocketmq-protocol" / "tests" / "fixtures" / "request_header_codec"
    inputs = json.loads((root / "golden-inputs-v1.json").read_text(encoding="utf-8"))
    by_id = {case["id"]: case for case in inputs["cases"]}

    production: list[dict[str, object]] = []
    for base_id, source in sorted(by_id.items()):
        tier = 1 if base_id in FAST_HEADERS else 2
        for serialize_type in ("ROCKETMQ", "JSON"):
            fixture = json.loads(
                (fixture_root / "golden" / f"{base_id}-{serialize_type.lower()}.json").read_text(encoding="utf-8")
            )
            canonical = fixture["canonicalExtFields"]
            for operation in ("encode", "decode"):
                fast_subset = base_id in FAST_HEADERS and (
                    operation == "decode" or serialize_type == "ROCKETMQ"
                )
                production.append(
                    {
                        "id": f"{base_id}-{serialize_type.lower()}-{operation}",
                        "tier": tier,
                        "gateWeight": 0.0,
                        "fastGateWeight": 1.0 / 12.0 if fast_subset else 0.0,
                        "operation": operation,
                        "header": source["rustTypeId"],
                        "fixtureId": fixture["id"],
                        "requestCode": fixture["requestCodeValue"],
                        "fields": canonical,
                        "unknownCount": 0,
                        "flattenDepth": 1 if any(key in canonical for key in ("lo", "ns", "bname")) else 0,
                        "optionDensity": round(
                            sum(value in ("", None) for value in source["fields"].values())
                            / max(1, len(source["fields"])),
                            6,
                        ),
                        "serializeType": serialize_type,
                        "expectedSemanticSha256": digest_map(canonical),
                        "expectedFrameFnv1a64": fixture["fnv1a64"],
                        "weightSource": "reviewed-equal-stratified-no-production-telemetry",
                    }
                )

    equal_weight = 1.0 / len(production)
    for case in production:
        case["gateWeight"] = equal_weight

    document = {
        "schemaVersion": 1,
        "corpusVersion": "request-header-codec-perf-v1",
        "javaCommit": inputs["javaCommit"],
        "fixtureManifestSha256": hashlib.sha256((fixture_root / "manifest.json").read_bytes()).hexdigest(),
        "weightProfile": {
            "kind": "reviewed-equal-stratified",
            "productionTelemetry": False,
            "operationCount": len(production),
            "weightSum": sum(case["gateWeight"] for case in production),
            "fastSubsetWeightSum": sum(case["fastGateWeight"] for case in production),
        },
        "cases": production,
        "diagnosticCases": [
            {
                "id": "query-consume-queue-rocketmq-decode-unknown-heavy",
                "baseCase": "query-consume-queue-sparse-rocketmq-decode",
                "gateWeight": 0.0,
                "unknownCount": 32,
                "purpose": "single-scan unknown-field diagnostic",
            },
            {
                "id": "pull-map-only-decode",
                "baseCase": "pull-fast-inherited-rocketmq-decode",
                "gateWeight": 0.0,
                "purpose": "component diagnostic; excluded from production-entrypoint gates",
            },
            {
                "id": "send-v2-preallocated-encode",
                "baseCase": "send-v2-fast-rocketmq-encode",
                "gateWeight": 0.0,
                "purpose": "buffer reuse diagnostic; excluded from production-entrypoint gates",
            },
        ],
    }
    output = root / "perf-corpus-v1.json"
    rendered = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
    if args.check:
        if not output.is_file() or output.read_text(encoding="utf-8") != rendered:
            print(f"stale performance corpus: {output}", file=sys.stderr)
            raise SystemExit(1)
        print(f"performance corpus is current: {len(production)} production operations")
        return
    output.write_text(rendered, encoding="utf-8", newline="\n")
    print(f"wrote {len(production)} production performance operations to {output}")


if __name__ == "__main__":
    main()
