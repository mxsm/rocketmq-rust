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
import json
import unittest

from scripts import public_api_snapshot as snapshot


def rustdoc_fixture(item_id: str = "17") -> dict[str, object]:
    return {
        "crate_version": "1.0.0",
        "paths": {
            item_id: {
                "crate_id": 0,
                "path": ["rocketmq_model", "message", "MessageView"],
                "kind": "struct",
            }
        },
        "index": {
            item_id: {
                "id": item_id,
                "crate_id": 0,
                "name": "MessageView",
                "visibility": "public",
                "attrs": ["#[cfg(feature = \"message-view\")]"],
                "inner": {
                    "struct": {
                        "kind": "plain",
                        "generics": {"params": [], "where_predicates": []},
                        "fields": ["41", "42"],
                        "impls": ["99"],
                    }
                },
            }
        },
    }


class PublicApiSnapshotTests(unittest.TestCase):
    def test_semantic_record_has_complete_readable_identity_without_digest(self) -> None:
        records = snapshot.semantic_public_items("rocketmq-model", rustdoc_fixture())

        self.assertEqual(1, len(records))
        record = records[0]
        self.assertEqual(
            {
                "package",
                "module",
                "item_path",
                "kind",
                "visibility",
                "signature",
                "feature",
            },
            set(record),
        )
        self.assertEqual("rocketmq-model", record["package"])
        self.assertEqual("rocketmq_model::message", record["module"])
        self.assertEqual("rocketmq_model::message::MessageView", record["item_path"])
        self.assertEqual("message-view", record["feature"])
        self.assertNotIn("sha", json.dumps(record).lower())
        self.assertNotIn("fingerprint", json.dumps(record).lower())

    def test_rustdoc_internal_ids_do_not_change_semantic_snapshot(self) -> None:
        first = snapshot.semantic_public_items("rocketmq-model", rustdoc_fixture("17"))
        second_document = rustdoc_fixture("200")
        second_document["index"]["200"]["inner"]["struct"]["fields"] = ["501", "502"]
        second_document["index"]["200"]["inner"]["struct"]["impls"] = ["777"]
        second = snapshot.semantic_public_items("rocketmq-model", second_document)

        self.assertEqual(first, second)

    def test_removed_semantic_item_is_reported_as_breaking(self) -> None:
        baseline = {
            "schema_version": 2,
            "feature_profile": "default",
            "toolchain": {"rustc": "x", "rustdoc": "x", "cargo": "x"},
            "packages": {
                "rocketmq-model": {
                    "target": "rocketmq_model",
                    "crate_version": "1.0.0",
                    "public_api": snapshot.semantic_public_items("rocketmq-model", rustdoc_fixture()),
                }
            },
            "public_api_intent": {},
        }
        candidate = copy.deepcopy(baseline)
        candidate["packages"]["rocketmq-model"]["public_api"] = []

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual("item-removed", differences[0]["kind"])
        self.assertEqual("breaking", differences[0]["classification"])


if __name__ == "__main__":
    unittest.main()
