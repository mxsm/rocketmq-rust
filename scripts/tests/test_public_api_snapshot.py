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
from types import SimpleNamespace
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
                        "kind": {
                            "plain": {
                                "fields": ["41", "42"],
                                "has_stripped_fields": False,
                            }
                        },
                        "generics": {"params": [], "where_predicates": []},
                        "impls": ["99"],
                    }
                },
            }
        },
    }


def rustdoc_associated_fixture(
    *,
    root_id: str = "17",
    field_id: str = "41",
    impl_id: str = "99",
    method_id: str = "77",
    field_type: str = "u64",
) -> dict[str, object]:
    document = rustdoc_fixture(root_id)
    root = document["index"][root_id]
    root["inner"]["struct"]["kind"]["plain"]["fields"] = [field_id]
    root["inner"]["struct"]["impls"] = [impl_id]
    document["index"].update(
        {
            field_id: {
                "id": field_id,
                "crate_id": 0,
                "name": "value",
                "visibility": "public",
                "attrs": [],
                "inner": {"struct_field": {"primitive": field_type}},
            },
            impl_id: {
                "id": impl_id,
                "crate_id": 0,
                "name": None,
                "visibility": "default",
                "attrs": [],
                "inner": {
                    "impl": {
                        "is_unsafe": False,
                        "generics": {"params": [], "where_predicates": []},
                        "provided_trait_methods": [],
                        "trait": None,
                        "for": {"resolved_path": {"path": "MessageView", "id": root_id}},
                        "items": [method_id],
                        "is_negative": False,
                        "is_synthetic": False,
                        "blanket_impl": None,
                    }
                },
            },
            method_id: {
                "id": method_id,
                "crate_id": 0,
                "name": "build",
                "visibility": "public",
                "attrs": [],
                "inner": {
                    "function": {
                        "sig": {
                            "inputs": [],
                            "output": {"resolved_path": {"path": "MessageView", "id": root_id}},
                            "is_c_variadic": False,
                        },
                        "generics": {"params": [], "where_predicates": []},
                        "header": {
                            "is_const": False,
                            "is_unsafe": False,
                            "is_async": False,
                            "abi": "Rust",
                        },
                        "has_body": True,
                    }
                },
            },
        }
    )
    return document


def rustdoc_proc_macro_fixture(*, include_v3_path: bool = False) -> dict[str, object]:
    root_id = "root"
    macro_helpers = {
        "RequestHeaderCodec": ["required"],
        "RequestHeaderCodecV2": ["required", "request_header", "request_header_codec_v2"],
        "RequestHeaderCodecV3": ["header", "required"],
        "RemotingSerializable": [],
    }
    macro_ids = {name: f"macro-{index}" for index, name in enumerate(macro_helpers, start=1)}
    index: dict[str, object] = {
        root_id: {
            "id": root_id,
            "crate_id": 0,
            "name": "rocketmq_macros",
            "visibility": "public",
            "attrs": [],
            "inner": {
                "module": {
                    "is_crate": True,
                    "is_stripped": False,
                    "items": [
                        *macro_ids.values(),
                        "private-macro",
                        "ordinary-function",
                    ],
                }
            },
        }
    }
    for name, helpers in macro_helpers.items():
        index[macro_ids[name]] = {
            "id": macro_ids[name],
            "crate_id": 0,
            "name": name,
            "visibility": "public",
            "attrs": ["#[doc = \"source attributes are intentionally irrelevant here\"]"],
            "inner": {"proc_macro": {"kind": "derive", "helpers": helpers}},
        }
    index["private-macro"] = {
        "id": "private-macro",
        "crate_id": 0,
        "name": "PrivateDerive",
        "visibility": "default",
        "attrs": [],
        "inner": {"proc_macro": {"kind": "derive", "helpers": ["private"]}},
    }
    index["ordinary-function"] = {
        "id": "ordinary-function",
        "crate_id": 0,
        "name": "ordinary_function",
        "visibility": "public",
        "attrs": [],
        "inner": {"function": {"sig": {"inputs": [], "output": None}}},
    }
    paths: dict[str, object] = {
        root_id: {
            "crate_id": 0,
            "path": ["rocketmq_macros"],
            "kind": "module",
        }
    }
    if include_v3_path:
        paths[macro_ids["RequestHeaderCodecV3"]] = {
            "crate_id": 0,
            "path": ["rocketmq_macros", "RequestHeaderCodecV3"],
            "kind": "proc_macro",
        }
    return {"crate_version": "1.0.0", "root": root_id, "paths": paths, "index": index}


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
        first = snapshot.semantic_public_items("rocketmq-model", rustdoc_associated_fixture())
        second = snapshot.semantic_public_items(
            "rocketmq-model",
            rustdoc_associated_fixture(
                root_id="200",
                field_id="501",
                impl_id="777",
                method_id="888",
            ),
        )

        self.assertEqual(first, second)

    def test_public_fields_and_inherent_methods_are_structural_records(self) -> None:
        records = snapshot.semantic_public_items("rocketmq-model", rustdoc_associated_fixture())

        self.assertEqual(
            [
                "rocketmq_model::message::MessageView",
                "rocketmq_model::message::MessageView::build",
                "rocketmq_model::message::MessageView::value",
            ],
            [record["item_path"] for record in records],
        )
        by_path = {record["item_path"]: record for record in records}
        self.assertIn('"primitive":"u64"', by_path["rocketmq_model::message::MessageView::value"]["signature"])
        self.assertNotIn('"implementations"', by_path["rocketmq_model::message::MessageView"]["signature"])

    def test_root_module_proc_macro_derives_are_collected_from_rustdoc_semantics(self) -> None:
        records = snapshot.semantic_public_items("rocketmq-macros", rustdoc_proc_macro_fixture())
        by_path = {record["item_path"]: record for record in records}

        self.assertEqual(
            {
                "rocketmq_macros",
                "rocketmq_macros::RequestHeaderCodec",
                "rocketmq_macros::RequestHeaderCodecV2",
                "rocketmq_macros::RequestHeaderCodecV3",
                "rocketmq_macros::RemotingSerializable",
            },
            set(by_path),
        )
        self.assertNotIn("rocketmq_macros::PrivateDerive", by_path)
        self.assertNotIn("rocketmq_macros::ordinary_function", by_path)
        self.assertEqual("proc_macro", by_path["rocketmq_macros::RequestHeaderCodec"]["kind"])
        self.assertEqual(
            {"kind": "derive", "helpers": ["required"]},
            json.loads(by_path["rocketmq_macros::RequestHeaderCodec"]["signature"]),
        )
        self.assertEqual(
            {
                "kind": "derive",
                "helpers": ["required", "request_header", "request_header_codec_v2"],
            },
            json.loads(by_path["rocketmq_macros::RequestHeaderCodecV2"]["signature"]),
        )
        self.assertEqual(
            {"kind": "derive", "helpers": ["header", "required"]},
            json.loads(by_path["rocketmq_macros::RequestHeaderCodecV3"]["signature"]),
        )
        self.assertEqual(
            {"kind": "derive", "helpers": []},
            json.loads(by_path["rocketmq_macros::RemotingSerializable"]["signature"]),
        )

    def test_root_module_proc_macro_is_deduplicated_when_rustdoc_adds_a_public_path(self) -> None:
        records = snapshot.semantic_public_items(
            "rocketmq-macros",
            rustdoc_proc_macro_fixture(include_v3_path=True),
        )

        self.assertEqual(
            1,
            sum(record["item_path"] == "rocketmq_macros::RequestHeaderCodecV3" for record in records),
        )

    def test_proc_macro_collection_uses_rustdoc_not_attribute_token_formatting(self) -> None:
        semantic_document = rustdoc_proc_macro_fixture()
        formatted_document = rustdoc_proc_macro_fixture()
        formatted_document["index"]["macro-2"]["attrs"] = [
            "#[proc_macro_derive(RequestHeaderCodecV2, attributes(required, request_header, request_header_codec_v2))]",
            "#[doc = concat!(\"token formatting and aliases are not parsed from source\")]",
        ]

        semantic_records = snapshot.semantic_public_items("rocketmq-macros", semantic_document)
        formatted_records = snapshot.semantic_public_items("rocketmq-macros", formatted_document)

        self.assertEqual(semantic_records, formatted_records)
        by_path = {record["item_path"]: record for record in formatted_records}
        self.assertEqual(
            {"kind": "derive", "helpers": ["required", "request_header", "request_header_codec_v2"]},
            json.loads(by_path["rocketmq_macros::RequestHeaderCodecV2"]["signature"]),
        )

    def test_proc_macro_helper_drift_is_a_breaking_structural_change(self) -> None:
        baseline = self.proc_macro_structural_snapshot()
        changed_document = rustdoc_proc_macro_fixture()
        changed_document["index"]["macro-3"]["inner"]["proc_macro"]["helpers"] = ["header"]
        candidate = self.proc_macro_structural_snapshot(changed_document)

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual(1, len(differences))
        self.assertEqual("item-changed", differences[0]["kind"])
        self.assertEqual("rocketmq_macros::RequestHeaderCodecV3", differences[0]["item_path"])
        self.assertEqual(["signature"], differences[0]["changed_fields"])
        self.assertFalse(differences[0]["allowed"])

    def test_legacy_proc_macro_removal_requires_an_exact_post_freeze_approval(self) -> None:
        for item_path in (
            "rocketmq_macros::RequestHeaderCodec",
            "rocketmq_macros::RequestHeaderCodecV2",
        ):
            with self.subTest(item_path=item_path):
                baseline = self.proc_macro_structural_snapshot()
                candidate = copy.deepcopy(baseline)
                public_api = candidate["profiles"]["rocketmq-macros:default"]["public_api"]
                public_api[:] = [item for item in public_api if item["item_path"] != item_path]

                difference = snapshot.compare_snapshots(baseline, candidate)[0]

                self.assertEqual("item-removed", difference["kind"])
                self.assertEqual("breaking", difference["classification"])
                self.assertFalse(difference["allowed"])

                baseline["compatibility_decisions"] = [
                    {
                        "id": "API-POST-MACRO-001",
                        "classification": "approved-break",
                        "applies_to": "post-freeze",
                        "profile_id": "rocketmq-macros:default",
                        "package": "rocketmq-macros",
                        "item_path": item_path,
                        "change": "removed",
                        "replacement": "RequestHeaderCodecV3",
                        "reason": "Synthetic approval used only to test exact matching.",
                        "approved_by": "release-approver",
                        "approved_on": "2026-08-25",
                    }
                ]
                candidate["compatibility_decisions"] = copy.deepcopy(baseline["compatibility_decisions"])
                approved = snapshot.compare_snapshots(baseline, candidate)[0]
                self.assertEqual("approved-break", approved["classification"])
                self.assertTrue(approved["allowed"])

    def test_mismatched_proc_macro_approval_does_not_allow_removal(self) -> None:
        item_path = "rocketmq_macros::RequestHeaderCodec"
        for field, value in (
            ("profile_id", "rocketmq-model:default"),
            ("package", "rocketmq-model"),
            ("item_path", "rocketmq_macros::RequestHeaderCodecV2"),
            ("change", "signature"),
            ("change", "any"),
        ):
            with self.subTest(field=field):
                baseline = self.proc_macro_structural_snapshot()
                candidate = copy.deepcopy(baseline)
                public_api = candidate["profiles"]["rocketmq-macros:default"]["public_api"]
                public_api[:] = [item for item in public_api if item["item_path"] != item_path]
                decision = {
                    "id": "API-POST-MACRO-MISMATCH",
                    "classification": "approved-break",
                    "applies_to": "post-freeze",
                    "profile_id": "rocketmq-macros:default",
                    "package": "rocketmq-macros",
                    "item_path": item_path,
                    "change": "removed",
                    "replacement": "RequestHeaderCodecV3",
                    "reason": "Synthetic mismatch used only to test exact matching.",
                    "approved_by": "release-approver",
                    "approved_on": "2026-08-25",
                }
                decision[field] = value
                baseline["compatibility_decisions"] = [decision]
                candidate["compatibility_decisions"] = copy.deepcopy(baseline["compatibility_decisions"])

                removals = [
                    difference
                    for difference in snapshot.compare_snapshots(baseline, candidate)
                    if difference["kind"] == "item-removed"
                ]

                self.assertEqual(1, len(removals))
                difference = removals[0]
                self.assertEqual(item_path, difference["item"]["item_path"])
                self.assertEqual("breaking", difference["classification"])
                self.assertFalse(difference["allowed"])

    def test_public_field_type_change_is_a_breaking_signature_change(self) -> None:
        baseline = self.structural_snapshot(rustdoc_associated_fixture())
        candidate = self.structural_snapshot(rustdoc_associated_fixture(field_type="u32"))

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual(1, len(differences))
        self.assertEqual("item-changed", differences[0]["kind"])
        self.assertEqual(
            "rocketmq_model::message::MessageView::value",
            differences[0]["item_path"],
        )
        self.assertEqual(["signature"], differences[0]["changed_fields"])
        self.assertFalse(differences[0]["allowed"])

    def test_removed_semantic_item_is_reported_as_breaking(self) -> None:
        baseline = self.structural_snapshot()
        candidate = copy.deepcopy(baseline)
        candidate["profiles"]["rocketmq-model:default"]["public_api"] = []

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual("item-removed", differences[0]["kind"])
        self.assertEqual("breaking", differences[0]["classification"])
        self.assertFalse(differences[0]["allowed"])

    def test_signature_change_is_one_breaking_change_not_remove_plus_add(self) -> None:
        baseline = self.structural_snapshot()
        candidate = copy.deepcopy(baseline)
        candidate["profiles"]["rocketmq-model:default"]["public_api"][0]["signature"] = "changed"

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual(1, len(differences))
        self.assertEqual("item-changed", differences[0]["kind"])
        self.assertEqual(["signature"], differences[0]["changed_fields"])
        self.assertEqual("breaking", differences[0]["classification"])
        self.assertFalse(differences[0]["allowed"])

    def test_post_freeze_approval_allows_only_the_matching_break(self) -> None:
        baseline = self.structural_snapshot()
        baseline["compatibility_decisions"] = [
            {
                "id": "API-POST-001",
                "classification": "approved-break",
                "applies_to": "post-freeze",
                "profile_id": "rocketmq-model:default",
                "package": "rocketmq-model",
                "item_path": "rocketmq_model::message::MessageView",
                "change": "signature",
                "replacement": "rocketmq_model::message::MessageViewV2",
                "reason": "The replacement preserves the supported behavior.",
                "approved_by": "release-approver",
                "approved_on": "2026-08-16",
            }
        ]
        candidate = copy.deepcopy(baseline)
        candidate["profiles"]["rocketmq-model:default"]["public_api"][0]["signature"] = "changed"

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual(1, len(differences))
        self.assertEqual("approved-break", differences[0]["classification"])
        self.assertEqual("API-POST-001", differences[0]["decision_id"])
        self.assertTrue(differences[0]["allowed"])

    def test_addition_is_compatible_without_an_approval(self) -> None:
        baseline = self.structural_snapshot()
        candidate = copy.deepcopy(baseline)
        added = copy.deepcopy(candidate["profiles"]["rocketmq-model:default"]["public_api"][0])
        added["item_path"] = "rocketmq_model::message::NewMessageView"
        candidate["profiles"]["rocketmq-model:default"]["public_api"].append(added)

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual(1, len(differences))
        self.assertEqual("compatible-addition", differences[0]["classification"])
        self.assertTrue(differences[0]["allowed"])

    def test_feature_profiles_include_defaults_and_each_frozen_matrix_entry(self) -> None:
        targets = [("demo", "demo")]
        package_features = {
            "demo": {
                "default": ["tls"],
                "tls": ["dep:tls"],
                "trace": ["dep:trace"],
            }
        }
        matrix = (
            SimpleNamespace(
                id="demo-no-default",
                group="feature",
                command=("cargo", "check", "-p", "demo", "--no-default-features"),
            ),
            SimpleNamespace(
                id="demo-trace",
                group="feature",
                command=(
                    "cargo",
                    "check",
                    "-p",
                    "demo",
                    "--no-default-features",
                    "--features",
                    "trace",
                ),
            ),
            SimpleNamespace(
                id="demo-all",
                group="feature",
                command=("cargo", "check", "-p", "demo", "--all-features"),
            ),
            SimpleNamespace(
                id="ignored-wire",
                group="wire",
                command=("cargo", "test", "-p", "demo"),
            ),
        )

        profiles = snapshot.derive_feature_profiles(targets, package_features, matrix)

        self.assertEqual(
            ["demo:default", "demo-all", "demo-no-default", "demo-trace"],
            [profile["id"] for profile in profiles],
        )
        self.assertEqual(
            {
                "id": "demo:default",
                "package": "demo",
                "target": "demo",
                "default_features": True,
                "all_features": False,
                "features": [],
                "declared_default_features": ["tls"],
                "source": "workspace-default",
                "matrix_ids": [],
            },
            profiles[0],
        )
        self.assertEqual(["trace"], profiles[3]["features"])
        self.assertFalse(profiles[3]["default_features"])
        self.assertTrue(profiles[1]["all_features"])

    def test_rustdoc_profile_command_uses_only_supported_cargo_rustdoc_flags(self) -> None:
        profile = {
            "id": "demo-trace",
            "package": "demo",
            "target": "demo",
            "default_features": False,
            "all_features": False,
            "features": ["trace"],
        }

        command = snapshot._rustdoc_command(profile)

        self.assertIn("--locked", command)
        self.assertNotIn("--no-deps", command)
        self.assertEqual(
            ["--no-default-features", "--features", "trace"],
            command[command.index("--lib") + 1 : command.index("--")],
        )

    @staticmethod
    def structural_snapshot(document: dict[str, object] | None = None) -> dict[str, object]:
        public_api = snapshot.semantic_public_items(
            "rocketmq-model",
            document or rustdoc_fixture(),
        )
        return {
            "schema_version": 3,
            "identity": "structural",
            "scope": "core-release",
            "freeze": {
                "version": "1.0.0-rc.1",
                "breaking_change_policy": "approval-required-after-freeze",
            },
            "toolchain": {"rustc": "x", "rustdoc": "x", "cargo": "x"},
            "packages": {
                "rocketmq-model": {
                    "target": "rocketmq_model",
                    "profile_ids": ["rocketmq-model:default"],
                }
            },
            "profiles": {
                "rocketmq-model:default": {
                    "package": "rocketmq-model",
                    "target": "rocketmq_model",
                    "default_features": True,
                    "all_features": False,
                    "features": [],
                    "declared_default_features": [],
                    "source": "workspace-default",
                    "matrix_ids": [],
                    "crate_version": "1.0.0",
                    "public_api": public_api,
                }
            },
            "public_api_intent": {},
            "compatibility_decisions": [],
            "frozen_contracts": [],
        }

    @staticmethod
    def proc_macro_structural_snapshot(document: dict[str, object] | None = None) -> dict[str, object]:
        public_api = snapshot.semantic_public_items(
            "rocketmq-macros",
            document or rustdoc_proc_macro_fixture(),
        )
        return {
            "schema_version": 3,
            "identity": "structural",
            "scope": "core-release",
            "freeze": {
                "version": "1.0.0-rc.1",
                "breaking_change_policy": "approval-required-after-freeze",
            },
            "toolchain": {"rustc": "x", "rustdoc": "x", "cargo": "x"},
            "packages": {
                "rocketmq-macros": {
                    "target": "rocketmq_macros",
                    "profile_ids": ["rocketmq-macros:default"],
                }
            },
            "profiles": {
                "rocketmq-macros:default": {
                    "package": "rocketmq-macros",
                    "target": "rocketmq_macros",
                    "default_features": True,
                    "all_features": False,
                    "features": [],
                    "declared_default_features": [],
                    "source": "workspace-default",
                    "matrix_ids": [],
                    "crate_version": "1.0.0",
                    "public_api": public_api,
                }
            },
            "public_api_intent": {},
            "compatibility_decisions": [],
            "frozen_contracts": [],
        }


if __name__ == "__main__":
    unittest.main()
