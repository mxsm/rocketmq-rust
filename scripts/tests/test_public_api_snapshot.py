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
from pathlib import Path
import tempfile
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


def rustdoc_reexport_fixture() -> dict[str, object]:
    def item(name: str | None, visibility: str, kind: str, value: object) -> dict[str, object]:
        return {
            "id": name,
            "crate_id": 0,
            "name": name,
            "visibility": visibility,
            "attrs": [],
            "inner": {kind: value},
        }

    index: dict[str, object] = {
        "root": item(
            "demo",
            "public",
            "module",
            {
                "is_crate": True,
                "is_stripped": False,
                "items": ["api", "private", "alias-a-use", "alias-b-use", "root-api-glob"],
            },
        ),
        "api": item(
            "api",
            "public",
            "module",
            {
                "is_crate": False,
                "is_stripped": False,
                "items": [
                    "plain-use",
                    "renamed-use",
                    "external-use",
                    "glob-use",
                    "trait-use",
                    "error-use",
                    "existing-use",
                    "private-use",
                    "child",
                ],
            },
        ),
        "child": item(
            "Child",
            "public",
            "module",
            {"is_crate": False, "is_stripped": False, "items": ["child-thing"]},
        ),
        "child-thing": item(
            "ChildThing",
            "public",
            "struct",
            {"kind": "unit", "generics": {"params": [], "where_predicates": []}, "impls": []},
        ),
        "private": item(
            "private",
            "default",
            "module",
            {"is_crate": False, "is_stripped": False, "items": ["hidden-use"]},
        ),
        "source": item(
            "source",
            "public",
            "module",
            {
                "is_crate": False,
                "is_stripped": False,
                "items": ["glob-thing", "nested-glob", "cycle-glob"],
            },
        ),
        "nested": item(
            "nested",
            "public",
            "module",
            {"is_crate": False, "is_stripped": False, "items": ["nested-thing"]},
        ),
        "cycle-a": item(
            "cycle_a",
            "public",
            "module",
            {"is_crate": False, "is_stripped": False, "items": ["cycle-a-glob"]},
        ),
        "cycle-b": item(
            "cycle_b",
            "public",
            "module",
            {"is_crate": False, "is_stripped": False, "items": ["cycle-b-glob", "cycle-thing"]},
        ),
        "thing": item(
            "Thing",
            "public",
            "struct",
            {"kind": "unit", "generics": {"params": [], "where_predicates": []}, "impls": []},
        ),
        "glob-thing": item(
            "GlobThing",
            "public",
            "struct",
            {"kind": "unit", "generics": {"params": [], "where_predicates": []}, "impls": []},
        ),
        "nested-thing": item(
            "NestedThing",
            "public",
            "struct",
            {"kind": "unit", "generics": {"params": [], "where_predicates": []}, "impls": []},
        ),
        "cycle-thing": item(
            "CycleThing",
            "public",
            "struct",
            {"kind": "unit", "generics": {"params": [], "where_predicates": []}, "impls": []},
        ),
        "trait": item(
            "Trait",
            "public",
            "trait",
            {
                "is_auto": False,
                "is_unsafe": False,
                "is_dyn_compatible": True,
                "items": ["compile", "try-compile"],
                "generics": {"params": [], "where_predicates": []},
                "bounds": [],
                "implementations": [],
            },
        ),
        "error": item(
            "Error",
            "public",
            "struct",
            {
                "kind": {"plain": {"fields": [], "has_stripped_fields": False}},
                "generics": {"params": [], "where_predicates": []},
                "impls": ["error-impl"],
            },
        ),
        "compile": item(
            "compile",
            "default",
            "function",
            {
                "sig": {"inputs": [], "output": {"primitive": "bool"}, "is_c_variadic": False},
                "generics": {"params": [], "where_predicates": []},
                "header": {"is_const": False, "is_unsafe": False, "is_async": False, "abi": "Rust"},
                "has_body": False,
            },
        ),
        "try-compile": item(
            "try_compile",
            "default",
            "function",
            {
                "sig": {"inputs": [], "output": {"primitive": "bool"}, "is_c_variadic": False},
                "generics": {"params": [], "where_predicates": []},
                "header": {"is_const": False, "is_unsafe": False, "is_async": False, "abi": "Rust"},
                "has_body": True,
            },
        ),
        "error-impl": item(
            None,
            "default",
            "impl",
            {
                "trait": None,
                "is_synthetic": False,
                "items": ["error-new", "error-message"],
            },
        ),
        "error-new": item(
            "new",
            "public",
            "function",
            {
                "sig": {"inputs": [], "output": {"generic": "Self"}, "is_c_variadic": False},
                "generics": {"params": [], "where_predicates": []},
                "header": {"is_const": False, "is_unsafe": False, "is_async": False, "abi": "Rust"},
                "has_body": True,
            },
        ),
        "error-message": item(
            "message",
            "public",
            "function",
            {
                "sig": {"inputs": [], "output": {"primitive": "str"}, "is_c_variadic": False},
                "generics": {"params": [], "where_predicates": []},
                "header": {"is_const": False, "is_unsafe": False, "is_async": False, "abi": "Rust"},
                "has_body": True,
            },
        ),
        "existing": item(
            "Existing",
            "public",
            "struct",
            {"kind": "unit", "generics": {"params": [], "where_predicates": []}, "impls": []},
        ),
    }
    for use_id, visibility, name, target_id, is_glob in (
        ("plain-use", "public", "Thing", "thing", False),
        ("renamed-use", "public", "RenamedThing", "thing", False),
        ("external-use", "public", "ExternalThing", "external", False),
        ("glob-use", "public", None, "source", True),
        ("trait-use", "public", "Trait", "trait", False),
        ("error-use", "public", "Error", "error", False),
        ("existing-use", "public", "Existing", "thing", False),
        ("private-use", "default", "PrivateImport", "thing", False),
        ("hidden-use", "public", "HiddenImport", "thing", False),
        ("nested-glob", "public", None, "nested", True),
        ("cycle-glob", "public", None, "cycle-a", True),
        ("cycle-a-glob", "public", None, "cycle-b", True),
        ("cycle-b-glob", "public", None, "cycle-a", True),
        ("alias-a-use", "public", "AliasA", "api", False),
        ("alias-b-use", "public", "AliasB", "api", False),
        ("root-api-glob", "public", None, "api", True),
    ):
        index[use_id] = item(
            None,
            visibility,
            "use",
            {"source": "semantic-only", "name": name, "id": target_id, "is_glob": is_glob},
        )
    paths = {
        "root": {"crate_id": 0, "path": ["demo"], "kind": "module"},
        "api": {"crate_id": 0, "path": ["demo", "api"], "kind": "module"},
        "child": {"crate_id": 0, "path": ["demo", "api", "Child"], "kind": "module"},
        "child-thing": {"crate_id": 0, "path": ["demo", "api", "Child", "ChildThing"], "kind": "struct"},
        "private": {"crate_id": 0, "path": ["demo", "private"], "kind": "module"},
        "source": {"crate_id": 0, "path": ["demo", "api", "source"], "kind": "module"},
        "nested": {"crate_id": 0, "path": ["demo", "api", "source", "nested"], "kind": "module"},
        "cycle-a": {"crate_id": 0, "path": ["demo", "api", "source", "cycle_a"], "kind": "module"},
        "cycle-b": {"crate_id": 0, "path": ["demo", "api", "source", "cycle_b"], "kind": "module"},
        "thing": {"crate_id": 0, "path": ["demo", "hidden", "Thing"], "kind": "struct"},
        "glob-thing": {"crate_id": 0, "path": ["demo", "api", "source", "GlobThing"], "kind": "struct"},
        "nested-thing": {"crate_id": 0, "path": ["demo", "api", "source", "nested", "NestedThing"], "kind": "struct"},
        "cycle-thing": {"crate_id": 0, "path": ["demo", "api", "source", "cycle_b", "CycleThing"], "kind": "struct"},
        "trait": {"crate_id": 0, "path": ["demo", "hidden", "Trait"], "kind": "trait"},
        "error": {"crate_id": 0, "path": ["demo", "hidden", "Error"], "kind": "struct"},
        "existing": {"crate_id": 0, "path": ["demo", "api", "Existing"], "kind": "struct"},
        "external": {"crate_id": 4, "path": ["external_crate", "ExternalThing"], "kind": "struct"},
    }
    return {"crate_version": "1.0.0", "root": "root", "paths": paths, "index": index}


def rustdoc_binding_resolution_fixture(
    *,
    explicit_after_glob: bool = False,
    glob_target_ids: tuple[str, ...] = ("glob-thing",),
    include_explicit: bool = True,
) -> dict[str, object]:
    """A minimal semantic fixture for module bindings and Rust shadowing rules."""

    def item(name: str | None, kind: str, value: object) -> dict[str, object]:
        return {
            "id": name,
            "crate_id": 0,
            "name": name,
            "visibility": "public",
            "attrs": [],
            "inner": {kind: value},
        }

    def unit_struct(name: str) -> dict[str, object]:
        return item(
            name,
            "struct",
            {"kind": "unit", "generics": {"params": [], "where_predicates": []}, "impls": []},
        )

    index: dict[str, object] = {
        "root": item("demo", "module", {"is_crate": True, "is_stripped": False, "items": ["api"]}),
        "api": item("api", "module", {"is_crate": False, "is_stripped": False, "items": []}),
        "explicit-thing": unit_struct("Thing"),
        "glob-thing": unit_struct("Thing"),
        "shared-thing": unit_struct("Thing"),
        "same-path-a": unit_struct("Thing"),
        "same-path-b": item(
            "Thing",
            "struct",
            {
                "kind": "unit",
                "generics": {
                    "params": [{"name": "T", "kind": {"type": {"bounds": []}}}],
                    "where_predicates": [],
                },
                "impls": [],
            },
        ),
    }
    api_items: list[str] = []
    glob_items: list[str] = []
    for position, target_id in enumerate(glob_target_ids):
        source_id = f"glob-source-{position}"
        source_use_id = f"glob-source-{position}-thing"
        glob_use_id = f"glob-use-{position}"
        index[source_id] = item(
            f"glob_source_{position}",
            "module",
            {"is_crate": False, "is_stripped": False, "items": [source_use_id]},
        )
        index[source_use_id] = item(
            None,
            "use",
            {"source": "semantic-only", "name": "Thing", "id": target_id, "is_glob": False},
        )
        index[glob_use_id] = item(
            None,
            "use",
            {"source": "semantic-only", "name": None, "id": source_id, "is_glob": True},
        )
        glob_items.append(glob_use_id)
    if include_explicit:
        index["explicit-use"] = item(
            None,
            "use",
            {"source": "semantic-only", "name": "Thing", "id": "explicit-thing", "is_glob": False},
        )
        if explicit_after_glob:
            api_items.extend([*glob_items, "explicit-use"])
        else:
            api_items.extend(["explicit-use", *glob_items])
    else:
        api_items.extend(glob_items)
    index["api"]["inner"]["module"]["items"] = api_items

    paths: dict[str, object] = {
        "root": {"crate_id": 0, "path": ["demo"], "kind": "module"},
        "api": {"crate_id": 0, "path": ["demo", "api"], "kind": "module"},
        "explicit-thing": {"crate_id": 0, "path": ["demo", "hidden", "explicit", "Thing"], "kind": "struct"},
        "glob-thing": {"crate_id": 0, "path": ["demo", "hidden", "glob", "Thing"], "kind": "struct"},
        "shared-thing": {"crate_id": 0, "path": ["demo", "hidden", "shared", "Thing"], "kind": "struct"},
        "same-path-a": {"crate_id": 0, "path": ["demo", "hidden", "same", "Thing"], "kind": "struct"},
        "same-path-b": {"crate_id": 0, "path": ["demo", "hidden", "same", "Thing"], "kind": "struct"},
    }
    for position in range(len(glob_target_ids)):
        paths[f"glob-source-{position}"] = {
            "crate_id": 0,
            "path": ["demo", "hidden", f"glob_source_{position}"],
            "kind": "module",
        }
    return {"crate_version": "1.0.0", "root": "root", "paths": paths, "index": index}


def rustdoc_external_module_reexport_fixture() -> dict[str, object]:
    def item(name: str | None, kind: str, value: object) -> dict[str, object]:
        return {
            "id": name,
            "crate_id": 0,
            "name": name,
            "visibility": "public",
            "attrs": [],
            "inner": {kind: value},
        }

    index = {
        "root": item(
            "demo",
            "module",
            {"is_crate": True, "is_stripped": False, "items": ["alias-use", "glob-use"]},
        ),
        "alias-use": item(
            None,
            "use",
            {"source": "external_crate::api", "name": "Alias", "id": "external-api", "is_glob": False},
        ),
        "glob-use": item(
            None,
            "use",
            {"source": "external_crate::api", "name": None, "id": "external-api", "is_glob": True},
        ),
    }
    paths = {
        "root": {"crate_id": 0, "path": ["demo"], "kind": "module"},
        "external-api": {"crate_id": 4, "path": ["external_crate", "api"], "kind": "module"},
        "external-child": {"crate_id": 4, "path": ["external_crate", "api", "Child"], "kind": "module"},
        "external-thing": {
            "crate_id": 4,
            "path": ["external_crate", "api", "Child", "Thing"],
            "kind": "struct",
        },
    }
    return {"crate_version": "1.0.0", "root": "root", "paths": paths, "index": index}


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


class PublicApiReexportTests(unittest.TestCase):
    SELECTED_PATHS = (
        "demo::api::Thing",
        "demo::api::RenamedThing",
        "demo::api::ExternalThing",
        "demo::api::GlobThing",
        "demo::api::NestedThing",
        "demo::api::CycleThing",
        "demo::api::Trait",
        "demo::api::Trait::compile",
        "demo::api::Trait::try_compile",
        "demo::api::Error",
        "demo::api::Error::new",
        "demo::api::Error::message",
        "demo::api::Existing",
    )

    @staticmethod
    def _selected_records(document: dict[str, object] | None = None) -> dict[str, dict[str, str]]:
        records = snapshot.semantic_public_items(
            "rocketmq-model",
            document or rustdoc_reexport_fixture(),
            selected_reexport_paths=PublicApiReexportTests.SELECTED_PATHS,
        )
        return {record["item_path"]: record for record in records}

    @staticmethod
    def _snapshot(records: list[dict[str, str]]) -> dict[str, object]:
        baseline = PublicApiSnapshotTests.structural_snapshot()
        baseline["profiles"]["rocketmq-model:default"]["public_api"] = records
        return baseline

    def test_reexport_collection_is_opt_in_and_private_edges_do_not_leak(self) -> None:
        records = snapshot.semantic_public_items("rocketmq-model", rustdoc_reexport_fixture())
        paths = {record["item_path"] for record in records}

        self.assertNotIn("demo::api::Thing", paths)
        self.assertNotIn("demo::api::PrivateImport", paths)
        self.assertNotIn("demo::private::HiddenImport", paths)

    def test_selected_reexports_cover_plain_rename_glob_nested_cycle_and_external_targets(self) -> None:
        records = self._selected_records()

        self.assertEqual(set(self.SELECTED_PATHS), set(self.SELECTED_PATHS) & set(records))
        self.assertEqual("reexport", records["demo::api::Thing"]["kind"])
        self.assertEqual("plain", json.loads(records["demo::api::Thing"]["signature"])["alias_kind"])
        self.assertEqual("renamed", json.loads(records["demo::api::RenamedThing"]["signature"])["alias_kind"])
        self.assertEqual("glob", json.loads(records["demo::api::GlobThing"]["signature"])["alias_kind"])
        self.assertIn("demo::api::NestedThing", records)
        self.assertIn("demo::api::CycleThing", records)
        external_signature = json.loads(records["demo::api::ExternalThing"]["signature"])
        self.assertEqual("external_crate::ExternalThing", external_signature["target_path"])
        self.assertIsNone(external_signature["target_signature"])

    def test_module_reexport_aliases_each_expose_their_public_descendants(self) -> None:
        selected = (
            "demo::AliasA::Child::ChildThing",
            "demo::AliasB::Child::ChildThing",
        )
        records = {
            record["item_path"]: record
            for record in snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_reexport_fixture(),
                selected_reexport_paths=selected,
            )
        }

        self.assertEqual(set(selected), set(selected) & set(records))
        for item_path in selected:
            signature = json.loads(records[item_path]["signature"])
            self.assertEqual("module-descendant", signature["alias_kind"])
            self.assertEqual("demo::api::Child::ChildThing", signature["target_path"])

    def test_glob_exported_child_module_exposes_its_public_subtree(self) -> None:
        item_path = "demo::Child::ChildThing"
        records = {
            record["item_path"]: record
            for record in snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_reexport_fixture(),
                selected_reexport_paths=(item_path,),
            )
        }

        signature = json.loads(records[item_path]["signature"])
        self.assertEqual("module-descendant", signature["alias_kind"])
        self.assertEqual("demo::api::Child::ChildThing", signature["target_path"])

    def test_explicit_binding_shadows_glob_regardless_of_rustdoc_item_order(self) -> None:
        item_path = "demo::api::Thing"
        for explicit_after_glob in (False, True):
            with self.subTest(explicit_after_glob=explicit_after_glob):
                records = {
                    record["item_path"]: record
                    for record in snapshot.semantic_public_items(
                        "rocketmq-model",
                        rustdoc_binding_resolution_fixture(
                            explicit_after_glob=explicit_after_glob,
                        ),
                        selected_reexport_paths=(item_path,),
                    )
                }
                signature = json.loads(records[item_path]["signature"])
                self.assertEqual("plain", signature["alias_kind"])
                self.assertEqual("demo::hidden::explicit::Thing", signature["target_path"])

    def test_glob_bindings_with_the_same_target_dedupe_but_conflicts_fail_closed(self) -> None:
        item_path = "demo::api::Thing"
        records = {
            record["item_path"]: record
            for record in snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_binding_resolution_fixture(
                    glob_target_ids=("shared-thing", "shared-thing"),
                    include_explicit=False,
                ),
                selected_reexport_paths=(item_path,),
            )
        }
        self.assertEqual("glob", json.loads(records[item_path]["signature"])["alias_kind"])
        self.assertEqual("demo::hidden::shared::Thing", json.loads(records[item_path]["signature"])["target_path"])

        with self.assertRaisesRegex(snapshot.SnapshotError, "ambiguous public glob bindings"):
            snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_binding_resolution_fixture(
                    glob_target_ids=("glob-thing", "shared-thing"),
                    include_explicit=False,
                ),
                selected_reexport_paths=(item_path,),
            )

        with self.assertRaisesRegex(snapshot.SnapshotError, "ambiguous public glob bindings"):
            snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_binding_resolution_fixture(
                    glob_target_ids=("same-path-a", "same-path-b"),
                    include_explicit=False,
                ),
                selected_reexport_paths=(item_path,),
            )

    def test_external_module_alias_and_glob_subtrees_use_rustdoc_paths_only(self) -> None:
        alias_path = "demo::Alias::Child::Thing"
        glob_path = "demo::Child::Thing"
        records = {
            record["item_path"]: record
            for record in snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_external_module_reexport_fixture(),
                selected_reexport_paths=(alias_path, glob_path),
            )
        }

        alias_signature = json.loads(records[alias_path]["signature"])
        glob_signature = json.loads(records[glob_path]["signature"])
        self.assertEqual("renamed-external-descendant", alias_signature["alias_kind"])
        self.assertEqual("glob-external-descendant", glob_signature["alias_kind"])
        self.assertEqual("external_crate::api::Child::Thing", alias_signature["target_path"])
        self.assertEqual("external_crate::api::Child::Thing", glob_signature["target_path"])
        self.assertIsNone(alias_signature["target_signature"])
        self.assertIsNone(glob_signature["target_signature"])

    def test_external_module_alias_target_drift_changes_the_public_facade_signature(self) -> None:
        item_path = "demo::Alias::Child::Thing"
        before = {
            record["item_path"]: record
            for record in snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_external_module_reexport_fixture(),
                selected_reexport_paths=(item_path,),
            )
        }[item_path]
        changed_document = rustdoc_external_module_reexport_fixture()
        changed_document["paths"]["external-api"]["path"] = ["external_crate", "renamed_api"]
        changed_document["paths"]["external-child"]["path"] = [
            "external_crate",
            "renamed_api",
            "Child",
        ]
        changed_document["paths"]["external-thing"]["path"] = [
            "external_crate",
            "renamed_api",
            "Child",
            "Thing",
        ]
        after = {
            record["item_path"]: record
            for record in snapshot.semantic_public_items(
                "rocketmq-model",
                changed_document,
                selected_reexport_paths=(item_path,),
            )
        }[item_path]

        differences = snapshot.compare_snapshots(self._snapshot([before]), self._snapshot([after]))

        self.assertEqual(1, len(differences))
        self.assertEqual("item-changed", differences[0]["kind"])
        self.assertEqual(["signature"], differences[0]["changed_fields"])
        self.assertFalse(differences[0]["allowed"])

    def test_selected_associated_items_use_the_public_alias_parent_and_target_semantics(self) -> None:
        records = self._selected_records()
        try_compile = json.loads(records["demo::api::Trait::try_compile"]["signature"])
        error_new = json.loads(records["demo::api::Error::new"]["signature"])

        self.assertEqual("plain-associated", try_compile["alias_kind"])
        self.assertEqual("demo::hidden::Trait::try_compile", try_compile["target_path"])
        self.assertEqual("function", try_compile["target_kind"])
        self.assertEqual({"primitive": "bool"}, try_compile["target_signature"]["sig"]["output"])
        self.assertEqual("demo::hidden::Error::new", error_new["target_path"])
        self.assertEqual("function", records["demo::hidden::Trait::try_compile"]["kind"])
        self.assertEqual("reexport", records["demo::api::Trait::try_compile"]["kind"])

    def test_existing_canonical_path_wins_dedupe_over_selected_reexport(self) -> None:
        records = self._selected_records()

        self.assertEqual("struct", records["demo::api::Existing"]["kind"])
        self.assertEqual(
            1,
            sum(record["item_path"] == "demo::api::Existing" for record in records.values()),
        )

    def test_associated_target_signature_drift_is_a_breaking_public_facade_change(self) -> None:
        before = self._selected_records()["demo::api::Trait::try_compile"]
        changed_document = rustdoc_reexport_fixture()
        changed_document["index"]["try-compile"]["inner"]["function"]["sig"]["output"] = {
            "primitive": "str"
        }
        after = self._selected_records(changed_document)["demo::api::Trait::try_compile"]
        baseline = self._snapshot([before])
        candidate = self._snapshot([after])

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual(1, len(differences))
        self.assertEqual("item-changed", differences[0]["kind"])
        self.assertEqual("demo::api::Trait::try_compile", differences[0]["item_path"])
        self.assertEqual(["signature"], differences[0]["changed_fields"])
        self.assertFalse(differences[0]["allowed"])

    def test_alias_retarget_is_a_breaking_public_facade_change(self) -> None:
        before = self._selected_records()["demo::api::Thing"]
        changed_document = rustdoc_reexport_fixture()
        changed_document["paths"]["thing"]["path"] = ["demo", "other", "Thing"]
        after = self._selected_records(changed_document)["demo::api::Thing"]
        baseline = self._snapshot([before])
        candidate = self._snapshot([after])

        differences = snapshot.compare_snapshots(baseline, candidate)

        self.assertEqual(1, len(differences))
        self.assertEqual("item-changed", differences[0]["kind"])
        self.assertEqual("demo::api::Thing", differences[0]["item_path"])
        self.assertEqual(["signature"], differences[0]["changed_fields"])
        self.assertFalse(differences[0]["allowed"])

    def test_selected_reexport_removal_requires_an_exact_approval(self) -> None:
        record = self._selected_records()["demo::api::Trait::try_compile"]
        baseline = self._snapshot([record])
        candidate = self._snapshot([])

        unapproved = snapshot.compare_snapshots(baseline, candidate)
        self.assertEqual("item-removed", unapproved[0]["kind"])
        self.assertFalse(unapproved[0]["allowed"])

        decision = {
            "id": "API-POST-REEXPORT-001",
            "classification": "approved-break",
            "applies_to": "post-freeze",
            "profile_id": "rocketmq-model:default",
            "package": "rocketmq-model",
            "item_path": "demo::api::Trait::try_compile",
            "change": "removed",
            "replacement": "demo::api::Trait::try_compile_v2",
            "reason": "Synthetic exact-match approval test.",
            "approved_by": "release-approver",
            "approved_on": "2026-08-25",
        }
        baseline["compatibility_decisions"] = [decision]
        candidate["compatibility_decisions"] = copy.deepcopy(baseline["compatibility_decisions"])

        approved = snapshot.compare_snapshots(baseline, candidate)
        self.assertEqual("approved-break", approved[0]["classification"])
        self.assertTrue(approved[0]["allowed"])

    def test_reexport_mismatched_approvals_do_not_allow_removal_or_signature_changes(self) -> None:
        record = self._selected_records()["demo::api::Trait::try_compile"]
        for field, value in (
            ("profile_id", "rocketmq-filter:default"),
            ("package", "rocketmq-filter"),
            ("item_path", "demo::api::Trait::compile"),
            ("change", "signature"),
            ("change", "any"),
        ):
            with self.subTest(removal_field=field, removal_value=value):
                baseline = self._snapshot([record])
                candidate = self._snapshot([])
                decision = {
                    "id": "API-POST-REEXPORT-MISMATCH",
                    "classification": "approved-break",
                    "applies_to": "post-freeze",
                    "profile_id": "rocketmq-model:default",
                    "package": "rocketmq-model",
                    "item_path": "demo::api::Trait::try_compile",
                    "change": "removed",
                    "replacement": "demo::api::Trait::try_compile_v2",
                    "reason": "Synthetic re-export mismatch test.",
                    "approved_by": "release-approver",
                    "approved_on": "2026-08-25",
                }
                decision[field] = value
                baseline["compatibility_decisions"] = [decision]
                candidate["compatibility_decisions"] = copy.deepcopy(baseline["compatibility_decisions"])

                difference = snapshot.compare_snapshots(baseline, candidate)[0]
                self.assertEqual("item-removed", difference["kind"])
                self.assertFalse(difference["allowed"])

        before = self._selected_records()["demo::api::Thing"]
        changed_document = rustdoc_reexport_fixture()
        changed_document["paths"]["thing"]["path"] = ["demo", "other", "Thing"]
        after = self._selected_records(changed_document)["demo::api::Thing"]
        baseline = self._snapshot([before])
        candidate = self._snapshot([after])
        decision = {
            "id": "API-POST-REEXPORT-ANY",
            "classification": "approved-break",
            "applies_to": "post-freeze",
            "profile_id": "rocketmq-model:default",
            "package": "rocketmq-model",
            "item_path": "demo::api::Thing",
            "change": "any",
            "replacement": "demo::api::ThingV2",
            "reason": "Wildcard approvals are not accepted.",
            "approved_by": "release-approver",
            "approved_on": "2026-08-25",
        }
        baseline["compatibility_decisions"] = [decision]
        candidate["compatibility_decisions"] = copy.deepcopy(baseline["compatibility_decisions"])

        difference = snapshot.compare_snapshots(baseline, candidate)[0]
        self.assertEqual("item-changed", difference["kind"])
        self.assertFalse(difference["allowed"])

    def test_selection_and_inventory_fail_closed(self) -> None:
        profiles = [
            {"id": "demo:default", "package": "demo-package", "target": "demo"},
        ]
        valid = {
            "schema_version": 1,
            "profiles": {
                "demo:default": {
                    "package": "demo-package",
                    "item_paths": ["demo::api::Thing"],
                }
            },
        }
        invalid_documents = (
            {"schema_version": 1, "profiles": {"unknown:default": valid["profiles"]["demo:default"]}},
            {"schema_version": 1, "profiles": {"demo:default": {"package": "wrong", "item_paths": ["demo::api::Thing"]}}},
            {"schema_version": 1, "profiles": {"demo:default": {"package": "demo-package"}}},
            {"schema_version": 1, "profiles": {"demo:default": {"package": "demo-package", "item_paths": []}}},
            {"schema_version": 1, "profiles": {"demo:default": {"package": "demo-package", "item_paths": ["demo::api::Thing", "demo::api::Thing"]}}},
            {"schema_version": 1, "profiles": {"demo:default": {"package": "demo-package", "item_paths": ["demo::api::*"]}}},
            {"schema_version": 1, "profiles": {"demo:default": {"package": "demo-package", "item_paths": ["other::Thing"]}}},
        )
        with tempfile.TemporaryDirectory() as directory:
            inventory_path = Path(directory) / "inventory.json"
            inventory_path.write_text(json.dumps(valid), encoding="utf-8")
            self.assertEqual(
                {"demo:default": ("demo::api::Thing",)},
                snapshot.load_reexport_surface_inventory(profiles, inventory_path),
            )
            for invalid in invalid_documents:
                with self.subTest(invalid=invalid):
                    inventory_path.write_text(json.dumps(invalid), encoding="utf-8")
                    with self.assertRaises(snapshot.SnapshotError):
                        snapshot.load_reexport_surface_inventory(profiles, inventory_path)

        with self.assertRaises(snapshot.SnapshotError):
            snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_reexport_fixture(),
                selected_reexport_paths=("demo::api::Thing", "demo::api::Thing"),
            )
        with self.assertRaises(snapshot.SnapshotError):
            snapshot.semantic_public_items(
                "rocketmq-model",
                rustdoc_reexport_fixture(),
                selected_reexport_paths=("demo::api::Missing",),
            )


if __name__ == "__main__":
    unittest.main()
