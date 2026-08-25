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

"""Generate and verify the core-release structural public API freeze."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent))
import core_release_scope
import m09_compatibility_matrix


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_API_INTENT = ROOT / "scripts" / "public-api-intent.json"
DEFAULT_FREEZE_POLICY = ROOT / "scripts" / "public-api-freeze-policy.json"
DEFAULT_REEXPORT_SURFACE_INVENTORY = ROOT / "scripts" / "public-api-reexport-surfaces.json"
SCHEMA_VERSION = 3
IDENTITY = "structural"
REEXPORT_SURFACE_INVENTORY_SCHEMA_VERSION = 1
LIB_KINDS = {"lib", "rlib", "proc-macro"}
RUSTDOC_TOOLCHAIN = "nightly-2026-07-05"
STRUCTURAL_ITEM_FIELDS = (
    "package",
    "module",
    "item_path",
    "kind",
    "visibility",
    "signature",
    "feature",
)
PROFILE_SPEC_FIELDS = (
    "package",
    "target",
    "default_features",
    "all_features",
    "features",
    "declared_default_features",
    "source",
    "matrix_ids",
)
COMPATIBILITY_CLASSIFICATIONS = {
    "compatible-addition",
    "approved-break",
    "renamed-wrapper",
    "removed-placeholder",
}
BREAKING_CLASSIFICATIONS = {
    "approved-break",
    "renamed-wrapper",
    "removed-placeholder",
}
REQUIRED_FROZEN_CAPABILITIES = {"F-13", "F-15", "F-18"}
VOLATILE_RUSTDOC_KEYS = frozenset(
    {"id", "fields", "impls", "implementations", "items", "variants"}
)
FEATURE_RE = re.compile(r"feature\s*=\s*\"([^\"]+)\"")


class SnapshotError(RuntimeError):
    """A public API input, build, or freeze contract is invalid."""


def run(command: list[str]) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        raise SnapshotError(
            f"command failed ({result.returncode}): {' '.join(command)}\n{result.stdout}{result.stderr}"
        )
    return result.stdout.strip()


def workspace_metadata() -> dict[str, Any]:
    value = json.loads(
        run(["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"])
    )
    if not isinstance(value, dict):
        raise SnapshotError("cargo metadata must return an object")
    return value


def workspace_library_targets(
    *,
    scope: str = "core-release",
    metadata: dict[str, Any] | None = None,
) -> list[tuple[str, str]]:
    metadata = metadata or workspace_metadata()
    members = set(metadata["workspace_members"])
    scope_document = core_release_scope.load_scope()
    core_names = {entry["name"] for entry in core_release_scope.core_packages(scope_document)}
    targets: list[tuple[str, str]] = []
    for package in metadata["packages"]:
        if package["id"] not in members:
            continue
        if scope == "core-release" and package["name"] not in core_names:
            continue
        library_targets = [
            target
            for target in package["targets"]
            if LIB_KINDS.intersection(target["kind"])
        ]
        if len(library_targets) > 1:
            raise SnapshotError(f"{package['name']} has multiple library targets")
        if library_targets:
            targets.append((package["name"], library_targets[0]["name"]))
    return sorted(targets)


def workspace_package_features(metadata: dict[str, Any]) -> dict[str, dict[str, list[str]]]:
    members = set(metadata["workspace_members"])
    result: dict[str, dict[str, list[str]]] = {}
    for package in metadata["packages"]:
        if package["id"] not in members:
            continue
        raw_features = package.get("features", {})
        if not isinstance(raw_features, dict):
            raise SnapshotError(f"{package['name']} features must be an object")
        result[package["name"]] = {
            name: sorted(values)
            for name, values in raw_features.items()
            if isinstance(name, str) and isinstance(values, list)
        }
    return result


def _matrix_profile(
    entry: Any,
    target_by_package: dict[str, str],
    package_features: dict[str, dict[str, list[str]]],
) -> dict[str, Any]:
    command = list(entry.command)
    package_flag = "-p" if "-p" in command else "--package" if "--package" in command else None
    if package_flag is None:
        raise SnapshotError(f"feature matrix entry {entry.id} has no package selector")
    try:
        package = command[command.index(package_flag) + 1]
    except IndexError as error:
        raise SnapshotError(f"feature matrix entry {entry.id} has an empty package selector") from error
    if package not in target_by_package:
        raise SnapshotError(f"feature matrix entry {entry.id} targets non-core library {package}")

    features: list[str] = []
    if "--features" in command:
        try:
            raw_features = command[command.index("--features") + 1]
        except IndexError as error:
            raise SnapshotError(f"feature matrix entry {entry.id} has an empty --features") from error
        features = sorted({feature.strip() for feature in raw_features.split(",") if feature.strip()})
    declared = package_features.get(package, {})
    unknown = sorted(set(features) - set(declared))
    if unknown:
        raise SnapshotError(
            f"feature matrix entry {entry.id} uses undeclared {package} features: {', '.join(unknown)}"
        )
    all_features = "--all-features" in command
    if all_features and features:
        raise SnapshotError(f"feature matrix entry {entry.id} mixes --all-features and --features")
    return {
        "id": entry.id,
        "package": package,
        "target": target_by_package[package],
        "default_features": "--no-default-features" not in command,
        "all_features": all_features,
        "features": features,
        "declared_default_features": sorted(declared.get("default", [])),
        "source": "m09-public-feature-matrix",
        "matrix_ids": [entry.id],
    }


def derive_feature_profiles(
    targets: list[tuple[str, str]],
    package_features: dict[str, dict[str, list[str]]],
    matrix_entries: Iterable[Any] = m09_compatibility_matrix.MATRIX,
) -> list[dict[str, Any]]:
    """Return each library default plus every frozen M09 public feature profile."""

    target_by_package = dict(targets)
    profiles: dict[str, dict[str, Any]] = {}
    for package, target in targets:
        profile_id = f"{package}:default"
        profiles[profile_id] = {
            "id": profile_id,
            "package": package,
            "target": target,
            "default_features": True,
            "all_features": False,
            "features": [],
            "declared_default_features": sorted(package_features.get(package, {}).get("default", [])),
            "source": "workspace-default",
            "matrix_ids": [],
        }

    for entry in matrix_entries:
        if entry.group != "feature":
            continue
        if entry.id in profiles:
            raise SnapshotError(f"duplicate public API profile id: {entry.id}")
        profiles[entry.id] = _matrix_profile(entry, target_by_package, package_features)
    return sorted(
        profiles.values(),
        key=lambda profile: (
            profile["package"],
            profile["source"] != "workspace-default",
            profile["id"],
        ),
    )


def toolchain() -> dict[str, str]:
    return {
        "rustc": run(["rustc", f"+{RUSTDOC_TOOLCHAIN}", "--version"]),
        "rustdoc": run(["rustdoc", f"+{RUSTDOC_TOOLCHAIN}", "--version"]),
        "cargo": run(["cargo", f"+{RUSTDOC_TOOLCHAIN}", "--version"]),
    }


def _item_by_id(index: dict[str, Any], item_id: Any) -> dict[str, Any] | None:
    item = index.get(str(item_id))
    return item if isinstance(item, dict) else None


def _semantic_value(value: Any, index: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in sorted(value.items()):
            if key in VOLATILE_RUSTDOC_KEYS:
                continue
            if (
                key == "tuple"
                and isinstance(item, list)
                and item
                and all(_item_by_id(index, item_id) is not None for item_id in item)
            ):
                result["tuple_arity"] = len(item)
                continue
            result[key] = _semantic_value(item, index)
        return result
    if isinstance(value, list):
        return [_semantic_value(item, index) for item in value]
    return value


def _item_kind_and_value(item: dict[str, Any]) -> tuple[str, Any]:
    inner = item.get("inner")
    if not isinstance(inner, dict) or len(inner) != 1:
        return "unknown", inner
    return next(iter(inner.items()))


def _item_feature(item: dict[str, Any], fallback: str = "default") -> str:
    attributes = item.get("attrs", [])
    features = sorted(
        {
            feature
            for attribute in attributes
            if isinstance(attribute, str)
            for feature in FEATURE_RE.findall(attribute)
        }
    )
    return ",".join(features) if features else fallback


def _reference_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _struct_field_ids(value: Any) -> list[Any]:
    if not isinstance(value, dict):
        return []
    kind = value.get("kind")
    if not isinstance(kind, dict):
        return []
    plain = kind.get("plain")
    if isinstance(plain, dict):
        return _reference_list(plain.get("fields"))
    tuple_fields = kind.get("tuple")
    if isinstance(tuple_fields, list):
        return [item_id for item_id in tuple_fields if item_id is not None]
    return []


def _variant_field_ids(value: Any) -> list[Any]:
    if not isinstance(value, dict):
        return []
    kind = value.get("kind")
    if not isinstance(kind, dict):
        return []
    tuple_fields = kind.get("tuple")
    if isinstance(tuple_fields, list):
        return [item_id for item_id in tuple_fields if item_id is not None]
    struct_fields = kind.get("struct")
    if isinstance(struct_fields, dict):
        return _reference_list(struct_fields.get("fields"))
    return []


def _direct_associated_ids(kind: str, value: Any) -> list[Any]:
    if kind == "struct":
        return _struct_field_ids(value)
    if kind == "union" and isinstance(value, dict):
        return _reference_list(value.get("fields"))
    if kind == "enum" and isinstance(value, dict):
        return _reference_list(value.get("variants"))
    if kind == "variant":
        return _variant_field_ids(value)
    if kind == "trait" and isinstance(value, dict):
        return _reference_list(value.get("items"))
    return []


def _inherent_associated_ids(kind: str, value: Any, index: dict[str, Any]) -> list[Any]:
    if kind not in {"struct", "enum", "union", "type_alias"} or not isinstance(value, dict):
        return []
    associated: list[Any] = []
    for impl_id in _reference_list(value.get("impls")):
        impl_item = _item_by_id(index, impl_id)
        if impl_item is None:
            continue
        impl_kind, impl_value = _item_kind_and_value(impl_item)
        if impl_kind != "impl" or not isinstance(impl_value, dict):
            continue
        if impl_value.get("trait") is not None or impl_value.get("is_synthetic"):
            continue
        associated.extend(_reference_list(impl_value.get("items")))
    return associated


def _is_public_associated(parent_kind: str, item: dict[str, Any]) -> bool:
    visibility = item.get("visibility", "default")
    if parent_kind in {"enum", "variant", "trait"}:
        return visibility in {"public", "default"}
    return visibility == "public"


def _record(
    package: str,
    item_path: str,
    item: dict[str, Any],
    index: dict[str, Any],
    *,
    fallback_feature: str = "default",
) -> dict[str, str]:
    kind, kind_value = _item_kind_and_value(item)
    visibility = item.get("visibility", "public")
    if not isinstance(visibility, str):
        visibility = json.dumps(visibility, sort_keys=True, separators=(",", ":"))
    return {
        "package": package,
        "module": item_path.rsplit("::", 1)[0] if "::" in item_path else "",
        "item_path": item_path,
        "kind": kind,
        "visibility": visibility,
        "signature": json.dumps(
            _semantic_value(kind_value, index),
            sort_keys=True,
            separators=(",", ":"),
        ),
        "feature": _item_feature(item, fallback_feature),
    }


def _normalized_reexport_selection(selected_reexport_paths: Iterable[str]) -> set[str]:
    selected = list(selected_reexport_paths)
    if any(not isinstance(item_path, str) or not item_path for item_path in selected):
        raise SnapshotError("selected re-export paths must be non-empty strings")
    if any("*" in item_path for item_path in selected):
        raise SnapshotError("selected re-export paths must be exact paths, not wildcards")
    if len(selected) != len(set(selected)):
        raise SnapshotError("selected re-export paths must not contain duplicates")
    return set(selected)


def _public_reexport_record(
    package: str,
    item_path: str,
    use_item: dict[str, Any],
    *,
    alias_kind: str,
    target_path: str,
    target_kind: str,
    target_signature: Any,
) -> dict[str, str]:
    return {
        "package": package,
        "module": item_path.rsplit("::", 1)[0] if "::" in item_path else "",
        "item_path": item_path,
        "kind": "reexport",
        "visibility": "public",
        "signature": json.dumps(
            {
                "alias_kind": alias_kind,
                "target_kind": target_kind,
                "target_path": target_path,
                "target_signature": target_signature,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        "feature": _item_feature(use_item),
    }


def _rustdoc_paths(document: dict[str, Any]) -> dict[str, tuple[str, str]]:
    paths = document.get("paths", {})
    if not isinstance(paths, dict):
        return {}
    result: dict[str, tuple[str, str]] = {}
    for item_id, path_record in paths.items():
        if not isinstance(path_record, dict):
            continue
        path = path_record.get("path")
        kind = path_record.get("kind")
        if (
            not isinstance(path, list)
            or not path
            or any(not isinstance(part, str) or not part for part in path)
            or not isinstance(kind, str)
            or not kind
        ):
            continue
        result[str(item_id)] = ("::".join(path), kind)
    return result


@dataclass(frozen=True)
class _ReexportTarget:
    """The canonical rustdoc target behind a public export binding."""

    path: str
    kind: str
    item_id: str
    item: dict[str, Any] | None

    def identity(self) -> tuple[str, str, str]:
        return self.path, self.kind, self.item_id


@dataclass(frozen=True)
class _ReexportBinding:
    """A named public binding within a semantic rustdoc module."""

    target: _ReexportTarget
    alias_kind: str
    source_item: dict[str, Any]
    origin: str


def _collect_selected_reexports(
    package: str,
    document: dict[str, Any],
    records: dict[str, dict[str, str]],
    selected_reexport_paths: set[str],
) -> None:
    """Collect selected public re-export aliases from rustdoc's semantic module graph."""

    index = document.get("index", {})
    if not isinstance(index, dict):
        raise SnapshotError(f"rustdoc index for {package} must be an object")
    paths = _rustdoc_paths(document)
    root_id = str(document.get("root", ""))
    root = _item_by_id(index, root_id)
    root_path = paths.get(root_id, ("", ""))[0]
    if root is None or not root_path:
        raise SnapshotError(f"rustdoc root for selected re-exports is missing for {package}")
    root_kind, root_value = _item_kind_and_value(root)
    if root_kind != "module" or not isinstance(root_value, dict):
        raise SnapshotError(f"rustdoc root for selected re-exports is not a module for {package}")

    def resolve_target(item_id: Any) -> _ReexportTarget | None:
        current_id = str(item_id)
        visited: set[str] = set()
        while current_id not in visited:
            visited.add(current_id)
            target_item = _item_by_id(index, current_id)
            target_path = paths.get(current_id)
            if target_item is not None:
                target_kind, target_value = _item_kind_and_value(target_item)
                if target_kind == "use" and isinstance(target_value, dict):
                    nested_id = target_value.get("id")
                    if nested_id is None:
                        return None
                    current_id = str(nested_id)
                    continue
                if target_path is not None:
                    return _ReexportTarget(
                        path=target_path[0],
                        kind=target_path[1],
                        item_id=current_id,
                        item=target_item,
                    )
                return None
            if target_path is not None:
                return _ReexportTarget(
                    path=target_path[0],
                    kind=target_path[1],
                    item_id=current_id,
                    item=None,
                )
            return None
        return None

    def binding_sort_key(binding: _ReexportBinding) -> tuple[str, str, str, str]:
        return (
            binding.target.path,
            binding.target.kind,
            binding.alias_kind,
            _item_feature(binding.source_item),
        )

    def add_explicit_binding(
        bindings: dict[str, _ReexportBinding],
        name: str,
        binding: _ReexportBinding,
        module_path: str,
    ) -> None:
        previous = bindings.get(name)
        if previous is None:
            bindings[name] = binding
            return
        if previous.target.identity() != binding.target.identity():
            raise SnapshotError(f"conflicting explicit public bindings for {module_path}::{name}")
        if binding_sort_key(binding) < binding_sort_key(previous):
            bindings[name] = binding

    def public_bindings(
        module_id: Any,
        module_stack: set[str],
    ) -> dict[str, _ReexportBinding]:
        module_key = str(module_id)
        if module_key in module_stack:
            return {}
        module = _item_by_id(index, module_key)
        if module is None:
            return {}
        module_kind, module_value = _item_kind_and_value(module)
        if module_kind != "module" or not isinstance(module_value, dict):
            return {}
        module_path = paths.get(module_key, (f"<module:{module_key}>", "module"))[0]
        next_stack = {*module_stack, module_key}
        explicit: dict[str, _ReexportBinding] = {}
        glob_sources: list[tuple[_ReexportTarget, dict[str, Any]]] = []
        for child_id in _reference_list(module_value.get("items")):
            child = _item_by_id(index, child_id)
            if child is None or child.get("visibility") != "public":
                continue
            child_kind, child_value = _item_kind_and_value(child)
            if child_kind == "use" and isinstance(child_value, dict):
                target_id = child_value.get("id")
                if target_id is None:
                    continue
                if child_value.get("is_glob"):
                    target = resolve_target(target_id)
                    if target is not None and target.kind == "module":
                        glob_sources.append((target, child))
                    continue
                name = child_value.get("name")
                target = resolve_target(target_id)
                if isinstance(name, str) and name and target is not None:
                    canonical_name = target.path.rsplit("::", 1)[-1]
                    add_explicit_binding(
                        explicit,
                        name,
                        _ReexportBinding(
                            target=target,
                            alias_kind="plain" if name == canonical_name else "renamed",
                            source_item=child,
                            origin="named",
                        ),
                        module_path,
                    )
                continue
            name = child.get("name")
            target = resolve_target(child_id)
            if isinstance(name, str) and name and target is not None:
                add_explicit_binding(
                    explicit,
                    name,
                    _ReexportBinding(
                        target=target,
                        alias_kind="direct",
                        source_item=child,
                        origin="direct",
                    ),
                    module_path,
                )

        globbed: dict[str, list[_ReexportBinding]] = {}
        for target, source_item in glob_sources:
            for name, binding in public_bindings(target.item_id, next_stack).items():
                if name in explicit:
                    continue
                globbed.setdefault(name, []).append(
                    _ReexportBinding(
                        target=binding.target,
                        alias_kind="glob",
                        source_item=source_item,
                        origin="glob",
                    )
                )
        resolved = dict(explicit)
        for name, bindings in globbed.items():
            identities = {binding.target.identity() for binding in bindings}
            if len(identities) != 1:
                raise SnapshotError(f"ambiguous public glob bindings for {module_path}::{name}")
            resolved[name] = min(bindings, key=binding_sort_key)
        return {name: resolved[name] for name in sorted(resolved)}

    def targets_at_path(canonical_path: str) -> list[_ReexportTarget]:
        targets = [
            _ReexportTarget(
                path=path,
                kind=kind,
                item_id=item_id,
                item=_item_by_id(index, item_id),
            )
            for item_id, (path, kind) in paths.items()
            if path == canonical_path
        ]
        return sorted(targets, key=lambda target: (target.kind, target.item_id))

    def unique_target_at_path(
        canonical_path: str,
        *,
        context: str,
    ) -> _ReexportTarget | None:
        targets = targets_at_path(canonical_path)
        if len(targets) > 1:
            raise SnapshotError(f"ambiguous rustdoc targets for {context}: {canonical_path}")
        return targets[0] if targets else None

    def public_external_glob_sources(module_id: Any) -> list[tuple[_ReexportTarget, dict[str, Any]]]:
        module = _item_by_id(index, str(module_id))
        if module is None:
            return []
        module_kind, module_value = _item_kind_and_value(module)
        if module_kind != "module" or not isinstance(module_value, dict):
            return []
        result: list[tuple[_ReexportTarget, dict[str, Any]]] = []
        for child_id in _reference_list(module_value.get("items")):
            child = _item_by_id(index, child_id)
            if child is None or child.get("visibility") != "public":
                continue
            child_kind, child_value = _item_kind_and_value(child)
            if child_kind != "use" or not isinstance(child_value, dict) or not child_value.get("is_glob"):
                continue
            target_id = child_value.get("id")
            if target_id is None:
                continue
            target = resolve_target(target_id)
            if target is not None and target.kind == "module" and target.item is None:
                result.append((target, child))
        return result

    matched: set[str] = set()

    def add_associated(
        target: _ReexportTarget,
        alias_path: str,
        alias_kind: str,
        source_item: dict[str, Any],
    ) -> None:
        if target.item is None:
            return
        _, target_value = _item_kind_and_value(target.item)
        direct_ids = _direct_associated_ids(target.kind, target_value)
        inherent_ids = _inherent_associated_ids(target.kind, target_value, index)
        for position, child_id in enumerate((*direct_ids, *inherent_ids)):
            child = _item_by_id(index, child_id)
            if child is None:
                continue
            relationship_kind = target.kind if position < len(direct_ids) else "impl"
            if not _is_public_associated(relationship_kind, child):
                continue
            child_name = child.get("name")
            if not isinstance(child_name, str) or not child_name:
                continue
            child_alias_path = f"{alias_path}::{child_name}"
            if child_alias_path not in selected_reexport_paths:
                continue
            child_kind, _ = _item_kind_and_value(child)
            child_target_path = paths.get(str(child_id), (f"{target.path}::{child_name}", child_kind))[0]
            matched.add(child_alias_path)
            records.setdefault(
                child_alias_path,
                _public_reexport_record(
                    package,
                    child_alias_path,
                    source_item,
                    alias_kind=f"{alias_kind}-associated",
                    target_path=child_target_path,
                    target_kind=child_kind,
                    target_signature=_semantic_value(_item_kind_and_value(child)[1], index),
                ),
            )

    def add_target(
        alias_path: str,
        alias_kind: str,
        source_item: dict[str, Any],
        target: _ReexportTarget,
    ) -> None:
        if alias_path in selected_reexport_paths:
            target_signature = None
            if target.item is not None:
                target_signature = _semantic_value(_item_kind_and_value(target.item)[1], index)
            matched.add(alias_path)
            records.setdefault(
                alias_path,
                _public_reexport_record(
                    package,
                    alias_path,
                    source_item,
                    alias_kind=alias_kind,
                    target_path=target.path,
                    target_kind=target.kind,
                    target_signature=target_signature,
                ),
            )
        add_associated(target, alias_path, alias_kind, source_item)

    def add_external_module_descendants(
        target: _ReexportTarget,
        alias_path: str,
        alias_kind: str,
        source_item: dict[str, Any],
    ) -> None:
        """Map exact selected descendants of an external module through its public alias."""

        prefix = f"{alias_path}::"
        for selected_path in sorted(path for path in selected_reexport_paths if path.startswith(prefix)):
            suffix = selected_path.removeprefix(prefix)
            descendant = unique_target_at_path(
                f"{target.path}::{suffix}",
                context=selected_path,
            )
            if descendant is not None:
                add_target(
                    selected_path,
                    f"{alias_kind}-external-descendant",
                    source_item,
                    descendant,
                )

    def add_external_glob_descendants(
        module_id: Any,
        public_path: str,
        bindings: dict[str, _ReexportBinding],
    ) -> None:
        """Resolve selected descendants of external glob modules from rustdoc path entries only."""

        sources = public_external_glob_sources(module_id)
        if not sources:
            return
        module_path = paths.get(str(module_id), (f"<module:{module_id}>", "module"))[0]
        prefix = f"{public_path}::"
        selected_by_name: dict[str, list[tuple[str, str]]] = {}
        for selected_path in selected_reexport_paths:
            if not selected_path.startswith(prefix):
                continue
            suffix = selected_path.removeprefix(prefix)
            name, _, _ = suffix.partition("::")
            if name and name not in bindings:
                selected_by_name.setdefault(name, []).append((selected_path, suffix))
        for name, selected_paths in sorted(selected_by_name.items()):
            candidates: list[tuple[_ReexportTarget, _ReexportTarget, dict[str, Any]]] = []
            for source_target, source_item in sources:
                target = unique_target_at_path(
                    f"{source_target.path}::{name}",
                    context=f"{module_path}::{name}",
                )
                if target is not None:
                    candidates.append((target, source_target, source_item))
            identities = {target.identity() for target, _, _ in candidates}
            if len(identities) > 1:
                raise SnapshotError(f"ambiguous public glob bindings for {module_path}::{name}")
            if not candidates:
                continue
            _, source_target, source_item = min(candidates, key=lambda entry: entry[1].item_id)
            for selected_path, suffix in sorted(selected_paths):
                descendant = unique_target_at_path(
                    f"{source_target.path}::{suffix}",
                    context=selected_path,
                )
                if descendant is None:
                    continue
                add_target(
                    selected_path,
                    "glob-external-descendant",
                    source_item,
                    descendant,
                )

    def walk_public_alias(
        module_id: Any,
        public_path: str,
        module_stack: set[str],
        through_module_alias: bool,
    ) -> None:
        module_key = str(module_id)
        if module_key in module_stack:
            return
        bindings = public_bindings(module_key, module_stack)
        for name, binding in bindings.items():
            alias_path = f"{public_path}::{name}"
            alias_kind = binding.alias_kind
            if binding.origin == "direct" and through_module_alias:
                alias_kind = "module-descendant"
            add_target(alias_path, alias_kind, binding.source_item, binding.target)
            if binding.target.kind == "module" and binding.target.item is not None:
                walk_public_alias(
                    binding.target.item_id,
                    alias_path,
                    {*module_stack, module_key},
                    through_module_alias or binding.origin != "direct",
                )
            elif binding.target.kind == "module":
                add_external_module_descendants(
                    binding.target,
                    alias_path,
                    alias_kind,
                    binding.source_item,
                )
        add_external_glob_descendants(module_key, public_path, bindings)

    walk_public_alias(root_id, root_path, set(), False)
    missing = sorted(selected_reexport_paths - matched)
    if missing:
        raise SnapshotError(
            f"selected public re-export paths were not found for {package}: {', '.join(missing)}"
        )


def semantic_public_items(
    package: str,
    document: dict[str, Any],
    *,
    selected_reexport_paths: Iterable[str] = (),
) -> list[dict[str, str]]:
    """Build stable, readable API records from rustdoc's public API graph."""

    index = document.get("index", {})
    path_by_id = {
        str(item_id): "::".join(path_record["path"])
        for item_id, path_record in document.get("paths", {}).items()
        if path_record.get("crate_id") == 0
        and isinstance(path_record.get("path"), list)
        and path_record["path"]
        and all(isinstance(part, str) for part in path_record["path"])
    }
    records: dict[str, dict[str, str]] = {}

    def add_associated(parent_id: Any, parent_path: str, parent_feature: str) -> None:
        parent = _item_by_id(index, parent_id)
        if parent is None:
            return
        parent_kind, parent_value = _item_kind_and_value(parent)
        direct_ids = _direct_associated_ids(parent_kind, parent_value)
        inherent_ids = _inherent_associated_ids(parent_kind, parent_value, index)
        for position, child_id in enumerate((*direct_ids, *inherent_ids)):
            child = _item_by_id(index, child_id)
            if child is None:
                continue
            relationship_kind = parent_kind if position < len(direct_ids) else "impl"
            if not _is_public_associated(relationship_kind, child):
                continue
            name = child.get("name")
            if not isinstance(name, str) or not name:
                name = str(position)
            child_path = path_by_id.get(str(child_id), f"{parent_path}::{name}")
            records[child_path] = _record(
                package,
                child_path,
                child,
                index,
                fallback_feature=parent_feature,
            )
            child_kind, _ = _item_kind_and_value(child)
            if child_kind == "variant":
                add_associated(child_id, child_path, _item_feature(child, parent_feature))

    for item_id, path_record in document.get("paths", {}).items():
        if path_record.get("crate_id") != 0:
            continue
        path = path_record.get("path")
        if not isinstance(path, list) or not path or any(not isinstance(part, str) for part in path):
            raise SnapshotError(f"invalid public path for {package}: {path!r}")
        item = _item_by_id(index, item_id)
        if item is None:
            continue
        item_path = "::".join(path)
        feature = _item_feature(item)
        records[item_path] = _record(package, item_path, item, index)
        add_associated(item_id, item_path, feature)

    root_id = document.get("root")
    root = _item_by_id(index, root_id)
    if root is not None:
        root_kind, root_value = _item_kind_and_value(root)
        root_path = path_by_id.get(str(root_id))
        if root_path is None:
            root_name = root.get("name")
            root_path = root_name if isinstance(root_name, str) and root_name else None

        visited_modules: set[str] = set()

        def add_public_proc_macros(module_id: Any, module_path: str, module_value: Any) -> None:
            module_key = str(module_id)
            if module_key in visited_modules or not isinstance(module_value, dict):
                return
            visited_modules.add(module_key)
            for child_id in _reference_list(module_value.get("items")):
                child = _item_by_id(index, child_id)
                if child is None:
                    continue
                child_kind, child_value = _item_kind_and_value(child)
                child_name = child.get("name")
                if not isinstance(child_name, str) or not child_name:
                    continue
                child_path = path_by_id.get(str(child_id), f"{module_path}::{child_name}")
                if child_kind == "proc_macro" and child.get("visibility") == "public":
                    records[child_path] = _record(package, child_path, child, index)
                elif child_kind == "module" and child.get("visibility") == "public":
                    add_public_proc_macros(child_id, child_path, child_value)

        if root_kind == "module" and root_path is not None:
            add_public_proc_macros(root_id, root_path, root_value)
    selected = _normalized_reexport_selection(selected_reexport_paths)
    if selected:
        _collect_selected_reexports(package, document, records, selected)
    return sorted(
        records.values(),
        key=lambda item: tuple(item[field] for field in STRUCTURAL_ITEM_FIELDS),
    )


def _profile_cache_path(profile_id: str) -> Path:
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", profile_id)
    return ROOT / "target" / "public-api-snapshot" / f"{safe_name}.json"


def _rustdoc_command(profile: dict[str, Any]) -> list[str]:
    command = [
        "cargo",
        f"+{RUSTDOC_TOOLCHAIN}",
        "rustdoc",
        "--locked",
        "-p",
        profile["package"],
        "--lib",
    ]
    if profile["all_features"]:
        command.append("--all-features")
    else:
        if not profile["default_features"]:
            command.append("--no-default-features")
        if profile["features"]:
            command.extend(("--features", ",".join(profile["features"])))
    command.extend(("--", "-Z", "unstable-options", "--output-format", "json"))
    return command


def snapshot_profile(
    profile: dict[str, Any],
    *,
    refresh: bool = True,
    selected_reexport_paths: Iterable[str] = (),
) -> dict[str, Any]:
    cache_path = _profile_cache_path(profile["id"])
    if refresh:
        run(_rustdoc_command(profile))
        rustdoc_json = ROOT / "target" / "doc" / f"{profile['target']}.json"
        if not rustdoc_json.is_file():
            raise SnapshotError(
                f"rustdoc JSON was not produced for {profile['id']}: {rustdoc_json}"
            )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(rustdoc_json, cache_path)
    if not cache_path.is_file():
        raise SnapshotError(f"cached rustdoc JSON is missing for {profile['id']}: {cache_path}")
    document = json.loads(cache_path.read_text(encoding="utf-8"))
    result = {field: profile[field] for field in PROFILE_SPEC_FIELDS}
    result.update(
        {
            "crate_version": document.get("crate_version"),
            "public_api": semantic_public_items(
                profile["package"],
                document,
                selected_reexport_paths=selected_reexport_paths,
            ),
        }
    )
    return result


def validate_existing_artifacts(profiles: list[dict[str, Any]]) -> None:
    dirty_api_inputs = run(
        [
            "git",
            "status",
            "--porcelain",
            "--",
            ":(glob)**/*.rs",
            ":(glob)**/Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
        ]
    )
    if dirty_api_inputs:
        raise SnapshotError(
            "--from-existing cannot verify dirty Rust/API inputs; refresh after committing or stashing them"
        )
    commit_timestamp = int(run(["git", "show", "-s", "--format=%ct", "HEAD"]))
    missing: list[str] = []
    stale: list[str] = []
    for profile in profiles:
        cache_path = _profile_cache_path(profile["id"])
        if not cache_path.is_file():
            missing.append(profile["id"])
        elif cache_path.stat().st_mtime < commit_timestamp:
            stale.append(profile["id"])
    if missing or stale:
        raise SnapshotError(
            "--from-existing requires profile artifacts refreshed after HEAD; "
            f"missing={','.join(missing) or '-'} stale={','.join(stale) or '-'}"
        )


def load_freeze_policy(path: Path = DEFAULT_FREEZE_POLICY) -> dict[str, Any]:
    try:
        policy = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SnapshotError(f"cannot read public API freeze policy {path}: {error}") from error
    if not isinstance(policy, dict) or policy.get("schema_version") != 1:
        raise SnapshotError("public API freeze policy schema_version must be 1")
    return policy


def load_reexport_surface_inventory(
    profiles: Iterable[dict[str, Any]],
    path: Path = DEFAULT_REEXPORT_SURFACE_INVENTORY,
) -> dict[str, tuple[str, ...]]:
    """Load exact re-export paths selected for compatibility-surface tracking."""

    try:
        inventory = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SnapshotError(f"cannot read public API re-export inventory {path}: {error}") from error
    if not isinstance(inventory, dict) or set(inventory) != {"schema_version", "profiles"}:
        raise SnapshotError("public API re-export inventory must contain only schema_version and profiles")
    if inventory.get("schema_version") != REEXPORT_SURFACE_INVENTORY_SCHEMA_VERSION:
        raise SnapshotError(
            "public API re-export inventory schema_version must be "
            f"{REEXPORT_SURFACE_INVENTORY_SCHEMA_VERSION}"
        )
    selections = inventory.get("profiles")
    if not isinstance(selections, dict):
        raise SnapshotError("public API re-export inventory profiles must be an object")
    known_profiles = {profile.get("id"): profile for profile in profiles}
    result: dict[str, tuple[str, ...]] = {}
    for profile_id, selection in selections.items():
        if not isinstance(profile_id, str) or profile_id not in known_profiles:
            raise SnapshotError(f"public API re-export inventory has unknown profile: {profile_id!r}")
        if not isinstance(selection, dict) or set(selection) != {"package", "item_paths"}:
            raise SnapshotError(
                f"public API re-export inventory selection {profile_id} must contain only package and item_paths"
            )
        profile = known_profiles[profile_id]
        if selection.get("package") != profile.get("package"):
            raise SnapshotError(
                f"public API re-export inventory selection {profile_id} has the wrong package"
            )
        item_paths = selection.get("item_paths")
        if not isinstance(item_paths, list) or not item_paths:
            raise SnapshotError(
                f"public API re-export inventory selection {profile_id} must have non-empty item_paths"
            )
        normalized = _normalized_reexport_selection(item_paths)
        target = profile.get("target")
        if not isinstance(target, str) or any(
            not item_path.startswith(f"{target}::") for item_path in normalized
        ):
            raise SnapshotError(
                f"public API re-export inventory selection {profile_id} has an item path outside its target"
            )
        result[profile_id] = tuple(sorted(normalized))
    return result


def generate_snapshot(
    *,
    refresh: bool = True,
    scope: str = "core-release",
    freeze_policy_path: Path = DEFAULT_FREEZE_POLICY,
    reexport_surface_inventory_path: Path = DEFAULT_REEXPORT_SURFACE_INVENTORY,
) -> dict[str, Any]:
    metadata = workspace_metadata()
    targets = workspace_library_targets(scope=scope, metadata=metadata)
    profiles = derive_feature_profiles(
        targets,
        workspace_package_features(metadata),
        m09_compatibility_matrix.MATRIX,
    )
    if not refresh:
        validate_existing_artifacts(profiles)
    reexport_selections = load_reexport_surface_inventory(
        profiles,
        reexport_surface_inventory_path,
    )
    profile_snapshots: dict[str, dict[str, Any]] = {}
    for index, profile in enumerate(profiles, start=1):
        print(
            f"PUBLIC_API_SNAPSHOT_PROFILE {index}/{len(profiles)} {profile['id']}",
            flush=True,
        )
        profile_snapshots[profile["id"]] = snapshot_profile(
            profile,
            refresh=refresh,
            selected_reexport_paths=reexport_selections.get(profile["id"], ()),
        )

    packages: dict[str, dict[str, Any]] = {}
    for package, target in targets:
        packages[package] = {
            "target": target,
            "profile_ids": sorted(
                profile_id
                for profile_id, profile in profile_snapshots.items()
                if profile["package"] == package
            ),
        }
    intent = json.loads(PUBLIC_API_INTENT.read_text(encoding="utf-8"))
    intent_counts = {
        package: {
            category: sum(entry["category"] == category for entry in spec["entries"])
            for category in ("stable", "experimental", "compat")
        }
        for package, spec in intent["crates"].items()
        if package in packages
    }
    freeze_policy = load_freeze_policy(freeze_policy_path)
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "scope": scope,
        "freeze": freeze_policy["freeze"],
        "toolchain": toolchain(),
        "packages": packages,
        "profiles": profile_snapshots,
        "public_api_intent": intent_counts,
        "compatibility_decisions": freeze_policy["compatibility_decisions"],
        "frozen_contracts": freeze_policy["frozen_contracts"],
    }


def _require_non_empty_string(value: Any, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise SnapshotError(f"{label} must be non-empty")


def _validate_decisions(value: Any) -> None:
    if not isinstance(value, list):
        raise SnapshotError("compatibility_decisions must be a list")
    ids: set[str] = set()
    for index, decision in enumerate(value):
        if not isinstance(decision, dict):
            raise SnapshotError(f"compatibility_decisions[{index}] must be an object")
        for field in ("id", "classification", "applies_to", "reason"):
            _require_non_empty_string(decision.get(field), f"compatibility_decisions[{index}].{field}")
        decision_id = decision["id"]
        if decision_id in ids:
            raise SnapshotError(f"duplicate compatibility decision: {decision_id}")
        ids.add(decision_id)
        classification = decision["classification"]
        if classification not in COMPATIBILITY_CLASSIFICATIONS:
            raise SnapshotError(
                f"compatibility_decisions[{index}].classification is invalid: {classification}"
            )
        if classification in BREAKING_CLASSIFICATIONS:
            _require_non_empty_string(
                decision.get("approved_by"),
                f"compatibility_decisions[{index}].approved_by",
            )
            _require_non_empty_string(
                decision.get("approved_on"),
                f"compatibility_decisions[{index}].approved_on",
            )
        if decision["applies_to"] == "post-freeze":
            for field in ("profile_id", "package", "item_path", "change", "replacement"):
                _require_non_empty_string(
                    decision.get(field),
                    f"compatibility_decisions[{index}].{field}",
                )


def _validate_profiles(baseline: dict[str, Any]) -> None:
    packages = baseline.get("packages")
    profiles = baseline.get("profiles")
    if not isinstance(packages, dict) or not packages:
        raise SnapshotError("packages must be a non-empty object")
    if not isinstance(profiles, dict) or not profiles:
        raise SnapshotError("profiles must be a non-empty object")
    referenced: set[str] = set()
    for package, package_record in packages.items():
        if not isinstance(package_record, dict):
            raise SnapshotError(f"packages.{package} must be an object")
        _require_non_empty_string(package_record.get("target"), f"packages.{package}.target")
        profile_ids = package_record.get("profile_ids")
        if not isinstance(profile_ids, list) or not profile_ids:
            raise SnapshotError(f"packages.{package}.profile_ids must be non-empty")
        if f"{package}:default" not in profile_ids:
            raise SnapshotError(f"packages.{package} is missing its default profile")
        for profile_id in profile_ids:
            _require_non_empty_string(profile_id, f"packages.{package}.profile_ids entry")
            if profile_id in referenced:
                raise SnapshotError(f"profile is referenced more than once: {profile_id}")
            referenced.add(profile_id)
            profile = profiles.get(profile_id)
            if not isinstance(profile, dict):
                raise SnapshotError(f"missing profile record: {profile_id}")
            if profile.get("package") != package:
                raise SnapshotError(f"profile {profile_id} package does not match {package}")
            for field in PROFILE_SPEC_FIELDS:
                if field not in profile:
                    raise SnapshotError(f"profile {profile_id} missing {field}")
            api = profile.get("public_api")
            if not isinstance(api, list) or not api:
                raise SnapshotError(f"profile {profile_id} public_api must be non-empty")
            item_paths: set[str] = set()
            for item_index, item in enumerate(api):
                if not isinstance(item, dict) or set(item) != set(STRUCTURAL_ITEM_FIELDS):
                    raise SnapshotError(
                        f"profile {profile_id} public_api[{item_index}] must contain the structural fields"
                    )
                for field in STRUCTURAL_ITEM_FIELDS:
                    if not isinstance(item[field], str):
                        raise SnapshotError(
                            f"profile {profile_id} public_api[{item_index}].{field} must be a string"
                        )
                if item["package"] != package:
                    raise SnapshotError(f"profile {profile_id} contains another package's item")
                if item["item_path"] in item_paths:
                    raise SnapshotError(
                        f"profile {profile_id} has duplicate item path: {item['item_path']}"
                    )
                item_paths.add(item["item_path"])
    if referenced != set(profiles):
        extras = sorted(set(profiles) - referenced)
        raise SnapshotError(f"unreferenced profiles: {', '.join(extras)}")


def _validate_frozen_contracts(baseline: dict[str, Any]) -> None:
    contracts = baseline.get("frozen_contracts")
    if not isinstance(contracts, list):
        raise SnapshotError("frozen_contracts must be a list")
    capabilities = [contract.get("capability_id") for contract in contracts if isinstance(contract, dict)]
    if set(capabilities) != REQUIRED_FROZEN_CAPABILITIES or len(capabilities) != len(REQUIRED_FROZEN_CAPABILITIES):
        raise SnapshotError("frozen_contracts must contain exactly F-13, F-15, and F-18")
    profiles = baseline["profiles"]
    for index, contract in enumerate(contracts):
        for field in ("capability_id", "profile_id", "package", "behavior"):
            _require_non_empty_string(contract.get(field), f"frozen_contracts[{index}].{field}")
        evidence = contract.get("evidence")
        if not isinstance(evidence, list) or not evidence:
            raise SnapshotError(f"frozen_contracts[{index}].evidence must be non-empty")
        profile = profiles.get(contract["profile_id"])
        if not isinstance(profile, dict) or profile.get("package") != contract["package"]:
            raise SnapshotError(f"frozen_contracts[{index}] references an invalid profile")
        item_paths = contract.get("item_paths")
        if not isinstance(item_paths, list) or not item_paths:
            raise SnapshotError(f"frozen_contracts[{index}].item_paths must be non-empty")
        available = {item["item_path"] for item in profile["public_api"]}
        missing = sorted(set(item_paths) - available)
        if missing:
            raise SnapshotError(
                f"frozen_contracts[{index}] item paths are absent from {contract['profile_id']}: {', '.join(missing)}"
            )


def _validate_reexport_surface_inventory_coverage(
    baseline: dict[str, Any],
    reexport_surface_inventory_path: Path,
) -> None:
    profiles = baseline["profiles"]
    profile_specs = [
        {
            "id": profile_id,
            "package": profile.get("package"),
            "target": profile.get("target"),
        }
        for profile_id, profile in profiles.items()
        if isinstance(profile, dict)
    ]
    selections = load_reexport_surface_inventory(profile_specs, reexport_surface_inventory_path)
    for profile_id, item_paths in selections.items():
        available = {item["item_path"] for item in profiles[profile_id]["public_api"]}
        missing = sorted(set(item_paths) - available)
        if missing:
            raise SnapshotError(
                f"public API re-export inventory paths are absent from {profile_id}: {', '.join(missing)}"
            )


def validate_baseline_contract(
    baseline: dict[str, Any],
    *,
    reexport_surface_inventory_path: Path | None = None,
) -> None:
    if baseline.get("schema_version") != SCHEMA_VERSION:
        raise SnapshotError(f"baseline schema_version must be {SCHEMA_VERSION}")
    if baseline.get("identity") != IDENTITY:
        raise SnapshotError(f"baseline identity must be {IDENTITY}")
    if baseline.get("scope") != "core-release":
        raise SnapshotError("baseline scope must be core-release")
    freeze = baseline.get("freeze")
    if not isinstance(freeze, dict):
        raise SnapshotError("freeze must be an object")
    if freeze.get("version") != "1.0.0-rc.1":
        raise SnapshotError("freeze.version must be 1.0.0-rc.1")
    if freeze.get("breaking_change_policy") != "approval-required-after-freeze":
        raise SnapshotError("freeze.breaking_change_policy must require approval after freeze")
    _validate_profiles(baseline)
    _validate_decisions(baseline.get("compatibility_decisions"))
    _validate_frozen_contracts(baseline)
    if reexport_surface_inventory_path is not None:
        _validate_reexport_surface_inventory_coverage(baseline, reexport_surface_inventory_path)


def _approval_for(
    baseline: dict[str, Any],
    *,
    profile_id: str,
    package: str,
    item_path: str,
    change: str,
) -> dict[str, Any] | None:
    for decision in baseline.get("compatibility_decisions", []):
        if (
            decision.get("applies_to") == "post-freeze"
            and decision.get("profile_id") == profile_id
            and decision.get("package") == package
            and decision.get("item_path") == item_path
            and decision.get("change") == change
            and decision.get("classification") == "approved-break"
            and isinstance(decision.get("approved_by"), str)
            and decision["approved_by"].strip()
        ):
            return decision
    return None


def _breaking_difference(
    baseline: dict[str, Any],
    difference: dict[str, Any],
    *,
    profile_id: str = "",
    package: str = "",
    item_path: str = "",
    change: str,
) -> dict[str, Any]:
    approval = _approval_for(
        baseline,
        profile_id=profile_id,
        package=package,
        item_path=item_path,
        change=change,
    )
    difference.update(
        {
            "classification": "approved-break" if approval else "breaking",
            "allowed": approval is not None,
        }
    )
    if approval:
        difference["decision_id"] = approval["id"]
    return difference


def _profile_items(profile: dict[str, Any]) -> dict[str, dict[str, str]]:
    return {item["item_path"]: item for item in profile.get("public_api", [])}


def compare_snapshots(baseline: dict[str, Any], candidate: dict[str, Any]) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []
    for field, expected in (
        ("schema_version", SCHEMA_VERSION),
        ("identity", IDENTITY),
        ("scope", "core-release"),
    ):
        if baseline.get(field) != expected:
            differences.append(
                {
                    "kind": field,
                    "expected": expected,
                    "actual": baseline.get(field),
                    "classification": "breaking",
                    "allowed": False,
                }
            )
        elif candidate.get(field) != baseline.get(field):
            differences.append(
                {
                    "kind": field,
                    "expected": baseline.get(field),
                    "actual": candidate.get(field),
                    "classification": "breaking",
                    "allowed": False,
                }
            )
    for field in ("freeze", "compatibility_decisions", "frozen_contracts"):
        if baseline.get(field) != candidate.get(field):
            differences.append(
                {
                    "kind": f"{field}-changed",
                    "classification": "breaking",
                    "allowed": False,
                }
            )

    baseline_packages = baseline.get("packages", {})
    candidate_packages = candidate.get("packages", {})
    removed_packages = set(baseline_packages) - set(candidate_packages)
    added_packages = set(candidate_packages) - set(baseline_packages)
    for package in sorted(removed_packages):
        differences.append(
            _breaking_difference(
                baseline,
                {"kind": "package-removed", "package": package},
                package=package,
                change="package-removed",
            )
        )
    for package in sorted(added_packages):
        differences.append(
            {
                "kind": "package-added",
                "package": package,
                "classification": "compatible-addition",
                "allowed": True,
            }
        )

    baseline_profiles = baseline.get("profiles", {})
    candidate_profiles = candidate.get("profiles", {})
    for profile_id in sorted(set(baseline_profiles) | set(candidate_profiles)):
        before = baseline_profiles.get(profile_id)
        after = candidate_profiles.get(profile_id)
        if before is None:
            differences.append(
                {
                    "kind": "profile-added",
                    "profile_id": profile_id,
                    "classification": "compatible-addition",
                    "allowed": True,
                }
            )
            continue
        if after is None:
            if before.get("package") in removed_packages:
                continue
            differences.append(
                _breaking_difference(
                    baseline,
                    {
                        "kind": "profile-removed",
                        "profile_id": profile_id,
                        "package": before.get("package"),
                    },
                    profile_id=profile_id,
                    package=str(before.get("package", "")),
                    change="profile-removed",
                )
            )
            continue
        spec_changes = [field for field in PROFILE_SPEC_FIELDS if before.get(field) != after.get(field)]
        if spec_changes:
            differences.append(
                _breaking_difference(
                    baseline,
                    {
                        "kind": "profile-changed",
                        "profile_id": profile_id,
                        "package": before.get("package"),
                        "changed_fields": spec_changes,
                    },
                    profile_id=profile_id,
                    package=str(before.get("package", "")),
                    change="profile-spec",
                )
            )
        before_items = _profile_items(before)
        after_items = _profile_items(after)
        package = str(before.get("package", ""))
        for item_path in sorted(set(before_items) - set(after_items)):
            differences.append(
                _breaking_difference(
                    baseline,
                    {
                        "kind": "item-removed",
                        "profile_id": profile_id,
                        "package": package,
                        "item": before_items[item_path],
                    },
                    profile_id=profile_id,
                    package=package,
                    item_path=item_path,
                    change="removed",
                )
            )
        for item_path in sorted(set(after_items) - set(before_items)):
            differences.append(
                {
                    "kind": "item-added",
                    "profile_id": profile_id,
                    "package": package,
                    "item": after_items[item_path],
                    "classification": "compatible-addition",
                    "allowed": True,
                }
            )
        for item_path in sorted(set(before_items) & set(after_items)):
            changed_fields = [
                field
                for field in STRUCTURAL_ITEM_FIELDS
                if before_items[item_path][field] != after_items[item_path][field]
            ]
            if not changed_fields:
                continue
            change = changed_fields[0] if len(changed_fields) == 1 else "structural"
            differences.append(
                _breaking_difference(
                    baseline,
                    {
                        "kind": "item-changed",
                        "profile_id": profile_id,
                        "package": package,
                        "item_path": item_path,
                        "changed_fields": changed_fields,
                        "before": before_items[item_path],
                        "after": after_items[item_path],
                    },
                    profile_id=profile_id,
                    package=package,
                    item_path=item_path,
                    change=change,
                )
            )
    return differences


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write-baseline", type=Path)
    mode.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scope", choices=("core-release",), default="core-release")
    parser.add_argument("--identity", choices=(IDENTITY,), default=IDENTITY)
    parser.add_argument("--freeze-policy", type=Path, default=DEFAULT_FREEZE_POLICY)
    parser.add_argument(
        "--reexport-surface-inventory",
        type=Path,
        default=DEFAULT_REEXPORT_SURFACE_INVENTORY,
    )
    parser.add_argument(
        "--from-existing",
        action="store_true",
        help="assemble from per-profile rustdoc JSON cached after the current HEAD",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        reexport_surface_inventory_path = args.reexport_surface_inventory.resolve()
        baseline: dict[str, Any] | None = None
        if args.check:
            baseline = json.loads(args.check.read_text(encoding="utf-8"))
            validate_baseline_contract(
                baseline,
                reexport_surface_inventory_path=reexport_surface_inventory_path,
            )
        candidate = generate_snapshot(
            refresh=not args.from_existing,
            scope=args.scope,
            freeze_policy_path=args.freeze_policy.resolve(),
            reexport_surface_inventory_path=reexport_surface_inventory_path,
        )
        validate_baseline_contract(
            candidate,
            reexport_surface_inventory_path=reexport_surface_inventory_path,
        )
        if args.write_baseline:
            write_json(args.write_baseline, candidate)
            print(
                f"PUBLIC_API_BASELINE_WRITTEN packages={len(candidate['packages'])} "
                f"profiles={len(candidate['profiles'])} path={args.write_baseline.as_posix()}"
            )
            return 0

        assert baseline is not None
        differences = compare_snapshots(baseline, candidate)
        incompatible = [difference for difference in differences if not difference.get("allowed", False)]
        report = {
            "schema_version": SCHEMA_VERSION,
            "identity": args.identity,
            "scope": args.scope,
            "packages": len(candidate["packages"]),
            "profiles": len(candidate["profiles"]),
            "differences": differences,
            "status": "compatible" if not incompatible else "review-required",
        }
        if args.output:
            write_json(args.output, report)
        if incompatible:
            print(json.dumps(report, indent=2, sort_keys=True))
            return 1
        print(
            f"PUBLIC_API_SNAPSHOT_OK packages={len(candidate['packages'])} "
            f"profiles={len(candidate['profiles'])} differences={len(differences)}"
        )
        return 0
    except (OSError, json.JSONDecodeError, SnapshotError) as error:
        print(f"PUBLIC_API_SNAPSHOT_FAILED {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
