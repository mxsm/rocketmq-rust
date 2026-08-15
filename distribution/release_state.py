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

"""Shared crash-safe primitives for local release-candidate state."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Iterator


SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
CANDIDATE_FIELDS = {
    "schema_version",
    "candidate_id",
    "candidate_kind",
    "version",
    "run_id",
    "attempt",
    "ordinal",
    "candidate_root",
    "series_manifest",
    "series_id",
    "series_generation",
    "parent_manifest",
    "state",
    "sealed",
    "outcome",
    "rejection_reason",
    "known_issues",
    "generation",
    "build_source_bundle",
    "source_snapshot",
    "artifact_index",
    "evidence_index",
    "event_index",
    "execution_context_index",
    "creation_operation_id",
    "created_at",
    "updated_at",
}
SERIES_FIELDS = {
    "schema_version",
    "release_line",
    "series_id",
    "generation",
    "next_ordinal",
    "next_rc_suffix",
    "head",
    "consecutive_successful_rcs",
    "entries",
    "pending_operation",
    "created_at",
    "updated_at",
}


class ReleaseStateError(ValueError):
    """Raised when candidate state is invalid or cannot be updated safely."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def require_safe_id(value: str, field: str) -> str:
    if not isinstance(value, str) or SAFE_ID.fullmatch(value) is None:
        raise ReleaseStateError(f"{field} must be a non-empty portable identifier")
    return value


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseStateError(f"cannot read JSON state {path}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseStateError(f"JSON state must be an object: {path}")
    return value


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as output:
            json.dump(value, output, indent=2, ensure_ascii=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except OSError as error:
        raise ReleaseStateError(f"cannot atomically write {path}: {error}") from error
    finally:
        temporary.unlink(missing_ok=True)


def resolve_existing_file(path: Path, field: str) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise ReleaseStateError(f"{field} does not exist: {path}: {error}") from error
    if not resolved.is_file():
        raise ReleaseStateError(f"{field} must be a file: {resolved}")
    return resolved


def resolve_within(root: Path, path: Path, field: str) -> Path:
    root = root.resolve()
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ReleaseStateError(f"{field} escapes {root}: {resolved}") from error
    return resolved


@contextmanager
def exclusive_lock(lock_path: Path, timeout_seconds: float = 10.0) -> Iterator[None]:
    """Acquire a small cross-platform advisory lock for one state transaction."""

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                if handle.tell() == handle.seek(0, os.SEEK_END):
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError:
            if time.monotonic() >= deadline:
                handle.close()
                raise ReleaseStateError(f"timed out acquiring state lock {lock_path}")
            time.sleep(0.025)
    try:
        yield
    finally:
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def series_lock_path(series_manifest: Path) -> Path:
    return series_manifest.resolve().with_name("RELEASE_SERIES.lock")


def validate_series(value: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "release_line",
        "series_id",
        "generation",
        "next_ordinal",
        "next_rc_suffix",
        "head",
        "consecutive_successful_rcs",
        "entries",
        "pending_operation",
    }
    missing = sorted(required - value.keys())
    if missing:
        raise ReleaseStateError(f"release series is missing fields: {', '.join(missing)}")
    extra = sorted(value.keys() - SERIES_FIELDS)
    if extra:
        raise ReleaseStateError(f"release series has unsupported fields: {', '.join(extra)}")
    if value["schema_version"] != 1 or not isinstance(value["entries"], list):
        raise ReleaseStateError("unsupported release-series schema")
    if not isinstance(value["release_line"], str) or re.fullmatch(r"\d+\.\d+", value["release_line"]) is None:
        raise ReleaseStateError("release-series release_line must use MAJOR.MINOR")
    if not all(
        isinstance(value[field], int) and value[field] >= 0
        for field in ("generation", "consecutive_successful_rcs")
    ):
        raise ReleaseStateError("release-series counters must be non-negative integers")
    if not all(isinstance(value[field], int) and value[field] >= 1 for field in ("next_ordinal", "next_rc_suffix")):
        raise ReleaseStateError("release-series next counters must be positive integers")
    ensure_no_digest_fields(value)
    entries = value["entries"]
    ordinals = [entry.get("ordinal") for entry in entries if isinstance(entry, dict)]
    if len(ordinals) != len(entries) or ordinals != list(range(1, len(entries) + 1)):
        raise ReleaseStateError("release-series ordinals must be unique, contiguous, and append-only")
    if value["next_ordinal"] != len(entries) + 1:
        raise ReleaseStateError("release-series next ordinal does not follow its entries")
    rc_suffixes: list[int] = []
    release_base = f"{value['release_line']}.0"
    previous_manifest: str | None = None
    identities: set[tuple[int, str, int]] = set()
    for entry in entries:
        required_entry = {
            "ordinal",
            "version",
            "candidate_kind",
            "run_id",
            "attempt",
            "candidate_manifest",
            "parent_manifest",
            "state",
            "outcome",
            "sealed",
        }
        if not required_entry.issubset(entry):
            raise ReleaseStateError("release-series entry is incomplete")
        if entry["parent_manifest"] != previous_manifest:
            raise ReleaseStateError("release-series parent chain is forked or skips the current head")
        previous_manifest = entry["candidate_manifest"]
        identity = (entry["ordinal"], entry["run_id"], entry["attempt"])
        if identity in identities:
            raise ReleaseStateError("release-series run identity is duplicated")
        identities.add(identity)
        if entry["candidate_kind"] == "rc":
            match = re.fullmatch(re.escape(release_base) + r"-rc\.([1-9]\d*)", entry["version"])
            if match is None:
                raise ReleaseStateError("release-series RC version is invalid")
            rc_suffixes.append(int(match.group(1)))
        elif entry["candidate_kind"] != "final" or entry["version"] != release_base:
            raise ReleaseStateError("release-series final version is invalid")
    if rc_suffixes != list(range(1, len(rc_suffixes) + 1)) or value["next_rc_suffix"] != len(rc_suffixes) + 1:
        raise ReleaseStateError("release-series RC suffixes are duplicated or non-contiguous")
    if entries:
        head = value["head"]
        if not isinstance(head, dict) or head.get("ordinal") != entries[-1]["ordinal"] or head.get(
            "candidate_manifest"
        ) != entries[-1]["candidate_manifest"]:
            raise ReleaseStateError("release-series head is not its last append-only entry")
    elif value["head"] is not None:
        raise ReleaseStateError("empty release series cannot have a head")
    expected_tail = 0
    for entry in reversed(entries):
        if not entry["sealed"] and entry["state"] in {"development", "staged-rc", "ga-candidate-ready"}:
            continue
        if entry["candidate_kind"] == "final":
            if entry["state"] == "rejected":
                break
            continue
        if entry["state"] == "rc-candidate-ready" and entry["sealed"] and entry["outcome"] == "success":
            expected_tail += 1
        else:
            break
    if value["consecutive_successful_rcs"] != expected_tail:
        raise ReleaseStateError("release-series consecutive successful RC count is inconsistent")


def validate_candidate(value: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "candidate_id",
        "candidate_kind",
        "version",
        "run_id",
        "attempt",
        "ordinal",
        "candidate_root",
        "series_manifest",
        "series_id",
        "series_generation",
        "parent_manifest",
        "state",
        "sealed",
        "outcome",
        "known_issues",
        "generation",
    }
    missing = sorted(required - value.keys())
    if missing:
        raise ReleaseStateError(f"candidate manifest is missing fields: {', '.join(missing)}")
    extra = sorted(value.keys() - CANDIDATE_FIELDS)
    if extra:
        raise ReleaseStateError(f"candidate manifest has unsupported fields: {', '.join(extra)}")
    if value["schema_version"] != 1 or value["candidate_kind"] not in {"rc", "final"}:
        raise ReleaseStateError("unsupported candidate schema")
    if not isinstance(value["known_issues"], list) or not isinstance(value["sealed"], bool):
        raise ReleaseStateError("candidate known_issues/sealed fields are invalid")
    ensure_no_digest_fields(value)
    if not all(isinstance(value[field], int) and value[field] >= 1 for field in ("attempt", "ordinal")):
        raise ReleaseStateError("candidate attempt and ordinal must be positive integers")
    if not isinstance(value["generation"], int) or value["generation"] < 0:
        raise ReleaseStateError("candidate generation must be a non-negative integer")
    kind = value["candidate_kind"]
    version = value["version"]
    if kind == "rc":
        valid_version = re.fullmatch(r"\d+\.\d+\.0-rc\.[1-9]\d*", version) is not None
        allowed_states = {"development", "staged-rc", "rc-candidate-ready", "rejected"}
        sealed_states = {"rc-candidate-ready", "rejected"}
    else:
        valid_version = re.fullmatch(r"\d+\.\d+\.0", version) is not None
        allowed_states = {"development", "ga-candidate-ready", "publication-ready", "rejected"}
        sealed_states = {"publication-ready", "rejected"}
    if not valid_version or value["state"] not in allowed_states:
        raise ReleaseStateError("candidate version, kind, and state are inconsistent")
    if value["sealed"] != (value["state"] in sealed_states):
        raise ReleaseStateError("candidate sealed flag does not match its terminal state")
    expected_outcome = (
        "rejected"
        if value["state"] == "rejected"
        else "success"
        if value["state"] in {"rc-candidate-ready", "ga-candidate-ready", "publication-ready"}
        else None
    )
    if value["outcome"] != expected_outcome:
        raise ReleaseStateError("candidate outcome does not match its state")


def ensure_no_digest_fields(value: Any, path: str = "root") -> None:
    """Reject digest-style release gates while allowing ordinary semantic fields."""

    if isinstance(value, dict):
        for key, child in value.items():
            normalized = key.lower().replace("-", "_")
            if normalized in {"sha", "sha1", "sha256", "digest", "checksum", "content_hash"}:
                raise ReleaseStateError(f"digest field is forbidden at {path}.{key}")
            ensure_no_digest_fields(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            ensure_no_digest_fields(child, f"{path}[{index}]")
