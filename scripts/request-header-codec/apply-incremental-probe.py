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

"""Apply or restore the frozen derive-input incremental-build probe."""

from __future__ import annotations

import argparse
from pathlib import Path


BEFORE = b'#[doc = "REQUEST_HEADER_CODEC_INCREMENTAL_PROBE: 0"]'
AFTER = b'#[doc = "REQUEST_HEADER_CODEC_INCREMENTAL_PROBE: 1"]'


def apply(source: Path, backup: Path) -> None:
    data = source.read_bytes()
    if data.count(BEFORE) != 1 or AFTER in data:
        raise ValueError("incremental probe source must contain exactly the frozen : 0 attribute")
    if backup.exists():
        raise ValueError(f"backup already exists: {backup}")
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_bytes(data)
    source.write_bytes(data.replace(BEFORE, AFTER))


def restore(source: Path, backup: Path) -> None:
    if not backup.is_file():
        raise ValueError(f"incremental probe backup is missing: {backup}")
    source.write_bytes(backup.read_bytes())
    backup.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("apply", "restore"))
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--backup", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "apply":
        apply(args.source.resolve(), args.backup.resolve())
    else:
        restore(args.source.resolve(), args.backup.resolve())


if __name__ == "__main__":
    main()
