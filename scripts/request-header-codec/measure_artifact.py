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

"""Measure a Windows PE artifact and its raw .text section deterministically."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path


def measure(path: Path) -> dict[str, object]:
    data = path.read_bytes()
    if data[:2] != b"MZ" or len(data) < 0x40:
        raise ValueError(f"not a PE artifact: {path}")
    pe_offset = struct.unpack_from("<I", data, 0x3C)[0]
    if data[pe_offset : pe_offset + 4] != b"PE\0\0":
        raise ValueError(f"missing PE signature: {path}")
    section_count = struct.unpack_from("<H", data, pe_offset + 6)[0]
    optional_size = struct.unpack_from("<H", data, pe_offset + 20)[0]
    section_offset = pe_offset + 24 + optional_size
    text_bytes = None
    sections: dict[str, int] = {}
    for index in range(section_count):
        offset = section_offset + index * 40
        name = data[offset : offset + 8].split(b"\0", 1)[0].decode("ascii", errors="strict")
        raw_size = struct.unpack_from("<I", data, offset + 16)[0]
        sections[name] = raw_size
        if name == ".text":
            text_bytes = raw_size
    if text_bytes is None:
        raise ValueError(f"PE artifact lacks .text: {path}")
    return {
        "schemaVersion": 1,
        "artifactFileName": path.name,
        "artifactBytes": len(data),
        "textBytes": text_bytes,
        "sha256": hashlib.sha256(data).hexdigest(),
        "sectionRawBytes": sections,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    document = measure(args.artifact.resolve())
    rendered = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8", newline="\n")
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
