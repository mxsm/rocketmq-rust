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

"""Enforce the Rust 2024 module layout in the standalone SRE workspace."""

from __future__ import annotations

from pathlib import Path


def main() -> int:
    workspace = Path(__file__).resolve().parents[1]
    legacy_modules = sorted(
        path.relative_to(workspace).as_posix()
        for path in (workspace / "crates").rglob("mod.rs")
    )
    if legacy_modules:
        print("Rust 2024 source layout forbids mod.rs:")
        for path in legacy_modules:
            print(f"- {path}")
        return 1

    print("SRE_SOURCE_LAYOUT_OK mod_rs=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
