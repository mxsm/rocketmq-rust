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

"""Publishable root workspace crates must package the repository legal files.

`cargo package` only archives files inside a package directory, so every publishable
member keeps its own copy of the root `LICENSE-APACHE` and `NOTICE`.
"""

from __future__ import annotations

import fnmatch
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LEGAL_FILES = ("LICENSE-APACHE", "NOTICE")


def load_manifest(path: Path) -> dict[str, object]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def publishable_members() -> list[Path]:
    workspace = load_manifest(ROOT / "Cargo.toml")
    members: list[Path] = []
    for pattern in workspace["workspace"]["members"]:
        for directory in sorted(ROOT.glob(pattern)):
            package = load_manifest(directory / "Cargo.toml").get("package", {})
            publish = package.get("publish", True)
            if publish is False or publish == []:
                continue
            members.append(directory)
    return members


class CrateLegalFilesTests(unittest.TestCase):
    def test_workspace_has_publishable_members(self) -> None:
        self.assertTrue(publishable_members(), "no publishable root workspace members were found")

    def test_publishable_members_carry_the_root_legal_files(self) -> None:
        for name in LEGAL_FILES:
            expected = (ROOT / name).read_bytes()
            for member in publishable_members():
                with self.subTest(member=member.relative_to(ROOT).as_posix(), file=name):
                    copy = member / name
                    self.assertTrue(copy.is_file(), f"copy the root {name} into {member.relative_to(ROOT).as_posix()}")
                    self.assertEqual(expected, copy.read_bytes(), f"{copy.relative_to(ROOT).as_posix()} differs from the root {name}")

    def test_package_file_lists_keep_the_legal_files(self) -> None:
        for member in publishable_members():
            package = load_manifest(member / "Cargo.toml").get("package", {})
            include = package.get("include")
            exclude = package.get("exclude", [])
            for name in LEGAL_FILES:
                with self.subTest(member=member.relative_to(ROOT).as_posix(), file=name):
                    if include is not None:
                        self.assertTrue(
                            any(fnmatch.fnmatch(name, pattern.lstrip("/")) for pattern in include),
                            f"package.include must list {name}",
                        )
                    self.assertFalse(
                        any(fnmatch.fnmatch(name, pattern.lstrip("/")) for pattern in exclude),
                        f"package.exclude must not match {name}",
                    )


if __name__ == "__main__":
    unittest.main()
