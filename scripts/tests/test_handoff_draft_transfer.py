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

import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "distribution" / "transfer_handoff_draft.py"


def load_module():
    spec = importlib.util.spec_from_file_location("transfer_handoff_draft_test", MODULE)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load handoff draft transfer module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HandoffDraftTransferTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MODULE.is_file(), "handoff draft transfer must be implemented")
        self.transfer = load_module()

    def test_round_trip_preserves_closed_inventory_without_digests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            draft = root / "draft"
            (draft / "docs").mkdir(parents=True)
            (draft / "PUBLICATION_HANDOFF.json").write_text("{}\n", encoding="utf-8")
            (draft / "docs" / "KNOWN_ISSUES.md").write_text("None.\n", encoding="utf-8")
            bundle = root / "HANDOFF_DRAFT_TRANSFER.tar"
            identity = self._identity()

            self.transfer.export_draft(draft, bundle, identity, ["linux", "windows", "macos"])
            manifest = self.transfer.read_transfer_manifest(bundle)
            imported = self.transfer.import_draft(bundle, root / "imported", identity)

            self.assertEqual(
                (draft / "docs" / "KNOWN_ISSUES.md").read_bytes(),
                (imported / "docs" / "KNOWN_ISSUES.md").read_bytes(),
            )
            self.assertEqual(["linux", "macos", "windows"], manifest["expected_platforms"])
            self.assertFalse((imported / "HANDOFF_DRAFT_TRANSFER.json").exists())
            serialized = json.dumps(manifest).lower()
            self.assertNotIn("sha256", serialized)
            self.assertNotIn("checksum", serialized)

    def test_import_rejects_extra_or_unsafe_members(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = root / "unsafe.tar"
            manifest = {
                "schema_version": 1,
                "candidate_id": "final-1",
                "version": "1.0.0",
                "run_id": "run-1",
                "attempt": 1,
                "expected_platforms": ["linux", "macos", "windows"],
                "files": [],
            }
            with tarfile.open(bundle, "w") as archive:
                payload = json.dumps(manifest).encode()
                info = tarfile.TarInfo("HANDOFF_DRAFT_TRANSFER.json")
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
                evil = b"escape"
                info = tarfile.TarInfo("../escape.txt")
                info.size = len(evil)
                archive.addfile(info, io.BytesIO(evil))
            with self.assertRaisesRegex(ValueError, "safe relative path|unsafe|closed manifest"):
                self.transfer.import_draft(bundle, root / "imported", self._identity())

    @staticmethod
    def _identity():
        return {
            "candidate_id": "final-1",
            "version": "1.0.0",
            "run_id": "run-1",
            "attempt": 1,
        }


if __name__ == "__main__":
    unittest.main()
