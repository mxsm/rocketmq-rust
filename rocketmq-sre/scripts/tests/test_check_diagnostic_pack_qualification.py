#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the complete diagnostic-pack qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_diagnostic_pack_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_diagnostic_pack_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class DiagnosticPackQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_object(MODULE.DEFAULT_MANIFEST)

    def test_checked_in_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_rejects_unknown_inspection_template(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["packs"][0]["inspection_template"] = "unknown"

        findings = MODULE.validate_manifest(manifest)

        self.assertTrue(any("inspection template" in finding for finding in findings))

    def test_rejects_missing_pack_scenario(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["packs"][0]["scenarios"].pop()

        findings = MODULE.validate_manifest(manifest)

        self.assertTrue(any("normal, fault, and missing" in finding for finding in findings))

    def test_rejects_credential_like_material(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["packs"][0]["id"] = "Bearer unsafe-example-token"

        findings = MODULE.validate_manifest(manifest)

        self.assertIn("manifest contains credential-like material", findings)


if __name__ == "__main__":
    unittest.main()
