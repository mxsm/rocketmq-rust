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
"""Tests for the Required Signals qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_required_signals_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_required_signals_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RequiredSignalsQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_rejects_production_certification_or_mutation(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["production_certified"] = True
        manifest["safety"]["target_mutations"] = 1

        findings = MODULE.validate_manifest(manifest)

        self.assertIn("production_certified must remain False", findings)
        self.assertIn("read-only safety boundary drifted", findings)

    def test_rejects_missing_component_or_metric_alias_drift(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["components"].pop()
        manifest["components"][0]["collector_metric"] = "wrong_metric"

        findings = MODULE.validate_manifest(manifest)

        self.assertIn("component qualification matrix drifted", findings)
        self.assertIn("broker Collector alias is inconsistent", findings)

    def test_rejects_unbounded_query_window(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["limits"]["query_window_minutes"] = 1_440

        self.assertIn("qualification limits drifted", MODULE.validate_manifest(manifest))


if __name__ == "__main__":
    unittest.main()
