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
"""Contracts for the enterprise OIDC role fixtures and qualification smoke."""

from __future__ import annotations

import json
import unittest
from pathlib import Path


SRE_ROOT = Path(__file__).resolve().parents[2]


class EnterpriseIdentityContractTest(unittest.TestCase):
    def test_realm_declares_human_and_service_roles(self) -> None:
        realm = json.loads(
            (SRE_ROOT / "deploy" / "dev" / "enterprise-realm.json").read_text(
                encoding="utf-8"
            )
        )
        role_names = {role["name"] for role in realm["roles"]["realm"]}

        self.assertTrue(
            {"operator", "approver", "executor-service", "execution-agent"}
            <= role_names
        )
        operator = next(user for user in realm["users"] if user["username"] == "sre-operator")
        self.assertTrue({"operator", "approver"} <= set(operator["realmRoles"]))

    def test_smoke_requires_standard_realm_role_claims(self) -> None:
        smoke = (SRE_ROOT / "scripts" / "enterprise-integrations-smoke.ps1").read_text(
            encoding="utf-8"
        )

        self.assertIn("$claims.realm_access.roles", smoke)
        self.assertIn("'operator', 'approver', 'model-governance'", smoke)
        self.assertIn("standard_realm_role_mapping_verified = $true", smoke)


if __name__ == "__main__":
    unittest.main()
