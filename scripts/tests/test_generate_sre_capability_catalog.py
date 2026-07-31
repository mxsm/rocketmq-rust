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

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import generate_sre_capability_catalog as generator  # noqa: E402


class SreCapabilityCatalogGeneratorTest(unittest.TestCase):
    def test_admin_catalog_keeps_phase00_action_and_domain_counts(self) -> None:
        commands = generator.parse_commands(
            generator.CATALOG_SOURCE.read_text(encoding="utf-8")
        )

        self.assertEqual(102, len(commands))
        self.assertEqual(18, len({command.domain for command in commands}))
        self.assertEqual(102, len({command.identifier for command in commands}))

    def test_component_source_surfaces_are_complete_and_resolvable(self) -> None:
        self.assertEqual([], generator.validate_component_surfaces())
        self.assertEqual(
            generator.EXPECTED_COMPONENT_SURFACES,
            {surface.component for surface in generator.COMPONENT_SURFACES},
        )
        self.assertEqual(
            ["MCP"],
            [
                surface.component
                for surface in generator.COMPONENT_SURFACES
                if surface.exposure == "queryable"
            ],
        )

    def test_render_includes_component_source_surface_inventory(self) -> None:
        commands = generator.parse_commands(
            generator.CATALOG_SOURCE.read_text(encoding="utf-8")
        )
        rendered = generator.render(commands, "sha256:fixture")

        self.assertIn("  component_surfaces: 14\n", rendered)
        self.assertIn("component_source_surfaces:\n", rendered)
        self.assertIn('  - component: "Kubernetes"\n', rendered)
        self.assertIn("capabilities:\n", rendered)


if __name__ == "__main__":
    unittest.main()
