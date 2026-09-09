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
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import generate_sre_capability_catalog as generator  # noqa: E402


class SreCapabilityCatalogGeneratorTest(unittest.TestCase):
    def test_admin_catalog_excludes_retired_container_commands(self) -> None:
        commands = generator.parse_commands(
            generator.CATALOG_SOURCE.read_text(encoding="utf-8")
        )

        identifiers = {command.identifier for command in commands}
        domains = {command.domain for command in commands}
        self.assertEqual(100, len(commands))
        self.assertEqual(17, len(domains))
        self.assertEqual(100, len(identifiers))
        self.assertNotIn("container.add_broker", identifiers)
        self.assertNotIn("container.remove_broker", identifiers)
        self.assertNotIn("Container", domains)

    def test_source_revision_is_portable_and_tracks_content_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "catalog.rs"
            with patch.object(generator, "CATALOG_SOURCE", source):
                source.write_bytes(b"fn command_catalog() {\n}\n")
                lf_revision = generator.source_revision()
                source.write_bytes(b"fn command_catalog() {\r\n}\r\n")
                self.assertEqual(lf_revision, generator.source_revision())
                source.write_bytes(b"fn changed_catalog() {\n}\n")
                self.assertNotEqual(lf_revision, generator.source_revision())

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
        self.assertIn('    exposure: "protected_component_endpoints"\n', rendered)
        self.assertIn("capabilities:\n", rendered)


if __name__ == "__main__":
    unittest.main()
