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
"""Contracts for the current model-provider compatibility documentation."""

from __future__ import annotations

import unittest
from pathlib import Path


SRE_ROOT = Path(__file__).resolve().parents[2]


class ProviderCompatibilityDocumentationTest(unittest.TestCase):
    def test_matrix_tracks_implemented_and_live_qualified_profiles(self) -> None:
        compatibility = (SRE_ROOT / "docs" / "compatibility.md").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("只提供 ProviderDescriptor 与能力 fixture", compatibility)
        for provider in (
            "OpenAI-compatible",
            "Anthropic",
            "Gemini",
            "AWS Bedrock",
            "DeepSeek",
            "智谱 GLM",
            "Kimi / Moonshot",
            "本地模型",
        ):
            self.assertIn(provider, compatibility)
        self.assertIn("deepseek-v4-flash", compatibility)
        self.assertIn("Ollama `qwen2.5:0.5b`", compatibility)
        self.assertIn("当前没有任何 Provider 被仓库声明为生产认证", compatibility)

    def test_status_manifest_link_resolves(self) -> None:
        status_manifest = SRE_ROOT / "config" / "implementation" / "implementation-status.v1.json"
        self.assertTrue(status_manifest.is_file())


if __name__ == "__main__":
    unittest.main()
