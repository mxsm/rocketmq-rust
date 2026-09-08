# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Shared contract for the chart that the core distribution actually packages."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY_RELATIVE_PATH = Path("distribution/helm/rocketmq-rust-core/files/deployment-policy.json")
POLICY = json.loads((ROOT / POLICY_RELATIVE_PATH).read_text(encoding="utf-8"))
REQUIRED_FILES = frozenset(POLICY["chart_files"])
