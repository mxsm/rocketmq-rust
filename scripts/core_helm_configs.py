#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Render core profiles and export their TOML for the real Rust config loaders.

Requires Helm, Python 3.11+, and PyYAML. Output is temporary validation data.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "distribution"))
from core_helm_contract import POLICY

CHART = ROOT / POLICY["chart"]


def render(profile: str, helm: str, *overrides: str) -> list[dict]:
    command = [helm, "template", "core", str(CHART), "--namespace", "core-test", "-f", str(CHART / profile)]
    for override in overrides:
        command.extend(["--set", override])
    result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=60)
    return [document for document in yaml.safe_load_all(result.stdout) if document]


def configurations(documents: list[dict]):
    for document in documents:
        if document["kind"] == "ConfigMap":
            service = document["metadata"]["labels"]["app.kubernetes.io/component"]
            for filename, source in document["data"].items():
                yield service, filename, source, tomllib.loads(source)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helm", default=shutil.which("helm"), required=not shutil.which("helm"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    count = 0
    for profile in POLICY["profiles"]:
        for service, filename, source, _ in configurations(render(profile, args.helm)):
            directory = args.output / service
            directory.mkdir(parents=True, exist_ok=True)
            (directory / f"{Path(profile).stem}--{filename}").write_text(source, encoding="utf-8")
            count += 1
    print(f"Rendered {len(POLICY['profiles'])} profiles and exported {count} TOML configs to {args.output}")


if __name__ == "__main__":
    main()
