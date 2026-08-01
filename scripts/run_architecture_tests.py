#!/usr/bin/env python3
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

"""Run explicitly inventoried architecture test modules."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "scripts/architecture-validation-inventory.json"
ACTIVE_TIERS = ("pr_static", "milestone_contract", "phase_contract", "dynamic_fixture")
TIERS = (*ACTIVE_TIERS, "deferred_validation")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier", action="append", choices=TIERS)
    args = parser.parse_args()

    policy = json.loads(INVENTORY.read_text(encoding="utf-8"))
    selected_tiers = set(args.tier or ACTIVE_TIERS)
    entries = [entry for entry in policy["python_tests"]["entries"] if entry["tier"] in selected_tiers]
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    failures = 0
    skipped = 0
    for index, entry in enumerate(entries, start=1):
        if entry["platform"] == "powershell" and powershell is None:
            skipped += 1
            print(
                f"ARCHITECTURE_TEST_SKIPPED {index}/{len(entries)} path={entry['path']} "
                "reason=powershell-unavailable",
                flush=True,
            )
            continue
        print(
            f"ARCHITECTURE_TEST_START {index}/{len(entries)} tier={entry['tier']} path={entry['path']}",
            flush=True,
        )
        module = Path(entry["path"]).with_suffix("").as_posix().replace("/", ".")
        command = [sys.executable, "-m", "unittest", module, "-v"]
        result = subprocess.run(command, cwd=ROOT, check=False)
        if result.returncode != 0:
            failures += 1
            print(
                f"ARCHITECTURE_TEST_FAILED path={entry['path']} exit_code={result.returncode}",
                file=sys.stderr,
                flush=True,
            )
        else:
            print(f"ARCHITECTURE_TEST_OK path={entry['path']}", flush=True)

    if failures:
        print(
            f"ARCHITECTURE_CONTRACTS_FAILED modules={len(entries)} failures={failures} skipped={skipped}",
            file=sys.stderr,
        )
        return 1
    status = "ARCHITECTURE_GUARDS_OK" if selected_tiers == {"pr_static"} else "ARCHITECTURE_CONTRACTS_OK"
    print(f"{status} modules={len(entries)} skipped={skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
