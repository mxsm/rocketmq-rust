#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON=python3
if ! command -v "$PYTHON" >/dev/null 2>&1; then
  PYTHON=python
fi
exec "$PYTHON" "$ROOT/scripts/runtime_audit.py" --output "${1:-target/runtime-audit}" --scope "${2:-all}"
