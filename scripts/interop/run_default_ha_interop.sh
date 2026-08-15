#!/usr/bin/env bash
# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/run_default_ha_interop.py" "$@"
