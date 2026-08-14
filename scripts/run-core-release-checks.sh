#!/usr/bin/env bash
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0 (the "License");

set -uo pipefail

phase=""
arguments=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --phase) phase="$2"; shift 2 ;;
    --version|--run-id|--attempt) arguments+=("$1" "$2"); shift 2 ;;
    --include-repo-global|--list) arguments+=("$1"); shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [[ ! "$phase" =~ ^[0-6]$ ]]; then
  echo "--phase 0..6 is required" >&2
  exit 2
fi
python_command="${PYTHON:-}"
if [[ -z "$python_command" ]]; then
  if command -v python >/dev/null 2>&1; then
    python_command="python"
  else
    python_command="python3"
  fi
fi
"$python_command" scripts/core_release_checks.py --phase "$phase" "${arguments[@]}"
