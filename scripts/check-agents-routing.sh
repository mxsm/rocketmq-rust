#!/usr/bin/env bash
#
# Copyright 2023 The RocketMQ Rust Authors
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

set -euo pipefail

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
ROOT="$(cd "$ROOT" && pwd)"

FAILURES=()

add_failure() {
  FAILURES+=("$1")
}

repo_relative_path() {
  local path="$1"
  local absolute
  absolute="$(cd "$(dirname "$path")" && pwd)/$(basename "$path")"
  local relative="${absolute#"$ROOT"/}"
  printf '%s\n' "${relative//\\//}"
}

read_repository_text() {
  local relative_path="$1"
  local path="$ROOT/$relative_path"

  if [[ ! -f "$path" ]]; then
    add_failure "Missing required file: $relative_path"
    printf '\n'
    return
  fi

  cat "$path"
}

text_contains() {
  local text="$1"
  local needle="$2"
  grep -Fqi -- "$needle" <<<"$text"
}

assert_text_contains() {
  local text="$1"
  local needle="$2"
  local context="$3"

  if ! text_contains "$text" "$needle"; then
    add_failure "$context does not mention '$needle'"
  fi
}

find_files_by_name() {
  local file_name="$1"

  find "$ROOT" \
    \( -name .git -o -name .idea -o -name target -o -name node_modules -o -name build -o -name dist \) -prune -o \
    -type f -name "$file_name" -print
}

assert_same_directory_agents() {
  local directory="$1"
  local reason="$2"

  if [[ ! -f "$directory/AGENTS.md" ]]; then
    local relative
    relative="$(repo_relative_path "$directory")"
    add_failure "$reason at '$relative' has no same-directory AGENTS.md"
  fi
}

ROOT_AGENTS_TEXT="$(read_repository_text "AGENTS.md")"
MCP_AGENTS_TEXT="$(read_repository_text "rocketmq-ai/rocketmq-mcp/AGENTS.md")"
MCP_CONTROL_AGENTS_TEXT="$(read_repository_text "rocketmq-ai/rocketmq-mcp-control/AGENTS.md")"

REQUIRED_ROUTE_PATHS=(
  "fuzz/"
  "rocketmq-example/"
  "rocketmq-ai/rocketmq-mcp/"
  "rocketmq-ai/rocketmq-mcp-control/"
  "rocketmq-ai/rocketmq-sre/"
  "rocketmq-dashboard/rocketmq-dashboard-gpui/"
  "rocketmq-dashboard/rocketmq-dashboard-tauri/"
  "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/"
  "rocketmq-dashboard/rocketmq-dashboard-web/"
  "rocketmq-dashboard/rocketmq-dashboard-web/backend/"
  "rocketmq-dashboard/rocketmq-dashboard-web/frontend/"
  "rocketmq-website/"
)

for route_path in "${REQUIRED_ROUTE_PATHS[@]}"; do
  assert_text_contains "$ROOT_AGENTS_TEXT" "$route_path" "Root AGENTS.md"
done

REQUIRED_MCP_COMMANDS=(
  "cargo check --locked"
  "python scripts/check_read_only_boundary.py"
  "cargo test --locked"
  "cargo clippy --locked --all-targets --features streamable-http -- -D warnings"
  "cargo doc --locked --no-deps"
)

REQUIRED_ROOT_TERMS=(
  'root `Cargo.toml`'
  "cargo fmt --all -- --check"
  "cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings"
  "cargo clippy --all-targets --all-features -- -D warnings"
  ".\\scripts\\check-agents-routing.ps1"
  "./scripts/check-agents-routing.sh"
  ".\\scripts\\runtime-audit.ps1 -SkipBaseline"
  ".\\scripts\\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline"
  ".\\scripts\\check-error-hygiene.ps1"
  "python scripts/error_architecture_guard.py"
  "rocksdb_store"
  "otlp-metrics"
  "Validation routes are cumulative"
  "rocketmq-ai/rocketmq-mcp/"
  "rocketmq-doc/en/agents-routing-validation-adr.md"
)

for term in "${REQUIRED_ROOT_TERMS[@]}"; do
  assert_text_contains "$ROOT_AGENTS_TEXT" "$term" "Root AGENTS.md"
done

for command in "${REQUIRED_MCP_COMMANDS[@]}"; do
  assert_text_contains "$MCP_AGENTS_TEXT" "$command" "rocketmq-mcp AGENTS.md validation"
done

for command in "cargo check --locked" "python scripts/check_control_boundary.py" "cargo test --locked" "cargo clippy --locked --all-targets --all-features -- -D warnings"; do
  assert_text_contains "$MCP_CONTROL_AGENTS_TEXT" "$command" "rocketmq-mcp-control AGENTS.md validation"
done

REQUIRED_SHARED_PATHS=(
  "rocketmq-model"
  "rocketmq-protocol"
  "rocketmq-runtime"
  "rocketmq-client"
  "rocketmq-transport"
  "rocketmq-macros"
  "rocketmq-error"
  "rocketmq-observability"
  "rocketmq-dashboard/rocketmq-dashboard-common"
  "rocketmq-tools/rocketmq-admin/rocketmq-admin-core"
)

for shared_path in "${REQUIRED_SHARED_PATHS[@]}"; do
  assert_text_contains "$ROOT_AGENTS_TEXT" "$shared_path" "Root AGENTS.md shared code rule"
done

EXPECTED_PROJECT_AGENTS=(
  "fuzz/AGENTS.md"
  "rocketmq-example/AGENTS.md"
  "rocketmq-ai/rocketmq-mcp/AGENTS.md"
  "rocketmq-ai/rocketmq-mcp-control/AGENTS.md"
  "rocketmq-ai/rocketmq-sre/AGENTS.md"
  "rocketmq-dashboard/rocketmq-dashboard-gpui/AGENTS.md"
  "rocketmq-dashboard/rocketmq-dashboard-tauri/AGENTS.md"
  "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/AGENTS.md"
  "rocketmq-dashboard/rocketmq-dashboard-web/AGENTS.md"
  "rocketmq-dashboard/rocketmq-dashboard-web/backend/AGENTS.md"
  "rocketmq-dashboard/rocketmq-dashboard-web/frontend/AGENTS.md"
  "rocketmq-website/AGENTS.md"
)

for agents_file in "${EXPECTED_PROJECT_AGENTS[@]}"; do
  if [[ ! -f "$ROOT/$agents_file" ]]; then
    add_failure "Missing project AGENTS file: $agents_file"
  fi
done

standalone_cargo_count=0
while IFS= read -r manifest; do
  relative_manifest="$(repo_relative_path "$manifest")"
  if [[ "$relative_manifest" == "Cargo.toml" ]]; then
    continue
  fi

  if grep -Eq '^[[:space:]]*\[workspace\][[:space:]]*$' "$manifest"; then
    standalone_cargo_count=$((standalone_cargo_count + 1))
    manifest_directory="$(dirname "$manifest")"
    relative_directory="$(repo_relative_path "$manifest_directory")/"
    assert_same_directory_agents "$manifest_directory" "Standalone Cargo project"
    assert_text_contains "$ROOT_AGENTS_TEXT" "$relative_directory" "Root AGENTS.md standalone Cargo routing"
  fi
done < <(find_files_by_name "Cargo.toml")

node_project_count=0
while IFS= read -r package_json; do
  package_directory="$(dirname "$package_json")"
  relative_directory="$(repo_relative_path "$package_directory")/"
  node_project_count=$((node_project_count + 1))
  assert_same_directory_agents "$package_directory" "Node project"
  assert_text_contains "$ROOT_AGENTS_TEXT" "$relative_directory" "Root AGENTS.md Node project routing"
done < <(find_files_by_name "package.json")

WORKFLOW_PATHS=(
  ".github/workflows/rocketmq-rust-ci.yaml|Root workspace validation"
  ".github/workflows/fuzz-ci.yml|fuzz/"
  ".github/workflows/rocketmq-example-ci.yaml|rocketmq-example/"
  ".github/workflows/rocketmq-mcp-ci.yaml|rocketmq-ai/rocketmq-mcp/"
  ".github/workflows/rocketmq-sre-ci.yml|rocketmq-ai/rocketmq-sre/"
  ".github/workflows/dashboard-web-ci.yml|rocketmq-dashboard/rocketmq-dashboard-web/"
  ".github/workflows/dashboard-tauri-ci.yml|rocketmq-dashboard/rocketmq-dashboard-tauri/"
  ".github/workflows/website-check.yml|rocketmq-website/"
  ".github/workflows/deploy.yml|rocketmq-website/"
)

for workflow_entry in "${WORKFLOW_PATHS[@]}"; do
  workflow_path="${workflow_entry%%|*}"
  route="${workflow_entry#*|}"
  if [[ ! -f "$ROOT/$workflow_path" ]]; then
    add_failure "Missing required workflow: $workflow_path"
    continue
  fi
  assert_text_contains "$ROOT_AGENTS_TEXT" "$route" "Root AGENTS.md workflow routing for $workflow_path"
done

MCP_WORKFLOW_PATH="$ROOT/.github/workflows/rocketmq-mcp-ci.yaml"
if [[ -f "$MCP_WORKFLOW_PATH" ]]; then
  MCP_WORKFLOW_TEXT="$(cat "$MCP_WORKFLOW_PATH")"
  for command in "${REQUIRED_MCP_COMMANDS[@]}"; do
    assert_text_contains "$MCP_WORKFLOW_TEXT" "$command" "Standalone rocketmq-mcp CI validation"
  done
  for trigger_path in \
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/**" \
    "rocketmq-auth/**" \
    "rocketmq-client/**" \
    "rocketmq-error/**" \
    "rocketmq-macros/**" \
    "rocketmq-model/**" \
    "rocketmq-observability/**" \
    "rocketmq-protocol/**" \
    "rocketmq-runtime/**" \
    "rocketmq-security-api/**" \
    "rocketmq-transport/**" \
    "Cargo.toml" \
    "Cargo.lock" \
    "rust-toolchain.toml"; do
    assert_text_contains "$MCP_WORKFLOW_TEXT" "$trigger_path" "Standalone rocketmq-mcp CI consumer trigger"
  done
fi

SRE_WORKFLOW_PATH="$ROOT/.github/workflows/rocketmq-sre-ci.yml"
if [[ -f "$SRE_WORKFLOW_PATH" ]]; then
  SRE_WORKFLOW_TEXT="$(cat "$SRE_WORKFLOW_PATH")"
  for trigger_path in \
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/**" \
    "rocketmq-ai/rocketmq-mcp/**" \
    "rocketmq-auth/**" \
    "rocketmq-client/**" \
    "rocketmq-error/**" \
    "rocketmq-macros/**" \
    "rocketmq-model/**" \
    "rocketmq-observability/**" \
    "rocketmq-protocol/**" \
    "rocketmq-runtime/**" \
    "rocketmq-security-api/**" \
    "rocketmq-transport/**"; do
    assert_text_contains "$SRE_WORKFLOW_TEXT" "$trigger_path" "Standalone rocketmq-sre CI consumer trigger"
  done
fi

STANDALONE_TRIGGER_GUARD_PATH="$ROOT/scripts/standalone_workspace_trigger_guard.py"
if [[ -f "$STANDALONE_TRIGGER_GUARD_PATH" ]]; then
  PYTHON_COMMAND="${PYTHON:-python3}"
  if ! command -v "$PYTHON_COMMAND" >/dev/null 2>&1; then
    add_failure "Python is required for standalone workspace consumer-trigger validation"
  elif ! "$PYTHON_COMMAND" "$STANDALONE_TRIGGER_GUARD_PATH" --repo-root "$ROOT"; then
    add_failure "Standalone workspace metadata consumer-trigger validation failed"
  fi
fi

ADR_PATH="$ROOT/rocketmq-doc/en/agents-routing-validation-adr.md"
if [[ -f "$ADR_PATH" ]]; then
  ADR_TEXT="$(cat "$ADR_PATH")"
  for term in "AGENTS.md" "check-agents-routing.ps1" "check-agents-routing.sh" 'root `Cargo.toml`' "standalone" "rocketmq-mcp" "rocketmq-mcp-control" "cumulative"; do
    assert_text_contains "$ADR_TEXT" "$term" "AGENTS routing ADR"
  done
else
  add_failure "Missing AGENTS routing ADR: rocketmq-doc/en/agents-routing-validation-adr.md"
fi

if (( ${#FAILURES[@]} > 0 )); then
  printf 'AGENTS routing check failed with %d issue(s):\n' "${#FAILURES[@]}"
  for failure in "${FAILURES[@]}"; do
    printf ' - %s\n' "$failure"
  done
  exit 1
fi

printf 'AGENTS_ROUTING_CHECK_OK standalone_cargo=%d node_projects=%d routes=%d\n' \
  "$standalone_cargo_count" "$node_project_count" "${#REQUIRED_ROUTE_PATHS[@]}"
