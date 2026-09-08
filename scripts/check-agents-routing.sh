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

ROOT_AGENTS_TEXT=""
if [[ -f "$ROOT/AGENTS.md" ]]; then
  ROOT_AGENTS_TEXT="$(cat "$ROOT/AGENTS.md")"
else
  add_failure "Missing required file: AGENTS.md"
fi

REQUIRED_ROUTE_PATHS=(
  "fuzz/"
  "rocketmq-example/"
  "rocketmq-ai/rocketmq-mcp/"
  "rocketmq-ai/rocketmq-mcp-control/"
  "rocketmq-ai/rocketmq-sre/"
  "rocketmq-ai/rocketmq-sre/ui/"
  "rocketmq-ai/rocketmq-sre/sdk/typescript/"
  "rocketmq-macros/tests/fixtures/renamed-consumer/"
  "rocketmq-dashboard/rocketmq-dashboard-gpui/"
  "rocketmq-dashboard/rocketmq-dashboard-tauri/"
  "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/"
  "rocketmq-dashboard/rocketmq-dashboard-web/"
  "rocketmq-dashboard/rocketmq-dashboard-web/backend/"
  "rocketmq-dashboard/rocketmq-dashboard-web/frontend/"
  "rocketmq-website/"
)

# Check routing structure, not wording or complete command profiles.
for route_path in "${REQUIRED_ROUTE_PATHS[@]}"; do
  assert_text_contains "$ROOT_AGENTS_TEXT" "$route_path" "Root AGENTS.md"
  agents_file="${route_path}AGENTS.md"
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

REQUIRED_WORKFLOWS=(
  ".github/workflows/rocketmq-rust-ci.yaml"
  ".github/workflows/fuzz-ci.yml"
  ".github/workflows/rocketmq-example-ci.yaml"
  ".github/workflows/rocketmq-mcp-ci.yaml"
  ".github/workflows/rocketmq-sre-ci.yml"
  ".github/workflows/dashboard-gpui-ci.yml"
  ".github/workflows/dashboard-web-ci.yml"
  ".github/workflows/dashboard-tauri-ci.yml"
  ".github/workflows/website-check.yml"
  ".github/workflows/deploy.yml"
)

for workflow in "${REQUIRED_WORKFLOWS[@]}"; do
  if [[ ! -f "$ROOT/$workflow" ]]; then
    add_failure "Missing required workflow: $workflow"
  fi
done

# Dependency/feature audits are separate integration checks; do not invoke Cargo metadata here.
for document in \
  "rocketmq-doc/en/agents-routing-validation-adr.md" \
  "rocketmq-doc/en/agent-validation-reference.md"; do
  if [[ ! -f "$ROOT/$document" ]]; then
    add_failure "Missing validation reference: $document"
  fi
done

if (( ${#FAILURES[@]} > 0 )); then
  printf 'AGENTS routing check failed with %d issue(s):\n' "${#FAILURES[@]}"
  for failure in "${FAILURES[@]}"; do
    printf ' - %s\n' "$failure"
  done
  exit 1
fi

printf 'AGENTS_ROUTING_CHECK_OK standalone_cargo=%d node_projects=%d routes=%d\n' \
  "$standalone_cargo_count" "$node_project_count" "${#REQUIRED_ROUTE_PATHS[@]}"
