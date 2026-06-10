# Standalone Projects

## Critical Rule

**Root workspace validation does NOT cover standalone projects.**

Running `cargo` commands from the repository root will not validate these projects.

## Standalone Project List

### 1. rocketmq-example
- **Location**: `rocketmq-example/`
- **Purpose**: Example applications demonstrating RocketMQ usage
- **Validation**:
  ```bash
  cd rocketmq-example
  cargo fmt
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test
  ```

### 2. rocketmq-dashboard-gpui
- **Location**: `rocketmq-dashboard/rocketmq-dashboard-gpui/`
- **Purpose**: GPUI-based dashboard application
- **Validation**:
  ```bash
  cd rocketmq-dashboard/rocketmq-dashboard-gpui
  cargo fmt
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test
  ```

### 3. rocketmq-dashboard-tauri (Backend)
- **Location**: `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/`
- **Purpose**: Tauri application Rust backend
- **Note**: The parent directory is NOT a Cargo workspace root
- **Validation**:
  ```bash
  cd rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri
  cargo fmt
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test
  ```

### 4. rocketmq-dashboard-web (Backend)
- **Location**: `rocketmq-dashboard/rocketmq-dashboard-web/backend/`
- **Purpose**: Web Dashboard Rust 2024 + Axum HTTP API
- **Note**: The Web Dashboard backend is NOT part of the root Cargo workspace
- **Validation**:
  ```bash
  cd rocketmq-dashboard/rocketmq-dashboard-web/backend
  cargo fmt --all
  cargo clippy --all-targets --all-features -- -D warnings
  cargo build --all-targets --all-features
  cargo test
  ```

### 5. rocketmq-dashboard-web (Frontend)
- **Location**: `rocketmq-dashboard/rocketmq-dashboard-web/frontend/`
- **Purpose**: Web Dashboard React + TypeScript + Vite frontend
- **Note**: Root Cargo workspace validation does not cover this Node/Vite project
- **Validation**:
  ```bash
  cd rocketmq-dashboard/rocketmq-dashboard-web/frontend
  npm ci
  npm run build
  ```

## Shared Code Impact Rule

When you modify a shared crate that standalone projects depend on:

### Affected Shared Crates
- `rocketmq-common`
- `rocketmq-runtime`
- `rocketmq-client`
- `rocketmq-remoting`
- Any other crate used by standalone projects

### Required Actions
1. Validate the workspace crate normally
2. Check which standalone projects use it
3. Validate those standalone projects too

### Example Workflow
```bash
# Modified rocketmq-common
cargo clippy -p rocketmq-common -- -D warnings

# Check if rocketmq-example uses it
cd rocketmq-example
cargo clippy -- -D warnings
cargo test

# Check if dashboard projects use it
cd ../rocketmq-dashboard/rocketmq-dashboard-gpui
cargo clippy -- -D warnings

cd ../rocketmq-dashboard-web/backend
cargo clippy --all-targets --all-features -- -D warnings
```

## Why This Matters

Standalone projects:
- Have their own `Cargo.toml` (not in workspace members)
- May have different dependencies or versions
- Can break even when workspace builds succeed
- Must be validated independently

## Quick Check

To verify if a project is standalone:
```bash
# Check workspace members
grep -A 50 "^\[workspace\]" Cargo.toml | grep members

# If a project isn't listed, it's standalone
```
