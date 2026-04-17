# RocketMQ Rust - Claude Code Instructions

This is an unofficial Rust implementation of Apache RocketMQ, a distributed messaging and streaming platform.

## Project Overview

- **Language**: Rust (nightly toolchain, MSRV 1.85.0)
- **Architecture**: Multi-crate Cargo workspace with standalone projects
- **Key Components**: Broker, NameServer, Client, Store, Remoting, Controller, Proxy
- **License**: Apache-2.0 OR MIT

## Repository Structure

### Main Workspace
Root Cargo workspace containing core crates:
- `rocketmq-broker` - Message broker implementation
- `rocketmq-client` - Client SDK
- `rocketmq-common` - Shared utilities and types
- `rocketmq-store` - Storage engine
- `rocketmq-remoting` - Network communication layer
- `rocketmq-namesrv` - Name server for routing
- `rocketmq-controller` - Cluster controller
- `rocketmq-proxy` - Proxy layer
- `rocketmq-auth` - Authentication module
- `rocketmq-filter` - Message filtering
- `rocketmq-runtime` - Async runtime abstractions
- `rocketmq-macros` - Procedural macros
- `rocketmq-error` - Error types
- `rocketmq-tools/*` - Admin CLI, TUI, and store inspection tools
- `rocketmq-dashboard/rocketmq-dashboard-common` - Dashboard shared code

### Standalone Projects (Not in Workspace)
These require separate validation in their own directories:
- `rocketmq-example` - Example applications
- `rocketmq-dashboard/rocketmq-dashboard-gpui` - GPUI-based dashboard
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri` - Tauri app backend

## Validation Rules

### Mandatory After Every Rust Change
1. **Format**: `cargo fmt --all` (for workspace) or `cargo fmt` (for standalone)
2. **Lint**: `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`
3. **Never finish** with unformatted code or clippy warnings

### Testing Policy
- **Default**: Targeted tests only (modified area)
- **Scope examples**:
  ```bash
  cargo test -p rocketmq-common              # Package-level
  cargo test -p rocketmq-client --lib        # Library tests only
  cargo test -p rocketmq-remoting test_name  # Specific test
  ```
- **Broader tests** only when changes affect:
  - Shared infrastructure
  - Multiple crates
  - Build configuration
  - Cross-crate APIs

### Standalone Project Validation
Root workspace commands do NOT cover standalone projects. When modifying:
- `rocketmq-example` → validate in `rocketmq-example/`
- `rocketmq-dashboard/rocketmq-dashboard-gpui` → validate in that directory
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri` → validate in that directory

If shared crate changes affect standalone projects, validate those too.

## Working Principles

1. **Minimal scope** - Only validate what you changed
2. **Targeted tests** - Don't run full workspace tests by default
3. **Clean diffs** - Keep changes focused on the task
4. **Test coverage** - Add/update tests when behavior changes
5. **Follow hierarchy** - Subdirectory `CLAUDE.md` files override this one

## Additional Rules

See also:
- `.claude/CLAUDE.md` - Extended project memory
- `.claude/rules/` - Specific rule files
- `AGENTS.md` - Agent-specific instructions (similar content)

## Code Style

- Follow `rustfmt.toml` configuration (max_width=120, imports_granularity="Item")
- Use nightly Rust features as configured in `rust-toolchain.toml`
- Respect existing patterns in the codebase
