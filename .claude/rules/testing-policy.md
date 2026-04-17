# Testing Policy

## Core Principle

**Test only what you changed.** Do not run full-repository or full-workspace tests by default.

## Scope Selection

### 1. Named Test (Most Targeted)
When you know the exact test:
```bash
cargo test -p rocketmq-remoting test_decode_request
cargo test -p rocketmq-store commit_log_test
```

### 2. Module-Level Tests
When you changed a specific module:
```bash
cargo test -p rocketmq-client --lib client::producer
cargo test -p rocketmq-broker --lib processor::
```

### 3. Package-Level Tests
When you changed multiple files in one package:
```bash
cargo test -p rocketmq-common
cargo test -p rocketmq-client --lib
cargo test -p rocketmq-store --tests
```

### 4. Workspace-Level Tests (Rarely Needed)
Only when changes affect shared infrastructure:
```bash
cargo test --workspace
```

## When to Expand Scope

Expand from targeted to broader testing when:
- Modifying `rocketmq-common` (used by many crates)
- Modifying `rocketmq-runtime` (async runtime abstractions)
- Changing `rocketmq-macros` (affects compile-time behavior)
- Changing `rocketmq-error` (error types used everywhere)
- Modifying build configuration or feature flags
- Refactoring cross-crate interfaces or traits

## Test Flags

```bash
# Show test output (useful for debugging)
cargo test -p rocketmq-client -- --nocapture

# Run tests in release mode
cargo test -p rocketmq-store --release

# Run only doc tests
cargo test -p rocketmq-common --doc

# Run only integration tests
cargo test -p rocketmq-broker --tests

# Run only unit tests (lib)
cargo test -p rocketmq-client --lib

# Enable logging in tests
RUST_LOG=debug cargo test -p rocketmq-remoting
```

## Test Coverage Expectations

- New features should have corresponding tests
- Bug fixes should have a regression test
- Refactors should not reduce test coverage
- Public API changes require updated tests

## Standalone Project Tests

For standalone projects, run tests from their directory:
```bash
cd rocketmq-example && cargo test
cd rocketmq-dashboard/rocketmq-dashboard-gpui && cargo test
cd rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri && cargo test
```
