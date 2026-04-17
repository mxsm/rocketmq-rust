# Rust Validation Rules

## Mandatory Validation Steps

After **every** Rust code change, you MUST:

1. **Format the code**
   ```bash
   cargo fmt --all  # For workspace changes
   cargo fmt        # For standalone projects
   ```

2. **Run clippy with strict warnings**
   ```bash
   cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
   ```

3. **Never finish** with:
   - Unformatted code
   - Clippy warnings
   - Uncommitted formatting changes

## Workspace vs Standalone

### Root Workspace Validation
For changes in workspace members (most crates), run from repository root:

```bash
# Format all workspace code
cargo fmt --all

# Lint all workspace code
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings

# Or target specific package
cargo clippy -p rocketmq-client --all-targets --all-features -- -D warnings
```

### Standalone Project Validation
Root workspace commands do **NOT** cover these projects:

- `rocketmq-example/`
- `rocketmq-dashboard/rocketmq-dashboard-gpui/`
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/`

For standalone projects, **cd into the directory first**:

```bash
cd rocketmq-example
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
```

## Validation Scope

### Minimal Scope (Default)
Only validate the packages you modified:

```bash
cargo fmt -p rocketmq-store
cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings
```

### Broader Scope (When Required)
Use workspace-wide validation when changes affect:
- Shared crates (`rocketmq-common`, `rocketmq-runtime`, `rocketmq-macros`)
- Build configuration (`Cargo.toml`, feature flags)
- Cross-crate APIs or traits
- Multiple packages simultaneously

## Handling Clippy Issues

### Fixing Warnings
1. Read the warning message carefully
2. Apply the suggested fix if appropriate
3. If the warning is incorrect, use `#[allow(clippy::...)]` with a comment explaining why

### Common Clippy Patterns
```rust
// Acceptable suppression with justification
#[allow(clippy::too_many_arguments)]  // Builder pattern would be overkill here
fn legacy_api(...) { }

// Prefer fixing over suppressing
// Bad: #[allow(clippy::needless_return)]
// Good: Remove the unnecessary return statement
```

## CI/CD Alignment

These validation rules match the CI pipeline expectations:
- All code must pass `cargo fmt --check`
- All code must pass `cargo clippy -- -D warnings`
- Tests must pass for affected packages

Running these locally prevents CI failures.
