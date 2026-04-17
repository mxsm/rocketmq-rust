# Extended Project Context

This file provides additional context beyond the main CLAUDE.md.

## Development Workflow

### Before Starting Work
1. Check if changes affect workspace or standalone projects
2. Identify which crates/packages will be modified
3. Plan minimal validation scope

### During Development
- Run `cargo fmt` frequently to catch formatting issues early
- Use `cargo clippy` on the specific package you're modifying
- Write/update tests alongside code changes
- Keep commits focused and atomic

### Before Finishing
- Ensure all modified code is formatted
- Run clippy on affected packages with `-D warnings`
- Run targeted tests for modified areas
- Verify no unintended changes in other files

## Common Patterns

### Adding a New Feature
1. Identify the target crate (e.g., `rocketmq-client`)
2. Write tests first or alongside implementation
3. Validate: `cargo test -p <crate-name>`
4. Format and lint: `cargo fmt && cargo clippy -p <crate-name> -- -D warnings`

### Fixing a Bug
1. Add a failing test that reproduces the bug
2. Fix the implementation
3. Verify the test passes
4. Run related tests: `cargo test -p <crate-name> <test-pattern>`

### Modifying Shared Crates
If you modify `rocketmq-common`, `rocketmq-runtime`, `rocketmq-macros`, or other shared crates:
- Consider impact on dependent crates
- Run tests for affected dependents
- Check if standalone projects are affected

### Working with Standalone Projects
When in `rocketmq-example` or dashboard projects:
- Change to that directory first
- Run validation commands from there
- Don't assume workspace commands cover these

## Crate Dependencies

### Core Foundation
- `rocketmq-common` - Used by almost everything
- `rocketmq-runtime` - Async runtime abstractions
- `rocketmq-macros` - Compile-time code generation
- `rocketmq-error` - Error types

### Communication Layer
- `rocketmq-remoting` - Network protocol and RPC

### Storage Layer
- `rocketmq-store` - Message persistence

### Service Layer
- `rocketmq-broker` - Main message broker
- `rocketmq-namesrv` - Service discovery
- `rocketmq-controller` - Cluster management
- `rocketmq-proxy` - Proxy gateway

### Client Layer
- `rocketmq-client` - Producer/Consumer SDK

## Performance Considerations

- This is a high-performance messaging system
- Be mindful of allocations in hot paths
- Use async/await appropriately
- Consider zero-copy patterns where applicable
- Profile before optimizing

## Testing Strategy

### Unit Tests
- Test individual functions and methods
- Mock external dependencies
- Fast execution

### Integration Tests
- Test interactions between components
- Use real implementations where practical
- May require setup/teardown

### Example Tests
```bash
# Test a specific module
cargo test -p rocketmq-store --lib storage::commit_log

# Test with output
cargo test -p rocketmq-client -- --nocapture

# Test in release mode (for performance-sensitive code)
cargo test -p rocketmq-remoting --release
```

## Troubleshooting

### Clippy Warnings
- Address all warnings; `-D warnings` treats them as errors
- Use `#[allow(clippy::...)]` sparingly and with justification
- Prefer fixing the code over suppressing warnings

### Test Failures
- Run with `--nocapture` to see println output
- Use `RUST_LOG=debug` for tracing output
- Isolate failing tests: `cargo test -p <pkg> <test-name>`

### Build Issues
- Ensure nightly toolchain is active: `rustup show`
- Clean build artifacts: `cargo clean`
- Check for conflicting feature flags
