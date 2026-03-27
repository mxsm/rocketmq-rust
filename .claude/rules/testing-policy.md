# Testing Policy

- Tests only need to cover the modified area.
- Do not run full-repository or full-workspace tests by default.
- Prefer the smallest effective scope:
  - package-level tests
  - module-related tests
  - named tests
  - integration tests related to the change
- Run broader tests only when the change affects shared infrastructure, shared crates, build configuration, or multiple crates/projects.

## Examples

```bash
cargo test -p rocketmq-common
cargo test -p rocketmq-client --lib
cargo test -p rocketmq-remoting some_test_name
```
