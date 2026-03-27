# Standalone Projects

- `rocketmq-example` must be validated in its own directory.
- `rocketmq-dashboard/rocketmq-dashboard-gpui` must be validated in its own directory.
- `rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri` must be validated in its own directory.
- Do not treat root workspace validation as sufficient for these projects.

## Shared code rule

- If a shared crate is modified and that change can affect a standalone project, also validate the affected standalone project.
