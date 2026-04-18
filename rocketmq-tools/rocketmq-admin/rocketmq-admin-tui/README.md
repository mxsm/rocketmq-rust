# RocketMQ Admin TUI

`rocketmq-admin-tui` is the terminal UI adapter for RocketMQ admin operations.

## Architecture

The TUI depends on `rocketmq-admin-core` for admin capabilities and does not depend on `rocketmq-admin-cli`.

```text
TUI event/state
  -> TuiAdminFacade
  -> rocketmq-admin-core request DTO
  -> rocketmq-admin-core service
  -> TUI view model
```

`TuiAdminFacade` currently exposes compile-checked core service access for topic, nameserver, broker, cluster, connection, consumer, offset, queue, HA, stats, and producer-oriented admin operations. New screens should reuse the facade or core DTO/service APIs instead of calling CLI command modules.

## How to Run

```shell
cargo run -p rocketmq-admin-tui
```

## Design

![](../../../resources/rocketmq-cli-ui.png)

