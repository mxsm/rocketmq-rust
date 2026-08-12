# Legacy Pull Consumer Compatibility

## Decision

`DefaultMQPullConsumer`, `MQPullConsumer`, and `MQPullConsumerScheduleService` remain exported only so existing
source code receives a stable, typed migration error. RocketMQ Rust does not provide a partial implementation of
these deprecated Java client APIs.

Use `DefaultLitePullConsumer` for manual assignment, polling, pause/resume, and offset management. This preserves
the required business capabilities without reproducing Java implementation classes or callback scheduling.

## Migration map

| Legacy API | RocketMQ Rust API |
| --- | --- |
| `DefaultMQPullConsumer.start` | Build and start `DefaultLitePullConsumer` |
| `MQPullConsumerScheduleService` | Configure LitePull assignments and `pull_thread_nums` |
| `PullTaskCallback` | Application polling loop around LitePull `poll` |
| Manual queue selection | LitePull `assign` |
| Manual offset update | LitePull `seek` and `commit` |

The legacy entry points never panic and never pretend that a request succeeded. They return a typed error naming
`DefaultLitePullConsumer` as the supported replacement.
