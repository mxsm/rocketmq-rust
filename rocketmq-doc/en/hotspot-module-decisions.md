# Hotspot module decisions

Status: Accepted

This ADR records the architecture decision for every hotspot on the original
top-20 board and for the replacement that entered the current board. A high
multi-factor score is a review signal, not an instruction to split a file by
line count. `decomposed` means domain-owned private child modules already carry
independent use cases. `retained` means the reviewed parent remains the safest
single public schema, protocol, storage, or mutable-state owner.

The implementation baseline is commit
`9b7af257d3315e3ac48616ae3d0f1d8acccf7835`. Metrics below were regenerated
from that checkout. Retention is conditional: each entry has an explicit
trigger that returns it to active decomposition review.

## Decision rules

- A child module must own a named use case, not merely re-export its parent.
- Mutable state, lock guards, task handles, and concrete storage backends stay
  with one parent owner unless a narrow value boundary exists.
- Public item and re-export counts must not grow as a side effect of a split.
- New production modules remain at or below 800 production lines.
- `misc.rs`, `utils2.rs`, and `impl_2.rs` are not valid domain boundaries.

## Decisions

### `rocketmq-broker/src/broker_runtime.rs`

- Decision: `decomposed`.
- Owner: RocketMQ broker runtime maintainers.
- State owner: `BrokerRuntime` remains the only composition and lifecycle state owner.
- Evidence: Seven private children own composition, control plane, data plane, lifecycle, metadata, request pipeline, and shutdown reporting; the parent is 767 production lines.
- Revisit when: The parent crosses 800 production lines, adds another state owner, or implements request algorithms outside those children.

### `rocketmq-client/src/producer/default_mq_producer.rs`

- Decision: `retained`.
- Owner: RocketMQ client producer API maintainers.
- State owner: `DefaultMQProducer` remains the public façade and delegates mutable execution state to one producer implementation.
- Evidence: The 2,138 production lines are dominated by 182 intentional public configuration and delegation methods; lifecycle, retry, send, and transaction algorithms live under `default_mq_producer_impl/`.
- Revisit when: A façade method gains non-delegating send or retry logic, fan-out exceeds 10, or the public surface grows without an API decision.

### `rocketmq-store/src/message_store/local_file_message_store.rs`

- Decision: `decomposed`.
- Owner: RocketMQ local message store maintainers.
- State owner: `LocalFileMessageStore` remains the single owner of commit log, queues, checkpoints, and lifecycle state.
- Evidence: Seven private children own composition, dispatch, health, lifecycle, read, recovery, and write paths while the parent preserves one storage owner.
- Revisit when: A child duplicates an `Arc` storage root, exposes a lock guard, or the parent adds an eighth unrelated use case.

### `rocketmq-store/src/log_file/commit_log.rs`

- Decision: `retained`.
- Owner: RocketMQ CommitLog maintainers.
- State owner: `CommitLog` remains the single owner of persisted segment state and recovery ordering.
- Evidence: Append sequencing, context, and handles already have private children; the remaining 2,277 production lines jointly enforce append, recovery, flush, and replication invariants over one on-disk format.
- Revisit when: A recovery or flush value boundary can be tested without sharing mapped-file state, or lock sites grow beyond the current 10.

### `rocketmq-client/src/consumer/default_mq_push_consumer.rs`

- Decision: `retained`.
- Owner: RocketMQ push-consumer public API maintainers.
- State owner: `DefaultMQPushConsumer` remains the public configuration façade and owns no second consume state machine.
- Evidence: Its 1,152 production lines and 190 public items express configuration and delegation; execution, rebalance, offset, and consume state stay in `DefaultMQPushConsumerImpl`.
- Revisit when: The façade starts owning background work, lock sites appear, or public items grow without an API decision.

### `rocketmq-client/src/factory/mq_client_instance.rs`

- Decision: `decomposed`.
- Owner: RocketMQ client runtime maintainers.
- State owner: `MQClientInstance` remains the only client-session and shared-factory state owner.
- Evidence: Connection listener and route conversion are private child modules, and producer, consumer, route, and shutdown work is reached through narrow capability paths rather than a second factory owner.
- Revisit when: Fan-out exceeds the current 13, lock sites exceed 12, or route conversion returns mutable factory state.

### `rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/admin_facade/operations.rs`

- Decision: `retained`.
- Owner: RocketMQ Admin TUI maintainers.
- State owner: `TuiAdminFacade` remains a stateless command façade over one admin client owner.
- Evidence: Despite 2,916 production lines and 203 public operations, the module has fan-out 3, no lock site, and no state owner; splitting it would create method containers without reducing dependencies or mutable ownership.
- Revisit when: A command family acquires independent state, fan-out exceeds 3, a lock is introduced, or focused family fixtures justify a value boundary.

### `rocketmq-client/src/consumer/consumer_impl/default_lite_pull_consumer_impl.rs`

- Decision: `retained`.
- Owner: RocketMQ lite-pull implementation maintainers.
- State owner: `DefaultLitePullConsumerImpl` remains the only assignment, poll, and offset state owner.
- Evidence: Assignment registry, configuration, and model already have private children; the parent coordinates 43 lock sites without exporting guards and has 40 focused tests.
- Revisit when: Assignment, polling, or offset transitions can cross a value-only boundary, or lock sites exceed 43.

### `rocketmq-store/src/config/message_store_config.rs`

- Decision: `retained`.
- Owner: RocketMQ store configuration maintainers.
- State owner: `MessageStoreConfig` remains the canonical serialized configuration schema.
- Evidence: The module has no lock or mutable service owner; 1,982 production lines and 126 public items are schema fields, defaults, validation, and compatibility projection with 39 focused tests.
- Revisit when: Validation becomes independently stateful, a projection can use a typed value object, or the schema gains a second owner.

### `rocketmq-broker/src/config/broker_config.rs`

- Decision: `retained`.
- Owner: RocketMQ broker configuration maintainers.
- State owner: `BrokerConfig` remains the canonical serialized broker configuration schema.
- Evidence: The module has no lock or service state owner; 1,794 production lines and 181 public items keep fields, defaults, validation, and serialization discoverable in one schema.
- Revisit when: Validation requires external I/O, a typed projection becomes independently reusable, or the schema gains a second owner.

### `rocketmq-client/src/implementation/mq_client_api_impl.rs`

- Decision: `decomposed`.
- Owner: RocketMQ client remoting API maintainers.
- State owner: `MQClientAPIImpl` remains the one remoting composition owner.
- Evidence: Eight private children own admin, consumer, producer, request building, response decoding, route, transaction, and transport use cases; the root is 401 production lines.
- Revisit when: The root crosses 500 production lines, fan-out exceeds 13, or a child reaches across another child's private state.

### `rocketmq-observability/src/semantic.rs`

- Decision: `retained`.
- Owner: RocketMQ observability maintainers.
- State owner: The module is a stateless canonical semantic-name registry.
- Evidence: All 234 production lines and 229 public items are unique metric, trace, log, and resource names; it has no lock, state owner, dependency fan-out, or executable algorithm to isolate.
- Revisit when: Executable registration logic enters the module, a semantic namespace exceeds a reviewable value boundary, or duplicate names are introduced.

### `rocketmq-client/src/consumer/consumer_impl/default_mq_push_consumer_impl.rs`

- Decision: `retained`.
- Owner: RocketMQ push-consumer implementation maintainers.
- State owner: `DefaultMQPushConsumerImpl` remains the only lifecycle, rebalance, consume, and offset state owner.
- Evidence: The module coordinates 19 lock sites and delegates to dedicated rebalance and consume services; keeping transitions together avoids exporting guards or cloning mutable roots.
- Revisit when: A lifecycle or offset transition can be expressed as an owned input/output value, or lock sites exceed 19.

### `rocketmq-client/src/consumer/default_lite_pull_consumer.rs`

- Decision: `retained`.
- Owner: RocketMQ lite-pull public API maintainers.
- State owner: `DefaultLitePullConsumer` remains a public delegation façade over one implementation owner.
- Evidence: Separate capability trait implementations group subscription, assignment, polling, offsets, and lifecycle without adding state; the façade has only four lock references and no background-task owner.
- Revisit when: Any capability implementation begins owning independent mutable state, fan-out exceeds 9, or the public surface grows.

### `rocketmq-broker/src/out_api/broker_outer_api.rs`

- Decision: `retained`.
- Owner: RocketMQ broker outbound API maintainers.
- State owner: `BrokerOuterAPI` remains the single owner of remoting client and nameserver address state.
- Evidence: The 1,491 production lines cover controller, nameserver, and broker requests over the same client and address snapshot; the module has no lock site and fan-out is 7.
- Revisit when: A request family gains independent lifecycle state, fan-out exceeds 7, or a value-only transport boundary becomes reusable.

### `rocketmq-client/src/producer/producer_impl/default_mq_producer_impl.rs`

- Decision: `decomposed`.
- Owner: RocketMQ producer implementation maintainers.
- State owner: `DefaultMQProducerImpl` remains the only producer execution and task-lifecycle owner.
- Evidence: Four private children own lifecycle, retry, send, and transaction behavior; the composition root is 334 production lines.
- Revisit when: The root crosses 500 production lines, lock sites exceed 10, or child modules expose task handles.

### `rocketmq-broker/src/processor/send_message_processor.rs`

- Decision: `retained`.
- Owner: RocketMQ broker send-path maintainers.
- State owner: `SendMessageProcessor` remains one request processor over injected store and runtime capabilities.
- Evidence: Capability and message construction already have private children; the parent preserves validation, routing, store dispatch, and response ordering without locks or a second state owner.
- Revisit when: A value-only validation or response mapper reaches 100 production lines, fan-out exceeds 13, or another defect is traced to mixed response ordering.

### `rocketmq-broker/src/schedule/schedule_message_service.rs`

- Decision: `retained`.
- Owner: RocketMQ delayed-message maintainers.
- State owner: `ScheduleMessageService` remains the only owner of delay offsets, delivery lifecycle, and persistence coordination.
- Evidence: The module's 22 lock sites and three state owners form one ordered scheduler; moving methods without an owned state transition would export guards or duplicate cancellation state.
- Revisit when: Delivery or persistence can accept an immutable snapshot and return a complete transition, or lock sites exceed 22.

### `rocketmq-client/src/producer/produce_accumulator.rs`

- Decision: `retained`.
- Owner: RocketMQ producer batching maintainers.
- State owner: `ProduceAccumulator` remains the only admission, batch, flush, and shutdown owner.
- Evidence: Four state owners and 47 lock or atomic sites coordinate permit release, synchronous and asynchronous guards, deadlines, and shutdown; all helpers remain private and guards never cross the module.
- Revisit when: Batch transitions can be represented by an owned command/result pair, lock sites exceed 47, or a guard task handle becomes public.

### `rocketmq-client/src/admin/default_mq_admin_ext_impl.rs`

- Decision: `decomposed`.
- Owner: RocketMQ client admin maintainers.
- State owner: `DefaultMQAdminExtImpl` remains the single admin composition owner.
- Evidence: Seven private children own generic admin API, broker, group, lifecycle, mutation, security, and topic capabilities; the root is 252 production lines with one public item and has left the current top-20 board.
- Revisit when: The root crosses 500 production lines, a new command family stays in the root, or a child duplicates admin client state.

### `rocketmq-broker/src/processor/pop_message_processor.rs`

- Decision: `retained`.
- Owner: RocketMQ broker POP-path maintainers.
- State owner: `PopMessageProcessor` remains the single POP request and offset-transition owner.
- Evidence: The capability boundary is already private; the current replacement hotspot has 1,534 production lines, fan-out 9, and 19 lock or atomic sites that participate in one ordered POP transition.
- Revisit when: Validation or response mapping can use owned values without offset state, lock sites exceed 19, or fan-out exceeds 9.

## Outcome

The original twenty hotspots all have a reviewed decision. Six are confirmed as
domain decomposition roots and fourteen are retained single-owner boundaries.
The original Client admin entry is one of the decomposed roots and has left the
current board. The retained Broker POP processor that replaced it is also governed.
Future board regeneration therefore cannot silently introduce an unowned
ranked hotspot.
