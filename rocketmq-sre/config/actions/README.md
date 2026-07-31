# Phase 3 supervised action catalog

These descriptors are the immutable planning contracts for all Phase 3
supervised actions. Wave 2 descriptors declare `plan_only: false`; Wave 3
descriptors declare `plan_only: true` and can never be registered by the
Execution Agent in this phase. An action remains `execution_supported: false`
until its typed handler, verification, compensation, and test-cluster
demonstration for that exact version are complete.

The catalog accepts only the closed `ExecutionAction` R1/R2 enum. Permanent
R3, raw request codes, shell commands, and arbitrary Kubernetes patches are
not representable. Descriptor changes require a new semantic version, a new
plan hash, and new human approval.
`../action-implementation/implementation-plan.v1.yaml` records Wave 2 delivery
order, driver ownership, the three representative handlers, all Wave 3
negative mappings, and the permanent R3 denylist.

For `broker.config.patch_allowlisted.v1`, visibility is not equivalent to live
mutability. The current Rust Broker proves generation-CAS execution for
`max_client_event_count`. Thread-pool sizes are absent from the current Broker
response, and `flush_delay_offset_interval_ms` is visible but restart-required;
the production Agent reports those fields as unsupported/restart-required and
never sends them. They remain in the closed schema for compatibility with
heterogeneous RocketMQ estates, but execution is capability-gated per target.
