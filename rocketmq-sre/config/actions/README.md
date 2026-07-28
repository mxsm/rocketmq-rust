# Phase 3 supervised action catalog

These descriptors are the immutable planning contracts for the first five
supervised actions. They intentionally declare `execution_supported: false`
until the typed Execution Agent handler, verification, compensation, and
test-cluster demonstration for that exact version are complete.

The catalog accepts only the closed `ExecutionAction` R1/R2 enum. R3, raw
request codes, shell commands, and arbitrary Kubernetes patches are not
representable. Descriptor changes require a new semantic version, a new plan
hash, and new human approval.

For `broker.config.patch_allowlisted.v1`, visibility is not equivalent to live
mutability. The current Rust Broker proves generation-CAS execution for
`max_client_event_count`. Thread-pool sizes are absent from the current Broker
response, and `flush_delay_offset_interval_ms` is visible but restart-required;
the production Agent reports those fields as unsupported/restart-required and
never sends them. They remain in the closed schema for compatibility with
heterogeneous RocketMQ estates, but execution is capability-gated per target.
