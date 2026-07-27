# Phase 3 supervised action catalog

These descriptors are the immutable planning contracts for the first five
supervised actions. They intentionally declare `execution_supported: false`
until the typed Execution Agent handler, verification, compensation, and
test-cluster demonstration for that exact version are complete.

The catalog accepts only the closed `ExecutionAction` R1/R2 enum. R3, raw
request codes, shell commands, and arbitrary Kubernetes patches are not
representable. Descriptor changes require a new semantic version, a new plan
hash, and new human approval.
