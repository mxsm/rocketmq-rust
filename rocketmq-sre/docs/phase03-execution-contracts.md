# Phase 03 supervised-execution contracts

This document is the implementation contract for P3-01. It describes only the
typed planning and execution boundary. Every action descriptor still declares
`execution_supported: false` until its exact Execution Agent handler,
verification, compensation, and test-cluster demonstration are complete.

## Trust boundary

- `ActionRisk` is a display and policy taxonomy and can represent Read, Plan,
  R1, R2, and R3.
- `ExecutionAction` is a closed Rust enum. It represents only the five
  registered R1/R2 action identifiers. R3, shell, raw Admin request codes,
  arbitrary JSON Patch, and unknown identifiers cannot be deserialized.
- Rules-only diagnoses produce `ManualRunbookDraft` with
  `execution_supported: false`. They cannot be sealed as `ActionPlan`.
- A plan is bound to one tenant, cluster, incident, diagnosis revision, and
  non-nil primary model invocation.

## Plan immutability and hashes

`ActionPlan::seal` validates a draft and computes a lowercase
`sha256:<64 hex>` digest using RFC 8785 canonical JSON. The protected plan
material includes schema version, identifiers, diagnosis eligibility, version,
creator, validity window, evidence hash, and every plan step. Lifecycle fields
(`status` and `submitted_at`) and `plan_hash` itself are excluded.

`canonical_evidence_hash` and `canonical_precondition_hash` use the same
canonicalization algorithm. Arrays retain their input ordering; callers must
provide a deterministic semantic order. Objects are canonicalized by RFC 8785.

`submit_for_review` consumes the draft snapshot, verifies its hash, and changes
only lifecycle fields. A content change after submission requires a new
`ActionPlan` version, new hash, and new approvals. The PostgreSQL layer enforces
append-only submitted snapshots in P3-02.

The executable fixture is
`tests/fixtures/phase3/action-plan-draft.v1.json`.

## State transitions

The normal execution path is:

```text
Pending -> Prechecking -> IntentPersisted -> Applying
        -> Verifying -> Succeeded
```

Recovery and compensation paths are:

```text
Applying -> Unknown -> Reconciling
Prechecking | Applying | Verifying -> Compensating
Reconciling -> Verifying | Compensating | Escalated
Compensating -> RolledBack | Escalated
```

An edge outside this graph returns
`ContractError::InvalidStateTransition`. Terminal states have no outgoing
edges.

## Fail-closed errors

| Condition | Typed failure |
| --- | --- |
| Unknown or R3 action identifier | Serde enum error / `ActionCatalogError::UnknownAction` |
| Unknown descriptor version | `ActionCatalogError::UnknownVersion` |
| Inactive, malformed, or unsafe descriptor | `ActionCatalogError::InvalidDescriptor` |
| Rules-only or nil model invocation | `ContractError::InvalidDescriptor` |
| Invalid evidence, precondition, or plan digest | `ContractError::InvalidContentHash` or `InvalidDescriptor` |
| Modified or incompatible plan snapshot | `ContractError::InvalidContentHash` |
| Illegal plan/execution transition | `ContractError::InvalidStateTransition` |
| Descriptor without an enabled typed handler | `ActionCatalogError::ExecutionUnsupported` |

Unknown JSON fields are rejected on every new supervised-execution DTO.
Unknown schema versions and exact descriptor-version drift are rejected before
an execution request can be accepted.

## Committed schemas and descriptors

- `config/schema/` contains the generated Phase 03 JSON Schemas.
- `config/actions/` contains the five immutable Wave 1 descriptor skeletons.
- Descriptor revisions use semantic versions. Any contract change creates a
  new version instead of changing a version already referenced by a plan.
