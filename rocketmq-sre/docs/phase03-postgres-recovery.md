# Phase 03 PostgreSQL persistence and recovery

P3-02 keeps every durable execution record in PostgreSQL. The development
database runs in Docker; no host PostgreSQL installation is required.

## Start and migrate

From `rocketmq-sre/`:

```powershell
docker compose -f deploy/dev/compose.yaml up -d postgres
$env:DATABASE_URL = 'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre'
cargo +1.95.0 run --locked -p rocketmq-sre-control-plane
```

The Control Plane applies forward-only migrations through
`0028_supervised_workflow.sql` before readiness becomes true. PostgreSQL data
is held in the Compose `postgres-data` volume.

The optional development identity graph can be loaded after migration:

```powershell
Get-Content -Raw deploy/dev/postgres/phase3-seed.sql |
  docker compose -f deploy/dev/compose.yaml exec -T postgres `
    psql -v ON_ERROR_STOP=1 -U rocketmq_sre -d rocketmq_sre
```

The seed is idempotent. It creates one read-only cluster, one incident, one
model-backed eligible diagnosis, one model profile, and bounded primary/Critic
invocation fixtures. It does not create approvals, executions, leases,
credentials, or RocketMQ changes. The Control Plane integration test reloads
the seed and verifies its canonical Evidence content hash.

## Durable tables and invariants

| Boundary | Tables | Invariant |
| --- | --- | --- |
| Planning | `action_plans`, `policy_decisions`, `approvals`, `critic_reviews` | Protected request snapshots are immutable; decisions/reviews are append-only |
| Execution | `executions`, `execution_steps`, `audit_events` | Idempotency key is unique; intent and audit commit in one transaction; result is appended |
| Resource safety | `resource_locks`, `resource_quarantines` | Locks expire/release; quarantine persists until an approver supplies reason and evidence |
| Executor ownership | `executor_leases` | Every takeover increments epoch and remains `PendingFence` until Agent acknowledgement |
| Agent safety | `execution_agent_fences`, `execution_agent_effects` | Highest epoch never decreases; effects persist `Prepared` then `Dispatched` before confirmation |

The database rejects StepIntent rows without the latest active lease epoch. It
also rejects Agent effects whose lease/epoch/tenant do not match the durable
Agent fence.

`diagnosis_revisions.execution_eligible=true` is accepted only when
`primary_model_invocation_id` is non-null. The former Phase 01 fail-closed
constraint is replaced, not removed without a successor.

## Restart recovery

Executor recovery uses `ExecutionJournal::pending_intents`:

1. Load intent rows without a matching result.
2. Use an exact, read-only action handler to inspect live state.
3. Classify the result as already applied, compensation required, or
   ambiguous.
4. Append the recovered result or move the execution to compensation/manual
   escalation. Never retry an external write from ambiguity alone.

Agent recovery loads the cluster's highest durable epoch and every effect in
`Prepared`, `Dispatched`, or `Unknown`. Reusing an idempotency key returns the
same effect only when the full immutable request snapshot matches.

The real PostgreSQL recovery test is:

```powershell
$env:ROCKETMQ_SRE_TEST_DATABASE_URL = `
  'postgres://rocketmq_sre:rocketmq_sre@127.0.0.1:5432/rocketmq_sre'
cargo +1.95.0 test --locked -p rocketmq-sre-executor `
  --test postgres_recovery -- --ignored --nocapture
```

It creates and drops an isolated PostgreSQL schema and does not mutate the
normal development schema.

## Development cleanup

Stop services while retaining PostgreSQL data:

```powershell
docker compose -f deploy/dev/compose.yaml down
```

To intentionally remove the complete development stack and its named volumes:

```powershell
docker compose -f deploy/dev/compose.yaml down --volumes
```

The second command is destructive for the Compose development database. It
does not touch host PostgreSQL installations or source files.
