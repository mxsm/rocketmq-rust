# Phase 03 Change Executor and Execution Agent fencing

P3-05 separates orchestration from the only target-write boundary:

```text
Operator
  -> Control Plane (plan, policy, approval, signing key, Lease Authority)
  -> Change Executor (journal, lock, precheck, reconcile; no target credentials)
  -> Execution Agent (durable fence/effect ledger and typed drivers)
  -> RocketMQ Admin / Kubernetes / configuration system
```

## Workload identities and network reachability

| Caller | Callee | Required identity | Additional binding |
| --- | --- | --- | --- |
| Control Plane | Executor | `spiffe://rocketmq-sre/control-plane` | Executor-only bearer token |
| Executor | Execution Agent | `spiffe://rocketmq-sre/executor` | Agent-only bearer token |
| Executor | Lease Authority | `executor_service` | tenant and exact cluster scope |
| Execution Agent | Lease Authority | `execution_agent` | tenant and exact cluster scope |

Executor has no target network and no Admin/Kubernetes/configuration
credentials. Execution Agent has the target network, a dedicated ServiceAccount
and only the typed RBAC permissions required by registered handlers. Neither
service accepts user tokens directly.

Development mode explicitly permits HTTP and header-projected fixture
identities. Production configuration rejects plaintext internal URLs and
requires transport-authenticated workload identity.

## Two-phase lease handoff

1. Lease Authority serializes cluster takeover and creates
   `PendingFence(epoch=N+1)` with a unique pending nonce.
2. It signs only a read-only `ReconcileGrant`; no dispatch grant exists while
   the lease is pending.
3. Executor asks Agent to reconcile every old non-terminal effect by
   operation ID, idempotency key, and live state. `Unknown` blocks handoff.
4. Agent `AdvanceFence` acquires a PostgreSQL exclusive advisory lock for the
   cluster. Existing dispatches retain the shared side until their driver
   result is durable.
5. Under the exclusive barrier Agent confirms there are no old
   `Prepared`/`Dispatched`/`Unknown` effects, persists its highest epoch and
   returns a signed `FenceAck` bound to cluster, epoch, pending nonce, Agent
   identity and acknowledgement time.
6. Lease Authority verifies the Agent signature and exact persisted fence
   snapshot, then atomically changes the lease to `Active`.
7. Only the active owner can obtain a short-lived
   execution/step/action/resource-bound `LeaseFenceGrant`.

Starting a new pending epoch expires the previous lease. Old requests that
already passed final verification may finish only while holding the shared
barrier; the new Agent acknowledgement cannot occur first. Requests arriving
after the new epoch is durably accepted fail the Agent highest-epoch check,
even if an isolated old Executor can still reach the Agent.

## Effect ledger and recovery

```text
Prepared -> Dispatched -> Confirmed
                    \-> Unknown -> read-only reconcile -> Confirmed
```

The Agent stores the complete bounded typed request before dispatch, then stores
the target operation ID before calling the driver. A database failure prevents
the call. Timeouts and malformed results become `Unknown`; retrying the same
idempotency key cannot call the driver again.

Executor stores `StepIntent` and audit in one transaction before dispatch and
appends `StepResult` afterwards. On restart it finds unresolved intents, reads
the Agent effect/live state and moves through `Unknown -> Reconciling`; it
never assumes that a timed-out target write did not happen.

## Failure behavior

| Failure | Result |
| --- | --- |
| Missing/invalid approval or request signature | Reject before journal/dispatch |
| Descriptor, risk, quarantine or precondition drift | Reject locally |
| Lease Authority unavailable | No new Agent write |
| Agent/PostgreSQL unavailable before `Dispatched` | No target write |
| Driver timeout or connection loss after `Dispatched` | Durable `Unknown`, no blind retry |
| Old non-terminal effect during takeover | No `FenceAck`, manual/read-only reconciliation required |
| Old epoch request after new `FenceAck` | Agent rejects it |
| Unknown or R3 action | No registry handler and no reachable driver |

## Verification

The Docker PostgreSQL suite includes:

- request/journal idempotency, resource lock and quarantine persistence;
- Executor and Agent restart recovery;
- forged `FenceAck`, missing durable Agent acknowledgement, wrong workload
  role and wrong cluster-scope rejection;
- a deterministic N/N+1 interleave proving the exclusive fence waits for the
  in-flight shared dispatch and no N write occurs after the acknowledgement;
- a simulated post-dispatch crash proving `Dispatched` blocks N+1 until
  read-only reconciliation records a terminal result.

The normal Executor dependency tree is checked separately and must not contain
`rocketmq-admin-core`, `kube`, `rocketmq-sre-execution-agent`, MCP, or the model
gateway. The Agent is the only production SRE graph that contains mutation
adapters.

Run the repeatable boundary and deployment checks from the repository root:

```powershell
python rocketmq-ai/rocketmq-sre/scripts/check_execution_dependency_boundary.py
.\rocketmq-ai\rocketmq-sre\scripts\verify-mtls-deployment.ps1
```
