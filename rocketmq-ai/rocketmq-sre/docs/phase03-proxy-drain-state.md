# Phase 03 Proxy DrainState

`proxy.restart_one.v1` may only restart one Proxy after the target process proves
that it has stopped admission, left readiness and routing, and has no pending
work. The primitive is deliberately separate from the irreversible process
shutdown lifecycle so a timeout can restore service.

## Wire contract

| Request code | Name | Required cluster action | Purpose |
| ---: | --- | --- | --- |
| `331` | `GetProxyDrainState` | `Get` | Query the bounded state and exact counters |
| `332` | `BeginProxyDrain` | `Update` | Atomically stop admission and suspend readiness |
| `333` | `CancelProxyDrain` | `Update` | Restore admission/readiness after timeout |

Begin and cancel use `rocketmq.proxy-drain.v1` with an operation ID. Unknown
request codes are rejected by old Proxy versions, so a caller cannot mistake an
unsupported Proxy for a drained Proxy. Management requests require an
authenticated principal even when general Proxy authentication is disabled;
authorization is evaluated against the configured cluster resource.

The state query returns:

- phase: `accepting`, `draining`, or `drained`;
- admission, routing, and readiness flags;
- active logical connections, sessions, receipt handles, prepared
  transactions, telemetry links, Remoting channels, pending telemetry commands,
  and RPCs in flight;
- `zeroPending`, which is true only when every counter is exactly zero.

`draining` changes to `drained` only after an exact zero snapshot. Missing,
unknown, malformed, or internally inconsistent state fails closed in the client
and Admin Core adapters.

## Restart sequence

1. Query the target and require `accepting`.
2. Begin with the execution operation ID.
3. Confirm Kubernetes readiness is removed.
4. Poll the state until `drained` and `zeroPending=true`.
5. Restart exactly one pod with `max_parallelism=1`.
6. Wait for the replacement pod to become ready and verify the action SLIs.
7. If drain times out before restart, cancel using the same operation ID.
8. If restart or re-admission verification fails, stop automation and require
   manual intervention.

The operation ID is idempotent. A different operation ID conflicts while a
drain is active. A real shutdown request remains irreversible and prevents a
cancel operation from republishing readiness.

## Phase 03 boundary

The primitive does not delete session, receipt, or transaction state to make
the counters appear empty. It does not close arbitrary client connections and
does not restart a workload. Those steps belong to the typed
`proxy.restart_one.v1` handler and its Kubernetes driver. Until that handler
completes the end-to-end precheck, drain, restart, re-admission, and verification
flow, the action descriptor remains `execution_supported=false`.
