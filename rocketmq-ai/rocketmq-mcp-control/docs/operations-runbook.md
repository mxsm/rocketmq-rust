# RocketMQ MCP Control operations runbook

## Prepare and enable

1. Build the standalone project with `write-tools`; the default build intentionally has no Admin dependency or
   mutation tools.
2. Configure TLS and the canonical HTTPS public origin, remote RS256 OAuth issuer/JWKS/audience, and the durable
   JSONL audit path.
3. Add a private cluster-registry entry for each logical cluster. Reference credentials only through protected
   environment-variable names; inline credentials are rejected.
4. Configure `mutations_enabled=true` and allowlist only `topic_upsert`, `consumer_group_upsert`, or both, plus
   their logical clusters. The identity provider must issue matching closed claims and `rocketmq:write`.
5. Restart the process. Configuration and emergency-disable changes are startup-time controls and do not hot
   reload.

Verify authenticated `tools/list`: a principal can see zero, one, or two tools according to the intersection.
The capability Resource must report compile/runtime/registered state consistent with that catalog. Resource
templates and prompts remain empty.

## Safe operating sequence

Start with dry-run and inspect the operation-specific top-level `target`, aggregate `before` and `requested`,
then the broker-sorted per-target evidence. For execution, preserve the same complete replacement and target
set, set `dry_run=false` and `confirm=true`, and provide a safe operator reason. Inspect aggregate `after` as
well as each target's persistence/verification evidence. Use a request key when retry or concurrent delivery is
possible. Do not change the payload for a reused key.

The server persists `started` before opening an Admin session. One lifecycle-owned supervisor then opens one
mutation-only session, performs targeted preflight, optionally executes the sealed conditional plan, performs a
targeted post-read, awaits exactly one bounded shutdown, and persists a terminal record. A caller disconnect,
timeout, cancellation, or contained adapter panic does not abandon an acquired session.

Interpret results conservatively:

- `planned` and `applied` are non-error statuses; unchanged success is `applied` with `changed=false`;
- `conflict` means the conditional expected state no longer matched and is not retried;
- `partial` retains per-target applied/persistence/verification truth and requires operator review;
- `failed` may still contain an applied target whose persistence or post-read verification failed;
- `order_reconciliation_failed` means the sealed NameServer-wide order value changed after broker CAS; the
  targeted path never tries to repair that global value and preserves the broker-applied truth.

Never infer success from HTTP delivery alone; consume the closed structured result and `isError` value.

## Failure and emergency handling

If audit `started` is unavailable, the operation opens no session and performs no RocketMQ RPC. Treat audit
poison, shutdown failure, persistence failure, or verification failure as fail-closed and investigate the
durable audit plus RocketMQ state using separately authorized operational systems. Audit records deliberately
contain no credentials, endpoints, principals, request reasons/keys, or backend error text.

To stop new mutations, set `mutations_enabled=false` and restart. Removing an operation or cluster from the
server allowlist also requires restart. Keep the service unavailable until incomplete or partial targets are
reconciled explicitly; the control server never retries conflicts or invents compensating mutations.
