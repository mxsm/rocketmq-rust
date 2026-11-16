# Execution drill records

The `execution-drill-report` tool combines bounded engineering drill records.
It does not authorize an action, enable a handler, attest a physical target,
or establish production qualification. Normal Control Plane admission retains
its independent approval, target-scope, evidence and qualification checks.

Keep these evidence paths separate:

| Evidence class | Execution path | What the record can describe |
| --- | --- | --- |
| `deterministic_contract` | `deterministic_harness` | A typed invariant exercised with controlled inputs or clocks. |
| `protocol_fault` | `injected_executor_agent` | Executor/Agent coordination using an explicit test authority. A persisted Synthetic effect is not a physical mutation. |
| `isolated_driver` | `direct_handler_adapter` | A real adapter operating on an explicitly owned disposable target. It is not a normal supervised-chain authorization. |
| `normal_admission` | `normal_control_plane` | The actual normal admission boundary, including a zero-write refusal when independent qualification is Unknown. |

A denied normal request cannot substitute for an accepted concurrent-duplicate
positive control. A Kubernetes Pod UID replacement cannot establish that an
actual Proxy restarted, that an SLI recovered, or that its product version was
attested. Report missing paths and source facts explicitly.

## Generate a report

Run from the standalone SRE workspace. Inputs and outputs belong in operator
artifact directories outside the repository:

```sh
cargo run --locked -p rocketmq-sre-eval --bin execution-drill-report -- \
  --input /operator/drills/current-protocol.json \
  --input /operator/drills/current-adapter.json \
  --output /operator/reports/new-execution-drills.json
```

Each input is a `rocketmq-sre.execution-drill-fragment.v1` document with one
run ID, explicit false authority flags and a `cases` array. Use the maintained
drill's observations and original accepted identity; do not invent a successful
case to fill the matrix. Error output contains closed codes without input bodies
or private paths. The output must be a new file: an existing file, including an
input alias, is never overwritten.

A direct adapter can lack SRE tenant/cluster fields in its actual target
protocol. Associate such a record only with an explicitly operator-declared
engineering scope, retain `operator_declared_record_scope`, and leave
`accepted_identity` absent. That association does not prove target membership.
Protocol-path records instead retain scope from the original immutable request.

Inputs are limited to 32 regular files, 1 MiB per file and 8 MiB in aggregate.
There are at most 128 cases, 32 bounded artifact references per case and 1 MiB
of encoded report output. The parser rejects duplicate JSON keys, unknown
fields, nil scope/accepted IDs, unsupported action versions, conflicting case
identities, invalid decimal epochs, oversized fields and contradictory counters.
References are opaque safe names, not paths, URLs or secret references.

The report lists all nine wired exact `1.0.0` action descriptors across four
evidence classes. Its 36 coverage rows count recorded **case claims**; they do
not mean 36 tests ran, all requested cells are complete, or physical truth has
been validated. Classes and different observed product versions remain distinct.

## Preserve identity and interpret outcomes

An accepted protocol record binds the original execution, correlation, Plan,
Plan step, Plan hash, selector digest and, when observed, lease epoch. A lost
response, cancelled observer or disconnected target must retain that identity.
It must not create a new idempotency key or infer that an unobserved effect did
not happen. An Unknown verification source cannot claim a completed stable
window or trigger a new compensation write.

Passed scenarios have their own checks. Concurrent original requests require
one accepted identity and one recorded forward effect. Expired approval/fence,
freeze and stale-target refusals require their intended guard to be reached and
zero forward/compensation effects. Known-false verification and failed
compensation use their explicitly permitted effect bounds; a failed or Unknown
compensation is never described as restored. These consistency checks do not
prove that an arbitrary input file truthfully describes a physical event.

`target_artifact_identity` describes the executed binary, deployed image or
deterministic fixture. `observed_software_version` separately records an actual
observation and its source. A controlled binary self-report applies only to a
controlled child binary; a deployed binary self-report applies only to a
deployed image. A tag, Cargo manifest version or operator declaration cannot
replace either observation. Missing product version, physical attestation,
exact SLI and independent business recovery remain explicit limitations.

Private configuration, credentials, Secret references, target addresses and
raw Agent responses do not belong in these safe records. Keep raw evidence in
the protected operator store and use bounded opaque references in reports.
Cleanup observations describe the exact target changes inspected or restored;
they do not establish that a timed-out remote operation cannot finish later.

## Maintained live-target gates

The opt-in tests below require an explicitly owned disposable environment.
Read each test's settings before enabling it. Supply private credentials and
CA-verified endpoints through the existing test environment, and use a new
private report file where required. Running `--list`, compiling with
`--no-run`, or selecting zero tests does not establish a live pass.

| Target | Maintained test source | Observation boundary |
| --- | --- | --- |
| Broker logger and runtime CAS | [Broker driver tests](../crates/rocketmq-sre-execution-agent/src/drivers/production_broker_config_tests.rs) and the logger-level child tests | Actual authenticated apply/readback/restore and stale-generation refusal. Explicit restore does not establish natural TTL expiry. |
| Topic and subscription group | [Topic tests](../crates/rocketmq-sre-execution-agent/src/drivers/production_topic_config_tests.rs) and [group tests](../crates/rocketmq-sre-execution-agent/src/drivers/production_subscription_group_e2e_tests.rs) | Exact selected resources, observed version CAS and inverse readback. Missing retry routing is a fixture prerequisite failure. |
| Proxy replacement with a missing exact SLI source | [Real Unknown/manual test](../crates/rocketmq-sre-execution-agent/src/drivers/production_proxy_restart_tests/live_unknown.rs) | One actual Proxy UID replacement; retained original operation; no second deletion on retry; irreversible UID restoration requires manual takeover. Operator annotation cleanup is separate from successful compensation. |
| Collector | [Real Collector test](../crates/rocketmq-sre-execution-agent/src/drivers/production_telemetry_collector_restart_tests/live.rs) | Exact running image and Pod identity, rollout binding and template restoration. Observe an actual pipeline separately; Pod readiness alone is insufficient. |

For example, from this standalone workspace, after supplying the explicit
settings required by the real Proxy fixture:

```sh
cargo test --locked -p rocketmq-sre-execution-agent \
  --features rocketmq-admin-core/tls --lib \
  drivers::production_proxy_restart::tests::live_unknown::real_proxy_restart_preserves_unknown_and_manual_takeover_after_one_replacement \
  -- --ignored --exact --test-threads=1
```

The real restart test uses the normal Control Plane's incomplete source; it
does not supply a successful scripted SLI. Its successful test result means
the expected Unknown/manual behavior held, not that restart verification or
business recovery succeeded. It is a direct-adapter observation without a
normally admitted SRE execution identity.

Keep the two main Proxy identities and actual image readbacks when exercising
the existing scale and canary tests. A packaging-only image variant cannot
qualify an application software upgrade. Capture complete process logs and
owned exit results before deleting task resources. A healthy main process
exit does not erase a distinct session warning observed earlier in its life.

### Secure Proxy traffic

The [secure messaging tests](../../../rocketmq-proxy/tests/secure_cluster_traffic.rs)
belong to the main repository workspace. They use only public messaging RPCs,
explicit verified TLS and the fixed disposable probe topic/group; bootstrap
those resources separately with the fixture owner's normal authorized tools.

Keep original-message recovery separate from a new probe. The ignored
`recover_original_secure_proxy_messages_without_sending` test requires
`ROCKETMQ_PROXY_LIVE_RECOVER_ORIGINAL=1`, the original run ID and a bounded
read-only `ROCKETMQ_PROXY_LIVE_ORIGINAL_REPORT`. It sends no messages. If the
original report did not retain response IDs, expected IDs must remain labelled
as derived from the original deterministic requests. Recovery matches actual
ID, body, marker and tag before one ACK per identity; an unknown result stops
the run.

The ignored `real_secure_proxy_pods_and_service_send_receive_and_ack_exact_messages`
test additionally requires `ROCKETMQ_PROXY_LIVE_FRESH_PROBE=1` and a new run ID
different from that original report. It checks both actual Pod-IP endpoints
and their Service, with one consistent run tag. A send/ACK failure stops later
endpoints instead of creating more unresolved messages. Neither mode reports
message contents, receipt handles, P99, business recovery or qualification.

For example, from the main repository root after supplying the selected mode's
explicit private settings:

```sh
cargo test --locked -p rocketmq-proxy --no-default-features \
  --features cluster-mode,tls --test secure_cluster_traffic \
  recovery::recover_original_secure_proxy_messages_without_sending \
  -- --ignored --exact --test-threads=1
```
