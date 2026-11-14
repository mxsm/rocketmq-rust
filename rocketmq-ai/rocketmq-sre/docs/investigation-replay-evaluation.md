# Paired investigation replay

The `investigation-replay` evaluator compares an explicitly chosen single-pack
baseline with the same case's selected multi-pack investigation. Both arms use
the production `plan_investigation`, Run admission/settlement, exact-version
`DiagnosticEngine`, saved run revision recording, and conservative final
aggregation. The runtime and evaluator share that aggregation in Core. A flat
loop over independent diagnostic packs is not used as a multi-pack run.

This is local replay of immutable, redacted Evidence. It opens no connector,
model-provider, SQL, shell, or target connection and grants no execution
eligibility. Fixture callbacks are counted when invoked after an accepted
reservation; they are not live target or provider measurements.

## Fixed conditions

`InvestigationReplayCase` is a closed JSON object. Both arms receive its exact
scope, absolute window, clock, deadline, budget, canonical read fixtures and
model policy. `baseline_pack` must be an exact member of `packs`; selection is
never inferred from a title or the registry's current active version.

| Input | Meaning and bound |
| --- | --- |
| `schema_version` | `rocketmq-sre.investigation-replay.v1` |
| `case_id`, `provenance` | Stable case identity and either `synthetic_fixture` or `redacted_capture` with an explicit source reference |
| `scope`, `as_of`, `deadline`, `budget` | The production Run scope, fixed UTC observation clock, and bounded read-only limits |
| `packs`, `baseline_pack` | Explicit exact compiled IDs and semantic versions; at most the Run contract's 16 packs |
| `bindings` | At most 256 exact compiled requirement/source/canonical-resource bindings in the case scope |
| `reads` | At most 256 unique source/resource fixtures, with one to three closed outcomes each |
| `model` | `disabled`, `no_eligible_provider`, `invoked_failure`, or `outcome_unknown` |
| `checkpoint` | `none` or `after_first_collection` |
| `refinement` | `none` or the explicit synthetic `reassess_same_facts_once` control input |

The input file is bounded to 16 MiB. Evidence must be inline, content-hash-valid,
and match its exact tenant, cluster, source, resource and requested window.
Conflicting uses of one Evidence ID are rejected. No arbitrary resource URL or
query expression is accepted. Captured bindings are evaluation input, not proof
that the production service authorized them; production continues to derive its
own bindings from authenticated inventory and saved-reference provenance.

The read callback returns saved Evidence, a known retryable unavailability, a
known permanent refusal, or an unknown outcome. An exhausted fixture sequence
repeats its final outcome. Repetition still requires a fresh admission and is
subject to the shared two-retry and twelve-read caps. Successful reads are not
reissued to repair missing fields. Identical source/resource/window reads share
one collection across consuming packs.

## Results and accounting

The report contains actual typed Run snapshots, saved rule revisions, citations,
current gaps, stop reasons and per-source callback counts. It omits inline
Evidence bodies. Null fields and unsupported/unknown temporal contracts retain
their quality; they do not become zero or healthy. Required evidence and optional
Evidence actually cited by a finding are requalified before a strong conclusion.
Independent counterevidence and competing faults remain visible and prevent the
conservative strong-conclusion gate from claiming a unique root cause.

Accounting fields have distinct meanings:

- `read_callbacks` and `model_callbacks` count closed in-process fixture calls.
  `model_invocations` counts the fixture provider branch actually reached, so
  no eligible provider has zero invocations even though the adapter was called.
- `known_reported_cost` sums only actual costs in known StepResults. Successful
  and known-error reads charge the complete wire-shaped
  `ConnectorResponseEnvelope`, using the runtime's 512 KiB response reservation.
  Evaluation output is bounded to 64 KiB before acceptance.
- `settled` is production admission accounting: known read/model attempts retain
  their admitted call charge even when a known result reports no invocation.
- `reserved` preserves the full unmeasured reservation of an interrupted attempt.
  Unknown is not reported as a measured zero and does not trigger automatic retry.

A known unavailable model does not replace saved deterministic findings. An
unknown model invocation retains its reservation while actual saved rules can
still finalize, as in the runtime. Model fixtures cannot change packs, scope,
rule conclusions, citations or limits.

`serialized_checkpoint_restored` means the evaluator serialized, hydrated and
validated its Run, saved revisions, Evidence and callback ledger after a completed
collection. Successful work is reused. This is **not a PostgreSQL or process
restart test**. Normal-process restart and persistent-event recovery require
separate deployment/test evidence.

## Separate adjudication

Run replay first, then supply annotations independently. With no annotations,
both results are `unadjudicated`; correctness is never inferred from successful
execution. `adjudicate_pair` binds each annotation to the case ID, arm, canonical
complete-case digest and exact observation: scope, time, versions, Evidence IDs,
findings and conclusion. Changing payloads, budget, failure sequence or model
policy invalidates an old annotation even if the final conclusion happens to be
identical. Duplicate or conflicting annotations are rejected.

The closed verdicts are `correct`, `wrong`, and `reasonable_inconclusive`. The
last cannot label a strong conclusion. Every supplied verdict includes rationale
and either `synthetic_fixture` provenance or caller-supplied adjudicator and source
references. External references are recorded as supplied; this tool does not
authenticate a human reviewer or independently verify their judgment. Synthetic
maintenance expectations remain synthetic and are not production qualification.

Actual independent SRE adjudication, real provider behavior, deployed-source
coverage and real process-restart evidence remain separate gates. No operator
annotations or target verification are invented by the fixture suite.

## Running and maintaining the evaluator

Run commands inside the task's authorized build container:

Reports require a new output path. Existing files, symlinks and hard links are
rejected, so an output alias cannot overwrite an immutable case or annotation.

```bash
cargo run --locked -p rocketmq-sre-eval --bin investigation-replay -- \
  --case /tmp/fixed-case.json --output /tmp/paired-report.json

cargo run --locked -p rocketmq-sre-eval --bin investigation-replay -- \
  --case /tmp/fixed-case.json --annotations /tmp/supplied-annotations.json \
  --output /tmp/adjudicated-report.json

cargo test --locked -p rocketmq-sre-eval --lib investigation_replay
cargo test --locked -p rocketmq-sre-core --test investigation_planner
cargo test --locked -p rocketmq-sre-control-plane --lib \
  workflow::investigation_run_service::dispatcher::finalize
```

Permanent regression fixtures use real compiled Store, ConsumerLag and
BrokerHealth field contracts. They exercise shared reads, counterevidence,
competing causes, null/missing inputs, source failures, model failure modes,
budget/no-progress stops, temporal qualification, checkpoint hydration and
annotation binding. Their provenance is synthetic; passing them is not a real
target acceptance record. Keep generated reports and temporary input captures
outside the repository.
