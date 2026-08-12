# Message-path release qualification

The message-path qualification harness verifies producer send, Broker persistence, and LitePull consumption on an explicitly confirmed target. It records machine-readable latency and throughput evidence without changing RocketMQ wire or storage formats.

The compatibility contract is business behavior: the Rust implementation must preserve the message semantics and externally visible results expected by Java clients while remaining free to use Rust-native scheduling, ownership, batching, and backpressure. The project has one implementation path and one set of effective configuration values. Durability is selected through the existing flush, replica acknowledgement, and clean-election settings and is recorded as an explicit contract string in every run.

DLedger is intentionally outside the Rust product scope. The qualification matrix covers Controller-based high availability and does not reserve a DLedger migration or compatibility gate.

## Safety boundary

The runner never discovers or guesses a target. Both `--namesrv` and an identical `--confirm-target` are required. Commands are executed as argument vectors without a shell, reports exclude credentials and message bodies, and an existing run directory is never overwritten. Release mode additionally requires a clean Git worktree and validated performance-comparison, fault-matrix, and six-hour soak evidence.

Validate the committed policy and inspect the concrete commands without connecting to a Broker:

```powershell
python scripts/message_path_qualification.py validate-policy
python scripts/message_path_qualification.py plan `
  --mode smoke `
  --namesrv 127.0.0.1:19876 `
  --confirm-target 127.0.0.1:19876 `
  --topic QualificationTopic `
  --durability-contract async-flush-single-replica
```

## Managed local smoke

The existing functional harness can start a local Rust NameServer and Broker and then run the four bounded smoke workloads:

```powershell
.\scripts\run_client_broker_functional_tests.ps1 `
  -SkipBaseGates `
  -SkipRustClientJavaBroker `
  -SkipJavaClientRustBroker `
  -SkipMixedMatrix `
  -RunMessagePathQualification
```

Smoke mode verifies that all messages complete without send or response failures and records sync, async, batch, and LitePull metrics. A passing smoke report deliberately has `release_qualified=false`.

## Release evidence

Release mode uses the broader payload and batch matrix from `scripts/message-path-qualification-policy.json`, at least five repetitions per workload, and one warm-up run. It accepts only:

- a performance comparison whose `status` is `pass`;
- a dynamic Kubernetes fault-matrix evidence directory accepted by `scripts/fault_matrix_guard.py`;
- a six-hour soak report with no detected monotonic resource growth and the same durability contract.

```powershell
python scripts/message_path_qualification.py run `
  --mode release `
  --namesrv 10.0.0.15:9876 `
  --confirm-target 10.0.0.15:9876 `
  --topic ReleaseQualification `
  --durability-contract sync-flush-required-replica-acks `
  --performance-comparison target/evidence/performance-comparison.json `
  --fault-evidence target/evidence/fault-matrix/run-001 `
  --soak-report target/evidence/soak-report.json
```

The resulting `qualification-report.json` binds the Git commit, hardware fingerprint, confirmed endpoint, workload parameters, raw artifact hashes, median throughput, median payload bandwidth, and median p99 latency. It fails closed when any message is missing, any response fails, evidence is absent, the worktree is dirty, or the target confirmation differs.

Compare two reports only when they used the same policy, hardware, business contract, durability contract, and workload parameters:

```powershell
python scripts/message_path_qualification.py compare `
  --baseline target/evidence/baseline/qualification-report.json `
  --candidate target/evidence/candidate/qualification-report.json `
  --output target/evidence/message-path-comparison.json
```

The default gates reject throughput regression above 10% and p99 latency regression above 15%. Estimates and broker-free microbenchmarks are supporting analysis only; they are not accepted as release qualification.
