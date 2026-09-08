# Configuration activation and Proxy dependency health

This contract covers the configuration changed by W01–W16. Existing configuration
transactions and component owners remain authoritative. Restart-required values
are frozen when their owner is constructed; editing a configuration file does not
replace a running pool, compiler registry, provider, or health policy.

## Activation

JSON/TOML property names below use their public camelCase spelling. Rust builder
ports have no serialized property. Defaults describe the built-in configuration;
an embedding application can supply an explicitly validated configuration.

| Field or port | Default | Compile feature / runtime activation | Activation | Validation and effective observation |
| --- | --- | --- | --- | --- |
| `grpc.maxSendMessagesPerRequest` | 1024 | Built-in gRPC / all send requests | RestartRequired | Nonzero; `ProxyRuntime::config().grpc`; request-count rejection tests |
| `grpc.maxDecompressedRequestBytes` | 8388608 bytes | Built-in gRPC / identity and gzip bodies | RestartRequired | Nonzero and at least the per-message limit; checked arena/input budget; effective Proxy configuration |
| `grpc.maxMessageBodySize` | 4194304 bytes | Built-in gRPC / each body | RestartRequired | Nonzero, checked total arena capacity; effective Proxy configuration |
| `grpc.gzipDecodeSlots` | 2 | Built-in gRPC / positive slot count enables gzip | RestartRequired | Zero explicitly disables gzip; arena admission and retained-owner tests; effective Proxy configuration |
| `local.commandQueueCapacity`, `local.commandQueueMaxBytes`, `local.controlReserve`, `local.ioMaxInflight` | 1024, 16777216 bytes, 2, 16 | `local-mode` / Local backend | RestartRequired | Count/byte reserve must leave data capacity; Local budget and control-progress tests; effective Proxy configuration |
| Broker `with_filter_registry` builder port | Frozen SQL92 registry | Built-in Filter / Broker composition | RestartRequired | Required SQL92, duplicate/empty-name rejection; `FilterRegistrySnapshot::id()` and `CompiledFilter::registry_id()` identify the same retained instance |
| Auth `with_provider_bundle` / `with_provider_registry` builder ports; authentication and authorization provider names | Built-in providers and local metadata | Built-in Auth / configured authentication and authorization | RestartRequired | Provider names/capabilities validated before initialize; startup rollback and isolation tests; `AuthRuntime::provider_registry()` retains the actual instance |
| ACL snapshot reload and whitelist update | Existing Auth policy | Auth / explicitly configured import/watch capability | Live | Failed validation/import preserves the published generation; `AuthRuntime::acl_generation()`; no provider replacement |
| `autoCreateTopicEnable` | true | Broker / every newly admitted send plan | Live | Existing closed patch list and boolean validation; Broker response `configGeneration` and config-read generation |
| `autoCreateSubscriptionGroup` | true | Broker / subscription policy | Live | Existing closed patch list; committed Broker generation |
| `brokerPermission` | 6 (read and write) | Broker / admission and pre-append permission recheck | Live | Canonical permission validation; committed Broker generation; permission revocation regression |
| `defaultTopicQueueNums` | 8 | Broker / topic creation | Live | Existing queue-count bounds; committed Broker generation |
| `messageIndexEnable` | true | Broker/Store / index dispatcher and recovery constraints | Live | Requires the actual runtime projection and safe persisted index progress; committed Broker generation |
| `traceTopicEnable` | false | Broker / topic policy | Live | Existing boolean validation and committed Broker generation |
| `dependencyHealth.intervalMs` | 5000 | Selected Proxy backend / always checked | RestartRequired | Positive, checked clock arithmetic; `ProxyRuntime::config().dependency_health` |
| `dependencyHealth.timeoutMs` | 3000 | Selected Proxy backend / each complete probe | RestartRequired | Positive and no greater than stale interval; effective health policy |
| `dependencyHealth.staleAfterMs` | 15000 | Selected Proxy backend / last successful evidence | RestartRequired | At least interval and timeout; monotonic freshness tests; health snapshot timestamps |
| `dependencyHealth.failureThreshold` | 3 | Selected Proxy backend / consecutive failed probes | RestartRequired | Nonzero; hysteresis tests and snapshot counters |
| `dependencyHealth.recoveryThreshold` | 2 | Selected Proxy backend / recovery after degradation | RestartRequired | Nonzero; recovery hysteresis tests |
| `dependencyHealth.jitterPercent` | 10 | Selected Proxy backend / interval jitter | RestartRequired | 0–50; bounded jitter tests |

Frozen owners have instance identity rather than a fictional changing configuration
generation. The health observation handle is scoped to one `ProxyRuntime`; it
does not keep that runtime or its poller alive. Configuration getters are in-process
APIs, not permission to serialize an entire configuration into diagnostics.

NameServer continues to report `desiredGeneration`, `durableGeneration`,
`effectiveGeneration`, `appliedKeys`, and `restartRequiredKeys`. It persists the
validated desired snapshot before publishing live values. Persistence failure
publishes neither new live values nor a new effective generation. A restart-only
update can be durable without being applied.

Broker runtime patch success reports its existing `configGeneration` plus additive
`applied=true` and `persisted=false` extension fields. The runtime transaction
does not write the Broker startup configuration. Topic metadata reconciliation
does not change that fact. Clients requiring restart durability must update their
deployment configuration as well. Existing response codes, headers, and success
remarks remain compatible; older Java clients can ignore these extension fields.

Known Broker properties outside the six reviewed live keys fail with
RestartRequired; unknown keys remain unsupported. Selecting Proxy Local/Cluster
without its compiled feature fails explicitly. Store backend feature checks and
the Telemetry resolver retain their existing fail-closed behavior; enabling a
Cargo feature alone does not activate a configured backend or exporter. Public
configuration/error reporting must retain credential presence or safe state only,
never credential contents, signing material, complete provider configuration, or
raw probe response bodies.

## Continuous health

All required listeners must bind and the initial backend check must succeed before
readiness can become true. Initial failure or timeout enters common startup
finalization. Thereafter one owned task probes at the configured interval, with
jitter and no overlapping checks or catch-up burst. A brief failure retains
readiness until the failure threshold or freshness deadline is reached. Recovery
from degradation requires consecutive successes; initial startup requires one.

`ProxyRuntime::dependency_health().snapshot()` reports mode, state, a fixed reason,
last check/success timestamps, and consecutive counts. Timestamps are Unix
milliseconds for observation; freshness and deadlines use a monotonic clock.
No resource names, credentials, response bodies, or arbitrary error strings enter
the snapshot.

- **Local:** the bounded control lane checks that embedded Broker startup completed,
  its runtime is not shutting down, and the actual Store reports writable and not
  shut down. A missing/unstarted backend cannot supply an empty successful check.
- **Cluster:** bypass the local route cache, fetch fresh NameServer cluster metadata,
  select a Broker from the configured cluster (prefer a master), and perform one
  authenticated read-only `GET_BROKER_RUNTIME_INFO` request. Both reads share a
  bounded deadline. A successful, decodable `KVTable` is required. This establishes
  a usable metadata path to the selected Broker; it does not certify every Broker,
  quorum replication, or end-to-end write success. The probe account needs this
  metadata permission and does not need message-send permission.

The request and response follow the existing
[Java MQClientAPIImpl contract](https://github.com/apache/rocketmq/blob/develop/client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java).
No write messages, topics, groups, or ACL changes are generated by the probe.
Cluster command ownership retains its normal admission, cancellation, and timeout
boundaries. Custom messaging processors must supply their actual metadata port
through `with_metadata_service` before serving.

The dependency gate is separate from listener startup, maintenance suspension,
and shutdown. A recovered dependency cannot restore readiness during maintenance
or draining. Dependency failure does not stop the liveness heartbeat. Shutdown
cancels and awaits the poller before stopping backend and Auth owners, using the
same absolute deadline and retaining primary and cleanup failures. Observation
handles finish in Stopped and cannot restart work.

The default policy is an initial production profile. Docker fault and capacity
exercises in W18 calibrate it; the values are not an availability SLA.
