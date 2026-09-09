# rocketmq-auth

[English](README.md) | [简体中文](README-zh_cn.md)

Authentication, ACL authorization, and service-owned auth runtime support for [RocketMQ-Rust](../README.md).

`rocketmq-auth` implements RocketMQ access-key signatures, Java-style ACL file imports, local user and ACL
metadata, and remoting request checks. It also exposes separately composed bootstrap, secret-provider,
credential-rotation, and maintenance-policy adapters. Building an `AuthRuntime` does not install a network
interceptor or enable those adapters automatically.

## Capabilities and boundaries

| Area | Current behavior |
|------|------------------|
| Authentication | Access-key lookup, enabled-user checks, HMAC signature verification, and optional timestamp skew checks. |
| Authorization | User/resource/action/environment policies, super-user authorization bypass, and denial when an evaluated context has no applicable ACL. |
| ACL compatibility | Java-style YAML files, recursive directory loading, global/account IP whitelists, and optional v1 migration. |
| Runtime | Provider initialization, request admission, metadata seeding, ACL reloads, metrics, and coordinated shutdown. |
| Metadata | In-memory users and ACLs with optional JSON snapshots; custom metadata bundles have explicit operation and lifecycle interfaces. |
| Strategies | Separate stateless/stateful evaluators. Stateful caches are not used by the runtime's direct remoting services. |
| gRPC | Authentication metadata parsing and an optional Tonic adapter; no complete gRPC authentication/authorization interceptor. |

## Public API and source layout

Implementation modules are private. Import supported types from the crate root, for example
`rocketmq_auth::AuthConfig`, rather than `rocketmq_auth::config::AuthConfig`.

| Source | Responsibility |
|--------|----------------|
| [Public exports](src/lib.rs), [configuration](src/config.rs) | Supported import paths and Serde configuration defaults. |
| [Runtime](src/runtime.rs), [remoting context](src/remoting_auth_context.rs) | Service lifecycle and trusted ingress facts. |
| [Provider ports](src/provider_ports.rs), [provider owner](src/provider_owner.rs) | Metadata operations, admission, initialization, and cleanup. |
| [Authentication](src/authentication.rs), [authorization](src/authorization.rs) | Context builders, providers, policies, strategies, signing, and client RPC hooks. |
| [ACL](src/acl.rs), [migration](src/migration.rs), [permissions](src/permission.rs) | YAML imports, legacy models, address matching, and permission conversion. |
| [Bootstrap](src/bootstrap.rs), [secret providers](src/secret_provider.rs), [rotation](src/credential_rotation.rs) | Explicit administrator enrollment and credential-management adapters. |
| [Maintenance](src/maintenance.rs), [layered authorization](src/layered_authorization.rs) | Policy loading and adapters for composed security decisions. |
| [Auth metrics](../rocketmq-observability/src/metrics/auth.rs) | Metrics owned by `rocketmq-observability`; `AuthMetrics`, `AuthMetricsSnapshot`, and `AuthMetricSample` are re-exported here. |

Runtime-neutral security contracts belong to `rocketmq-security-api`:

```rust
use rocketmq_security_api::{Principal, Resource};
```

Auth policy models have distinct canonical names:

```rust
use rocketmq_auth::{AuthorizationRequest, PolicyDecision, PolicyResource};
```

The root `rocketmq_auth::Resource` and `RequestContext` aliases remain for compatibility with the policy
models. `SecurityPrincipal` and `SecurityResource` are deprecated aliases; use the security API types directly.
Maintenance contracts also come directly from `rocketmq-security-api`. An ordinary denial is a decision;
`AuthServiceError`, `SecurityContractViolation`, and `SecurityProviderError` represent operational or
contract failures. These Rust API distinctions do not change RocketMQ remoting numeric response codes.

## Dependencies

For a package that is a member of this repository's workspace:

```toml
[dependencies]
rocketmq-auth = { workspace = true }
rocketmq-runtime = { workspace = true }
tokio = { workspace = true }
```

For a standalone package beside `rocketmq-auth` and `rocketmq-runtime`, the quick start uses:

```toml
[dependencies]
rocketmq-auth = { path = "../rocketmq-auth" }
rocketmq-runtime = { path = "../rocketmq-runtime" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Adjust relative paths to your checkout. The API described here follows this source tree; it is not a
guarantee that a published crate with the same workspace version exposes every API shown here.

There are no default features. Enable `grpc` on the `rocketmq-auth` dependency to use
`AuthenticationContextBuilder::build_from_grpc` on `DefaultAuthenticationContextBuilder` with Tonic's
`MetadataMap`; bring the `AuthenticationContextBuilder` trait into scope.
The `HashMap<String, String>` adapter, `build_from_grpc_metadata_map`, is available without that feature.
The default authorization provider's gRPC context method returns an empty vector, so enabling `grpc`
alone does not enforce authorization for gRPC methods.

## Quick start

Create `conf/plain_acl.yml` relative to the process working directory before running the program.
This local example grants Alice publishing access to `TopicA` and subscription access to `GroupA`,
without an IP whitelist bypass. Replace the example secret before using it outside a local test.

```yaml
globalWhiteRemoteAddresses: []
accounts:
  - accessKey: alice
    secretKey: replace-with-a-local-test-secret
    admin: false
    defaultTopicPerm: DENY
    defaultGroupPerm: DENY
    topicPerms:
      - TopicA=PUB
    groupPerms:
      - GroupA=SUB
```

Both authentication and authorization must be enabled explicitly. The builder requires a
`ChildServiceContext` from `rocketmq-runtime` to own its background work:

```rust,no_run
use rocketmq_auth::{AuthConfig, AuthRuntimeBuilder, AuthServiceResult};
use rocketmq_runtime::RuntimeContext;

#[tokio::main]
async fn main() -> AuthServiceResult<()> {
    let service_runtime = RuntimeContext::from_current("auth-example");
    let config = AuthConfig {
        auth_config_path: "store/auth".into(),
        acl_file: "conf/plain_acl.yml".into(),
        authentication_enabled: true,
        authorization_enabled: true,
        acl_file_watch_enabled: true,
        ..AuthConfig::default()
    };

    let runtime = AuthRuntimeBuilder::new(config, service_runtime.service_context("auth"))
        .build()
        .await?;

    println!("auth reload attempts: {}", runtime.metrics_snapshot().acl_reload_attempts);

    runtime.shutdown().await?;
    Ok(())
}
```

This program loads metadata and shuts down; it does not serve requests. A broker/proxy integration
must call `runtime.check_remoting(&auth_context, &command).await?` before dispatching protected requests.
Build `RemotingAuthContext` from trusted transport facts with `from_request(&RemotingRequest)`, or use
`network(source_ip, channel_id)` at the trusted network ingress. Network contexts require a nonempty
source address and channel identity. The embedded path is derived from a trusted broker-proxy caller.

If middleware can rewrite a command's operation code, retain the original code at ingress and use
`check_remoting_for_code`. Shut down the auth runtime before its owning runtime exits: shutdown closes
request admission, drains admitted work, stops the ACL watcher, and flushes/closes providers. An injected
metadata I/O actor remains owned by its caller.

## Configuration

`AuthConfig` accepts camelCase Serde field names and fills omitted fields from `Default`.
Rust struct fields use snake_case. Selected defaults and their scope are:

| Field | Default | Meaning |
|-------|---------|---------|
| `authenticationEnabled`, `authorizationEnabled` | `false` | Independently enable the ordinary runtime checks. |
| `authConfigPath` | empty | Optional local snapshot root; empty means in-memory metadata. |
| `aclFile` | empty | YAML file or directory; directories are searched recursively for `.yml`/`.yaml` files. |
| `aclFileWatchEnabled` | `false` | Start the watcher only when an ACL path is also configured. |
| `aclFileWatchIntervalMillis` | `5000` | Poll interval, clamped to at least 1 ms. |
| `authenticationWhitelist`, `authorizationWhitelist` | empty | Comma-separated decimal remoting request codes; each bypasses only its respective check. |
| `signatureAlgorithm` | `HmacSHA1` | Also accepts `HmacSHA256` and `HmacMD5`; clients must use the matching algorithm. |
| `requestTimestampExpiredMillis` | `0` | Optional timestamp skew window; see the limits below. |
| `authenticationProvider`, `authorizationProvider` | empty | Runtime uses the built-in default providers. Unsupported configured names are rejected. |
| `authenticationMetadataProvider`, `authorizationMetadataProvider` | empty | Runtime constructs local providers. Custom providers are supplied through a `ProviderBundle`. |
| `authenticationStrategy`, `authorizationStrategy` | empty | Factory/evaluator selection, defaulting to stateless; does not select a strategy for `AuthRuntime::check_remoting`. |
| `configName` | empty | Legacy factories cache instances by this name when no runtime registry is injected. |
| `clusterName` | empty | Cluster resource used by the remoting authorization mapping. |
| `initAuthenticationUser`, `innerClientAuthenticationCredentials` | empty | Optional startup super-user seeds, created only when absent; not a credential-rotation mechanism. |
| `migrateAuthFromV1Enabled` | `false` | Import legacy ACLs through the v1 plain permission manager. |
| `aclCacheMaxNum`, `aclCacheExpiredSecond`, `aclCacheRefreshSecond` | `1000`, `600`, `60` | Local ACL lookup-cache capacity, lifetime, and read-through refresh interval. |
| `userCacheMaxNum`, `userCacheExpiredSecond`, `userCacheRefreshSecond` | `1000`, `600`, `60` | Compatibility configuration; the current local user provider does not apply these cache settings. |
| `statefulAuthenticationCacheMaxNum`, `statefulAuthenticationCacheExpiredSecond` | `10000`, `60` | Separate stateful authentication strategy capacity and expiry in seconds. |
| `statefulAuthorizationCacheMaxNum`, `statefulAuthorizationCacheExpiredSecond` | `10000`, `60` | Separate stateful authorization strategy capacity and expiry in seconds. |
| `statefulAuthorizationCacheNegativeEnable` | `false` | Cache denied authorization decisions only when explicitly enabled; errors are not cached. |
| `maintenanceEnabled` | `false` | Opt-in maintenance configuration, validated through `maintenance_policy_reference()`. |
| `maintenancePolicyPath`, `maintenancePolicyVersion`, `maintenancePolicySha256` | empty, `0`, empty | Required policy reference when maintenance is enabled; both ordinary auth flags must also be enabled. |

## Request-check semantics

- The ordinary runtime path validates ingress context, checks global/account IP whitelists, authenticates,
  then authorizes. A matching `globalWhiteRemoteAddresses` entry, or the selected account's
  `whiteRemoteAddress`, bypasses **both authentication and authorization**. Request-code whitelists
  apply independently after that IP check.
- Enabled super users bypass ACL lookup during authorization. They still require authentication unless
  an applicable whitelist or disabled authentication setting bypasses it.
- ACL evaluation prefers matching custom policies and falls back to default policies only when none
  match. Within the selected tier, resource type and pattern specificity take precedence
  (literal before prefix before wildcard, and longer prefixes first); `DENY` wins a specificity tie.
  Deny is not an unconditional override across all policies.
  For concrete requested actions, a policy entry needs to match at least one action, not every action
  in the context. The ordinary authorization service requires every generated context to allow access.
- Default denial applies to evaluated authorization contexts. The remoting builder maps supported
  request codes to resources/actions; unmapped codes can produce no contexts and therefore no ACL
  decision in the ordinary check path. Integrations must review the mapping when exposing new operations.
  Supervised mutation codes explicitly reject missing required contexts.
- With a nonzero timestamp window, a present timestamp must parse and lie within the allowed past/future
  skew. A request with no timestamp is still accepted by this check. There is no nonce or replay cache;
  the setting alone does not provide replay protection. A zero window disables timestamp checks.

The detailed evaluation API returns `Abstain` when authorization is disabled. Resolve it using the
explicit layer requirement; errors remain failures rather than abstentions.
`authenticate_maintenance_principal` requires authentication to be enabled and verifies credentials
without the ordinary whitelist shortcuts. It returns an identity for a separate maintenance policy;
it does not itself grant permission to perform maintenance.

## Persistence, reloads, and caches

Local providers persist `users.json` and `acls.json` when `authConfigPath` is set. A path with an extension
is normalized by removing that extension before appending these names; prefer an explicit directory
such as `store/auth`. User snapshots include the secret needed for HMAC verification in plaintext.
Debug redaction does not encrypt JSON or the ACL YAML file.

The configured ACL is loaded during startup. The watcher compares file paths and contents and skips
unchanged inputs; `reload_acl_file().await` forces a reload and returns the imported account count.
A successful import replaces the in-memory address whitelist, advances the shared ACL generation, and
removes accounts that were managed by an earlier file import in that runtime but are absent from the new one.

Read, parse, and validation failures happen before import and leave the imported metadata unchanged.
The local importer then updates users and ACLs sequentially: it has **no transaction or rollback across
the entire import**. A metadata/persistence failure during import can leave partial updates. The
whitelist and generation are published only after import succeeds. Watcher failures are recorded and
retried on later polls; this is not a guarantee of an atomic snapshot switch for concurrent requests.

For custom storage, inject a `ProviderBundle` with `UserMetadataPort`, `AclMetadataPort`, and
`ProviderControl` implementations. Configured ACL imports and v1 migration also require
`AclSnapshotImport`; the builder rejects a bundle missing that capability before startup mutations.

Stateful evaluators require explicit composition. Authentication caches success and failure by
generation, channel, and username; a cache hit skips signature/timestamp revalidation for that request.
Authorization keys also include subject, resource, actions, and source IP, and cache allow decisions
by default. Bind evaluators to the runtime's `ProviderRegistry` or shared generation counter so ACL
reloads invalidate their entries. Independent strategies do not automatically observe another runtime's reloads.

The local ACL lookup cache is separate from those decision caches. Its refresh reads the provider's
in-memory storage, not external edits to `acls.json`; YAML polling is the file-reload mechanism.
Runtime metrics cover the components using its metrics handle. A standalone strategy's default metrics
are separate and are not automatically aggregated into `runtime.metrics_snapshot()`.

## Separately composed security adapters

These APIs require explicit integration by the owning service; none is automatically wired by
`AuthRuntimeBuilder`.

| API | Integration boundary |
|-----|----------------------|
| `OneTimeBootstrap` | Validates a scoped, expiring proof and trusted TLS attestation, persists a claim before invoking `BootstrapAdminProvisioner`, and prevents reuse across restarts. The transport must verify TLS; a failed provisioning attempt leaves the claim closed. |
| `SecretProviderRegistry` | Resolves explicitly registered provider IDs. `EnvironmentSecretProvider` reads only mapped environment variables and is read-only. |
| `EncryptedFileSecretProvider` | Local AES-256-GCM development adapter with versioned files and restrictive Unix permissions. It does not encrypt the ordinary user/ACL snapshots. |
| `CredentialRotationManager` | Validated credential snapshots, overlap/finalization, rollback, and bounded break-glass access with an audit sink. The injected parser owns certificate/key/proof validation; this is not automatic access-key rotation or TLS listener reconfiguration. |
| `MaintenancePolicyReference::load_from` | Loads and validates JSON using an explicit path, version, and SHA-256 pin. The security API owns maintenance policy evaluation; the service must compose it with authentication. |

The encrypted-file secret adapter and persistent one-time bootstrap currently fail closed on Windows
because equivalent filesystem ACL verification is not implemented. Their Unix success paths require
Unix validation; ordinary remoting authentication and local JSON metadata are separate facilities.

## Examples and validation

From the workspace root, the examples demonstrate individual APIs:

```bash
cargo run -p rocketmq-auth --example authentication_strategy_usage
cargo run -p rocketmq-auth --example authentication_manager_usage
cargo run -p rocketmq-auth --example authorization_evaluator_usage
cargo run -p rocketmq-auth --example acl_authorization_handler_usage
cargo run -p rocketmq-auth --example metadata_provider_example
```

Select checks appropriate to the changed behavior:

```bash
cargo fmt -p rocketmq-auth -- --check
cargo test -p rocketmq-auth --test public_api_contract --test security_api_identity --test java_alignment
cargo test -p rocketmq-auth --examples --no-run
cargo check -p rocketmq-auth --features grpc
```

`public_api_contract` checks exports and private-module boundaries; `security_api_identity` checks shared
type identity. `java_alignment` covers signing content, YAML semantics, equal-resource deny precedence,
super users, and reload behavior. Its invalid-file reload test does not establish transactional rollback
on storage failure. The `secure_bootstrap_contract`, `secret_provider_contract`,
`credential_rotation_contract`, and `release_checkpoint_authorization` integration suites cover their
respective adapters. Use `cargo test -p rocketmq-auth` for the broader crate suite.

## Benchmarks

```bash
cargo bench -p rocketmq-auth --bench auth_hot_path_bench
cargo bench -p rocketmq-auth --bench auth_acl_watcher_lifecycle_bench
```

The first measures signing, IP matching, ACL matching, and stateful cache hits. The second measures ACL
reload and shutdown lifecycle behavior and writes a report under `target/runtime-baseline/prototype`.
To compile benchmark targets without collecting measurements, use
`cargo test -p rocketmq-auth --benches --no-run`. Compare measurements on the same toolchain and hardware;
keep generated reports under `target/`.

## License

RocketMQ-Rust is licensed under the Apache License 2.0. See [LICENSE-APACHE](../LICENSE-APACHE).
