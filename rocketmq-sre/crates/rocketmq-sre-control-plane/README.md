# rocketmq-sre-control-plane

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-control-plane` is the composition root for cluster onboarding,
read-only investigations, and model-assisted diagnosis. It serves the HTTP
API, stores workflow and model provenance in PostgreSQL, and publishes the
capability and telemetry coverage views consumed by the separate AI SRE UI.

## Responsibilities

- Apply SQLx migrations and persist clusters, append-only capability snapshots,
  and append-only onboarding events.
- Drive the `Pending` through read-only ready, degraded, rejected, and
  offboarded lifecycle.
- Make repeated onboarding and handshakes idempotent.
- Offboard through a tombstone and identity revocation while retaining history.
- Route bounded diagnosis prompts through the provider-neutral Model Gateway,
  with reference-only credentials and limited availability fallback.
- Persist the actual provider/model, prompt and schema versions, usage, cost,
  fallback chain, rationale, stable error, correlation ID, incident, diagnosis
  revision, invocation purpose, and repair parent invocation.

## Read-only boundary

Effective cluster access is always `read_only`. The service contains no
RocketMQ Admin mutation client, approval workflow, Executor integration, or
automatic remediation path. Models receive only bounded, sanitized evidence
summaries and eligible human-validated knowledge; they never receive MCP,
RocketMQ, or executor credentials and cannot call MCP directly. Invalid,
policy-denied, safety-refused, or unavailable model results retain the stable
`RulesOnlyDiagnosisNotExecutable` result with no primary model invocation.
Invalid structured output receives at most one bounded, tool-free repair call
to the same provider. The rejected response and repair are separate invocation
records linked by `parent_invocation_id`; a failed repair never triggers
schema-driven provider fallback. Transient timeout, 429, 5xx, and transport
failures alone can use the configured finite fallback chain.
`/readyz` requires successful database setup; `/healthz` reports process
liveness only.

The public API listener defaults to `0.0.0.0:8090`. Connector reverse-channel
routes are not mounted there: they use a separate listener configured by
`ROCKETMQ_SRE_CONNECTOR_BIND_ADDR` (default `127.0.0.1:8093`) and must remain
behind the mTLS identity proxy. Compose exposes `8093` only on its private
backend network; the Kind sidecar reaches it over Pod loopback.

## Evidence object storage

Inline Evidence is bounded by `ROCKETMQ_SRE_EVIDENCE_INLINE_BYTES` (64 KiB by
default). Larger sanitized JSON payloads require durable object storage:

- Development may set `ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH` to an explicit
  absolute directory while `ROCKETMQ_SRE_DEV_AUTH=true`. This backend emits an
  opaque `rocketmq-sre-local://` reference and never exposes its filesystem
  path. Object keys reject absolute, relative, encoded traversal, and
  backslash forms.
- Production requires `ROCKETMQ_SRE_OBJECT_STORE_ENDPOINT`,
  `ROCKETMQ_SRE_OBJECT_STORE_BUCKET`, `ROCKETMQ_SRE_OBJECT_STORE_ACCESS_KEY`,
  and `ROCKETMQ_SRE_OBJECT_STORE_SECRET_KEY`. Non-development endpoints must
  use HTTPS. Credential values are never included in logs or returned errors.
- The in-memory adapter is not selectable by runtime configuration and remains
  only for isolated router/unit tests. Development startup fails closed when
  neither the local directory nor an S3-compatible endpoint is configured.

## Model bootstrap

Network model calls are disabled by default. A local OpenAI-compatible endpoint
can be enabled without storing a credential:

```powershell
$env:ROCKETMQ_SRE_MODEL_ENABLED = "true"
$env:ROCKETMQ_SRE_MODEL_LOCAL_ENDPOINT = "http://127.0.0.1:8000/v1"
$env:ROCKETMQ_SRE_MODEL_LOCAL_NAME = "served-model"
$env:ROCKETMQ_SRE_MODEL_SECRET_PROVIDER = "none"
```

For multiple OpenAI-compatible, Anthropic, Gemini, Bedrock, DeepSeek, Zhipu
GLM, Kimi/Moonshot, or local profiles, set
`ROCKETMQ_SRE_MODEL_PROFILES_JSON` to a JSON array of Model Gateway
`ProviderProfile` objects. Credentials must be expressed only as
`env://`, `file://`, `external://`, or `adapter://` references. Development
environment/file resolution additionally requires both
`ROCKETMQ_SRE_DEV_AUTH=true` and `ROCKETMQ_SRE_MODEL_DEV_SECRETS=true`; the
default permitted environment prefix is `ROCKETMQ_SRE_MODEL_`. Select this
adapter explicitly with `ROCKETMQ_SRE_MODEL_SECRET_PROVIDER=dev`.

Production profiles with `external://` references select
`ROCKETMQ_SRE_MODEL_SECRET_PROVIDER=vault_agent_file` and configure:

- `ROCKETMQ_SRE_MODEL_VAULT_AGENT_ROOT`: read-only regular-file render root.
- `ROCKETMQ_SRE_MODEL_SECRET_NAMESPACE`: allowed external-reference prefix.
- `ROCKETMQ_SRE_MODEL_SECRET_CACHE_TTL_SECONDS`: bounded cache TTL (default 30).
- `ROCKETMQ_SRE_MODEL_SECRET_MAX_BYTES`: per-secret limit (default 64 KiB).
- `ROCKETMQ_SRE_MODEL_SECRET_VERSION_SUFFIX`: optional required non-secret
  revision sidecar such as `.version`.

The Control Plane constructs `VaultAgentFileSecretClient` and wraps it in
`ExternalSecretManagerProvider`; paths, reference locators, and values are
redacted. Startup rejects unowned reference schemes/namespaces, missing Vault
configuration, or a development provider in production mode. Plaintext
non-loopback model HTTP is also rejected in production.

The current Control Plane runtime invokes built-in HTTP protocol families.
The Model Gateway separately ships the runnable gRPC/mTLS Provider SPI client
and generated server contract; wiring a `provider_spi` profile into this
Control Plane process requires an explicit adapter endpoint and workload
identity configuration and is not silently downgraded to HTTP.

Optional finite controls are
`ROCKETMQ_SRE_MODEL_TIMEOUT_SECONDS`,
`ROCKETMQ_SRE_MODEL_MAX_REQUEST_BYTES`,
`ROCKETMQ_SRE_MODEL_MAX_RESPONSE_BYTES`, and
`ROCKETMQ_SRE_MODEL_MAX_FALLBACKS` (maximum 3).
`ROCKETMQ_SRE_MODEL_ALLOW_INSECURE_HTTP=true` exists only for an isolated
development network and itself requires `ROCKETMQ_SRE_DEV_AUTH=true`.

Authenticated model visibility is available at:

- `GET /v1/models/capabilities`
- `GET /v1/models/status`
- `GET /v1/models/invocations?cluster_id=<uuid>&limit=50`

## Validation

Run from `rocketmq-sre/` with PostgreSQL supplied by the development Compose
stack when exercising persistence:

```powershell
cargo check --locked -p rocketmq-sre-control-plane
cargo test --locked -p rocketmq-sre-control-plane
```

The tests cover API mapping, onboarding idempotency, capability aggregation,
degraded handshakes, offboarding, schema-repair bounds, snake_case rules-only
metrics, and PostgreSQL model invocation/revision lineage.
