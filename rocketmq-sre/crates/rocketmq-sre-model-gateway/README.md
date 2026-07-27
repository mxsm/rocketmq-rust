# rocketmq-sre-model-gateway

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-sre-model-gateway` is the provider-neutral model boundary for the
read-only RocketMQ Rust AI SRE. It translates a canonical model IR, filters
provider profiles by capability/data class/region/budget, applies tightly
limited fallback, and records the provider identity that actually produced a
result.

## Implemented surface

- Canonical chat, embedding, rerank, tool, tool-result, vision, reasoning,
  structured-output, usage, finish-reason, and bounded-stream contracts.
- `ChatModelProvider`, `EmbeddingProvider`, and `RerankProvider` traits.
- Injectable protocol adapters plus a production rustls-backed asynchronous
  HTTP transport for OpenAI-compatible, Anthropic Messages, Gemini native, and
  Bedrock Converse protocols.
- Profiles for OpenAI, Azure OpenAI, Anthropic, Gemini, Bedrock, DeepSeek
  (OpenAI and Anthropic dialects), Zhipu GLM, Kimi/Moonshot, vLLM, Ollama,
  llama.cpp, SGLang, and an enterprise OpenAI-compatible proxy.
- DeepSeek reasoning continuity, explicit Zhipu tool-choice rejection, and
  profile-gated Kimi MFJS behavior.
- Registry/router filtering and timeout/rate-limit/service-unavailable-only
  fallback, local structured-output validation, rules-only degradation, and
  `ModelInvocationRecord`.
- Reference-only secret profiles, deny-by-default development env/file
  resolution, a namespace-constrained external Secret Manager adapter with
  TTL/watch refresh, a production Vault Agent/CSI rendered-file client, and
  redacted credential-version fingerprints.
- Versioned process-external Provider SPI contract plus a runnable asynchronous
  gRPC client/server surface with rustls mutual TLS, payload/deadline bounds,
  streaming, cancellation, health, correlation IDs, verified application
  identities, stable redacted errors, and adapter-owned credentials.

## Security boundary

Profiles contain only references such as
`external://rocketmq-sre/models/deepseek`; they never contain plaintext API
keys. Built-in adapters resolve a reference at the injected model transport
boundary. SPI adapters use an independent workload identity and resolve their
own `adapter://` credential. Neither path receives RocketMQ, MCP, or executor
credentials.

The crate does not select a cloud SDK. A service supplies
`AsyncModelTransport` with its owned lifecycle. `HttpModelTransport` supplies
the reusable reqwest/rustls pool, explicit root certificates, optional client
identity, absolute-deadline enforcement, and bounded JSON reads. It disables
redirects and ambient proxies, rejects plaintext non-loopback endpoints by
default, and never includes endpoint, payload, or credential material in
errors or `Debug` output.

The original synchronous `ModelTransport` remains for compatibility and
deterministic fixtures. Production async code should use
`AsyncBuiltinProviderClient`. Resolve credentials with an async secret client;
when integrating an existing synchronous `SecretProvider`, submit that lookup
to the service-owned
`ServiceContext::metadata_io().spawn_io("model-secret-resolve", ...)` lane
before calling the async client. Do not call the synchronous provider adapter
or create a nested runtime from a Tokio task.

## Vault Agent/CSI rendered secrets

`VaultAgentFileSecretClient` is the concrete production
`ExternalSecretClient` for deployments that render credentials to a local
volume with Vault Agent templates or the Vault CSI provider. It does not use a
Vault token or vendor SDK. The gateway workload receives read-only access only
to one canonical render root; Vault Agent or CSI remains the sole writer.

```rust
use std::sync::Arc;
use std::time::Duration;

use rocketmq_sre_model_gateway::ExternalSecretManagerProvider;
use rocketmq_sre_model_gateway::VaultAgentFileSecretClient;

let client = VaultAgentFileSecretClient::new("/run/rocketmq-sre/model-secrets")?
    .with_max_secret_bytes(64 * 1024)?
    .with_required_version_sidecar(".version")?;
let secrets = ExternalSecretManagerProvider::new(
    Arc::new(client),
    "rocketmq-sre/models",
    Duration::from_secs(30),
);
# Ok::<(), rocketmq_sre_model_gateway::ProviderError>(())
```

For the reference
`external://rocketmq-sre/models/deepseek`, the example reads these regular
files:

```text
/run/rocketmq-sre/model-secrets/rocketmq-sre/models/deepseek
/run/rocketmq-sre/model-secrets/rocketmq-sre/models/deepseek.version
```

The sidecar is optional at the API level. Without it, the version fingerprint
contains only file modification time and length; it never hashes or embeds the
credential. With `.version`, Vault should render an opaque non-secret revision
such as `kv-v42`, then emit the watch notification only after both files are
current. `ExternalSecretManagerProvider::on_watch_event` bypasses its TTL cache
and makes that revision visible without restarting the gateway.

Deployment requirements:

- Mount the canonical root read-only in the gateway container, owned for write
  only by Vault Agent/CSI, with least-privilege filesystem permissions.
- Render regular UTF-8 files. Absolute paths, `..`, empty components,
  backslashes, directories, symbolic links (including the final file), and
  files over the configured limit fail closed.
- Do not use a projected-volume layout that exposes credentials through
  symlinks. Configure the Vault renderer to atomically replace regular files.
- Treat the version sidecar as metadata, never as a second copy or derivative
  of the credential. Keep its value to ASCII letters, digits, `.`, `_`, `-`,
  or `:`.
- Run synchronous lookup on the service-owned metadata I/O lane described
  above. Error and `Debug` output redact the root, locator, and secret value.

## HTTP authentication

| Dialect | Authentication |
| --- | --- |
| OpenAI, DeepSeek, Zhipu GLM, Kimi/Moonshot, enterprise proxy | Sensitive `Authorization: Bearer` |
| Azure OpenAI | Sensitive `api-key` |
| Anthropic and DeepSeek Anthropic dialect | Sensitive `x-api-key` plus a fixed `anthropic-version` |
| Gemini native | Sensitive `x-goog-api-key` |
| Bedrock Converse | AWS Signature Version 4 for service `bedrock`, including optional session token |

Bedrock credential material uses a secret-manager value with this JSON shape:

```json
{
  "access_key_id": "resolved-at-runtime",
  "secret_access_key": "resolved-at-runtime",
  "session_token": "optional-resolved-at-runtime",
  "region": "us-east-1"
}
```

This JSON is never part of a provider profile or log record.

## Provider SPI

The frozen wire contract is
`proto/provider/v1/provider.proto`. A minimal adapter implementation is in
`examples/provider_spi_adapter.rs`. `GrpcProviderSpiClient` connects only over
`https://`/`grpcs://`, presents a client certificate, validates the server CA
and DNS name, then verifies the negotiated SPI wire version, SPIFFE
application identity, adapter-owned credential declaration, and capabilities.
Adapter processes implement the exported generated
`provider_spi_wire::provider_adapter_server::ProviderAdapter` service, wrap it
with `bounded_provider_adapter_service`, and configure Tonic `ServerTlsConfig`
with both a server identity and client CA root.

`tests/grpc_spi.rs` runs a real loopback HTTP/2 gRPC client/server pair and
proves valid mTLS, rejection of an untrusted client certificate, wire-version
failure, unary and streaming invocation, health, cancellation, payload
bounds, and redacted adapter error mapping.

## Contract fixtures

`tests/fixtures/providers/provider-profile-manifest.v1.yaml` is JSON-compatible
YAML. It freezes every built-in profile's dialect, normalized model family,
revision, capabilities, and protocol fixture. `provider_contracts` verifies
text, JSON, tool call, error mapping, DeepSeek/Zhipu/Kimi differences, routing,
fallback identity, structured-output validation, and SPI behavior without real
cloud credentials. `http_transport` starts an ephemeral loopback mock and
verifies protocol authentication, canonical text/JSON/tool parsing, provider
errors, timeouts, response/request bounds, redirect refusal, invalid JSON,
expired credentials, and TLS configuration.

## Manual provider smoke

The hand-run example exits successfully without network access when the
credential or model variable is absent:

```powershell
cargo run --locked -p rocketmq-sre-model-gateway --example provider_http_smoke
```

To explicitly invoke a real configured provider:

```powershell
$env:ROCKETMQ_SRE_MODEL_SMOKE_PROFILE = "deepseek"
$env:ROCKETMQ_SRE_MODEL_SMOKE_MODEL = "deepseek-chat"
$env:ROCKETMQ_SRE_MODEL_SMOKE_CREDENTIAL = "<resolved API key>"
cargo run --locked -p rocketmq-sre-model-gateway --example provider_http_smoke
```

Use `ROCKETMQ_SRE_MODEL_SMOKE_ENDPOINT` for Azure, Bedrock, or an enterprise
endpoint whose built-in value is only a fixture. For Bedrock, the credential
variable contains the JSON value documented above. The example prints only
profile/model/finish/usage metadata, never response content or credentials.

An ignored, profile-JSON-driven smoke is also available. With no profile it
prints an explicit skip and makes no network call:

```powershell
cargo test --locked -p rocketmq-sre-model-gateway --test live_provider_smoke -- --ignored --nocapture
```

To enable it, set `ROCKETMQ_SRE_LIVE_PROVIDER_PROFILE_JSON` to one validated
reference-only `ProviderProfile`. Its `credential_ref` should normally be
`env://ROCKETMQ_SRE_LIVE_<NAME>`; set the referenced environment value at
runtime. `ROCKETMQ_SRE_LIVE_PROVIDER_SECRET_PREFIX` can narrow or change the
allowed prefix. The test reports only skipped/passed status and never prints
the endpoint, profile, response, or credential. A skipped run is not evidence
of a real provider invocation.

## Validation

Run from `rocketmq-sre/`:

```powershell
cargo fmt -p rocketmq-sre-model-gateway -- --check
cargo test --locked -p rocketmq-sre-model-gateway --all-targets
cargo clippy --locked -p rocketmq-sre-model-gateway --all-targets -- -D warnings
cargo doc --locked -p rocketmq-sre-model-gateway --no-deps
```
