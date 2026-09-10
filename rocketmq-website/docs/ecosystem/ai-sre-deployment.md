---
title: "Deploy and develop AI SRE"
---

Use the checked-in Docker Compose development environment to bring up the cooperating services. It includes PostgreSQL, private Evidence storage, RocketMQ services, query MCP, Connector, Control Plane, Executor, Execution Agent, UI and observability components. This is a disposable development cluster, not a configuration for an existing business cluster. Read [AI SRE architecture](./ai-sre.md) before enabling individual execution actions.

## Prepare the environment and ports

Install Docker with Compose, Git and PowerShell; host Rust 1.95.0 and Node/npm are needed for local source development. PostgreSQL runs in Docker, so a host PostgreSQL installation is unnecessary. Ensure Docker can build the repository's Linux container images and that the intended development ports are available.

| Service | Development access | Meaning |
| --- | --- | --- |
| AI SRE UI | `http://localhost:3004` | Separate browser workspace |
| Control Plane | `http://localhost:8090` | Public versioned API; `/healthz` liveness, `/readyz` database readiness |
| Query MCP | `https://localhost:8089` | TLS MCP endpoint used by Connector |
| Connector | 8091 | Development integration service |
| Executor / Agent | 8094 / 8095 | Separate internal execution services; health is not execution authority |
| Connector reverse channel | 8444, published only on loopback | Enforcing mTLS proxy to the Connector-only listener |
| Internal Connector upstream | 8093, not published | Loopback in the shared Control Plane network namespace |
| NameServer / Broker | 9876 / 10911 and 10912 | Dedicated development RocketMQ services |
| Proxy | 8080 / 8081 | Development Proxy interfaces |
| PostgreSQL | 5432 | Durable SRE metadata |
| Prometheus / Loki / Tempo | 9090 / 3100 / 3200 | Metric, log and trace sources |
| OTLP | 4317 / 4318 | gRPC / HTTP telemetry ingestion |

MCP Control's example bind also uses 8090, and a tutorial NameServer/Broker may already occupy their ports. Do not run conflicting stacks simultaneously or terminate unrelated services. Choose an isolated environment or consistently adjust the deployment's published ports and references.

## Start and inspect the stack

From the repository root:

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\dev.ps1 -Action Up
.\rocketmq-ai\rocketmq-sre\scripts\dev.ps1 -Action Status
```

`Up` prepares missing local certificate/identity material, checks Compose configuration and starts/builds the stack with the `observability` profile and dependency waiting. Certificates and development identity files are generated under the repository's `target/phase00-certs`. Only required runtime files are mounted; the CA private key stays on the host. Do not copy these fixtures into production.

The development stack uses separate MCP reader, Agent reader, Probe, bootstrap administrator and Agent mutation identities. Probe traffic is restricted to dedicated `SRE_PROBE_` topics/groups. The Compose configuration explicitly enables the narrow Broker/Topic configuration handlers; this is an override of the Agent's default-disabled switches, not a general write surface. Executor still has neither target credentials nor target network.

The one-shot onboarding service creates the development cluster `00000000-0000-4000-8000-000000000001` in tenant `00000000-0000-4000-8000-000000000002`, with logical MCP alias `sre-dev`. Connector depends on onboarding and completes its authenticated capability handshake. Database readiness alone does not mean that handshake and every evidence source are ready.

Inspect minimal public health without credentials:

```powershell
Invoke-RestMethod 'http://127.0.0.1:8090/healthz'
Invoke-RestMethod 'http://127.0.0.1:8090/readyz'
```

Open `http://localhost:3004`. The development UI has explicitly configured fixture identity; it does not use a Dashboard login. The development OAuth issuer supports Connector-to-MCP client credentials, not browser OIDC/PKCE. A production UI cannot use it as an OIDC authority.

## Follow one diagnosis path

1. Open `/clusters` and select the bootstrapped development cluster. Confirm state `ready_read_only` and its capability/source details. For `read_only_degraded`, inspect the unavailable required source before continuing an interpretation.
2. Open `/coverage` and `/topology` to check what was actually observed. Missing topology or partial source coverage must remain visible; a route inferred from a label is not a substitute for RocketMQ evidence.
3. Open `/ask` and submit a bounded read question, for example: “For this cluster, which consumer groups currently have backlog, and what evidence supports that conclusion?” Select the intended cluster context. This requests diagnosis, not an offset reset.
4. Inspect the persisted conversation/answer, evidence citations, observation times, partial warnings and model/rules-only state. The default Compose model is a local fixture named `phase01-read-only-fixture`, so its answer does not qualify a production model.
5. Use `/incidents` or `/inspections` to follow the corresponding operational record when created by that workflow. A missing record or missing evidence is a result to investigate, not a reason to fabricate an Incident or an executable plan.

This procedure describes the UI/API flow; it was not executed against a live stack during this documentation task. The existing `phase00-smoke.ps1 -Target Compose` is a broader opt-in integration scenario: it sends bounded synthetic messages, exercises data sources and restarts, rotates fixture identity and finally offboards the cluster. It changes test state and is not a read-only health check. Offboarding retains history and is terminal; repeated full smoke requires a deliberate reset described in the local runbook.

## Develop one part at a time

Rust services belong to the independent `rocketmq-ai/rocketmq-sre/` workspace. From that directory, select the required packages:

```powershell
cargo build --locked -p rocketmq-sre-control-plane -p rocketmq-sre-connector
cargo run --locked -p rocketmq-sre-cli --bin rocketmq-sre -- --url http://127.0.0.1:8090 status
```

Building services does not supply database, object-store, identity or dependency configuration. Keep the Compose-managed backend while working on UI unless deliberately replacing a service. From a new shell at repository root:

```powershell
cd rocketmq-ai/rocketmq-sre/ui
npm ci
$env:ROCKETMQ_SRE_API_URL = 'http://127.0.0.1:8090'
npm run dev -- --host 127.0.0.1 --port 3005
```

Port 3005 avoids the Compose UI's published 3004. Vite proxies `/v1`, `/healthz` and `/readyz` to `ROCKETMQ_SRE_API_URL`, defaulting to `http://127.0.0.1:8090`. A separate host UI must explicitly configure the matching development identity values from the Compose UI build arguments; it does not inherit the running container's environment. Use production OIDC configuration when testing that path, never a real secret in `VITE_` variables. `npm run build` generates `ui/dist/`; it does not build Rust services.

The TypeScript SDK is the private source package `@rocketmq-rust/sre-client` under `sdk/typescript/`, not an asserted published npm artifact. It exports `SreClient` with fixed bounded GET operations; bearer tokens come from a value or provider callback and must not follow redirects to another origin. Build/test it from that directory with its existing `npm run build` / `npm test` scripts. There is no generic request, approval, execution or target mutation API.

The Rust CLI binary is `rocketmq-sre`. Options precede the command; `--url` overrides `ROCKETMQ_SRE_URL`. Protected reads use `ROCKETMQ_SRE_TOKEN` or the environment name supplied by `--token-env`, not a `--token` argument. Local `draft-plan` and `draft-runbook` validate bounded files without network access or execution authority. See the [CLI guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/crates/rocketmq-sre-cli/README.md) and [SDK source](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/sdk/typescript/src/index.ts).

## Production configuration boundaries

| Boundary | Production requirement |
| --- | --- |
| UI identity | OIDC; build with public `VITE_SRE_OIDC_AUTHORITY` and `VITE_SRE_OIDC_CLIENT_ID`. Missing required OIDC configuration fails closed. Development fixture tokens are not production identities. |
| Control Plane persistence | Configure PostgreSQL and apply its forward migrations; retain metadata and private Evidence objects together. `/readyz` requires database setup. |
| Evidence objects | Beyond the default 64 KiB inline bound, configure HTTPS S3-compatible endpoint, bucket and separate access/secret credentials. Local filesystem storage requires explicit development auth; no runtime memory fallback. |
| Connector to MCP | TLS, OAuth client credentials and matching read-only capability/schema/tenant/cluster negotiation; no anonymous fallback. |
| Connector reverse channel | Dedicated client-certificate identity and separate bearer validation behind the enforcing mTLS proxy; do not expose 8093 or accept client-supplied forwarded identity headers. |
| Model providers | Explicit enablement, capability profiles and secret references. Default network calls are off; production secret-provider configuration is separate from development environment/file resolution. |
| Executor / Agent | Internal listeners behind enforcing workload mTLS proxies and separate bearer tokens. The Axum listeners do not themselves terminate TLS. Restrict direct listener access and replace incoming identity headers at the trusted proxy. |
| Target changes | Enable only configured typed Agent actions, distinct read/mutation identities, target allowlists, PostgreSQL effect state, lease/fence authority and verification dependencies. |

Control Plane production object storage uses `ROCKETMQ_SRE_OBJECT_STORE_ENDPOINT`, `ROCKETMQ_SRE_OBJECT_STORE_BUCKET`, `ROCKETMQ_SRE_OBJECT_STORE_ACCESS_KEY` and `ROCKETMQ_SRE_OBJECT_STORE_SECRET_KEY`. Model profiles use reference-only credentials and provider-specific validation; consult the [Control Plane configuration guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/crates/rocketmq-sre-control-plane/README.md) before enabling a real provider. A provider endpoint inside a container cannot use host loopback unless that network arrangement is explicitly established.

## Stop, preserve state and diagnose failures

From repository root, stop the stack while preserving volumes:

```powershell
.\rocketmq-ai\rocketmq-sre\scripts\dev.ps1 -Action Down
```

`Down` preserves PostgreSQL, Evidence objects and observation volumes. The separate `Reset -Force` action deletes development volumes and generated certificate fixtures; use it only for an intentional disposable-environment reset, not as a generic restart command. Do not migrate production data by copying fixture volumes.

| Symptom | First check |
| --- | --- |
| Startup fails on a port | Existing tutorial, Dashboard, MCP Control or another development stack using that port |
| UI healthy, API unavailable | Reverse proxy/Vite target, Control Plane readiness and browser identity mode |
| Cluster remains degraded | MCP handshake, required Prometheus/Loki/Tempo sources, Connector mTLS and allowlists |
| Model unavailable or rules-only result | Model enablement, actual fixture/provider profile, capability/secret policy and stable error |
| Evidence reference cannot be read after restart | PostgreSQL/object-store volume pairing, storage configuration and authorization |
| Execution action not registered | Its individual Agent enable switch and required driver configuration; do not broaden query MCP |
| A full smoke cannot repeat onboarding | Prior offboard tombstone; use a deliberately reset test environment if repetition is intended |

Sources: [development runbook](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/deploy/dev/README.md), [Compose](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/deploy/dev/compose.yaml), [development script](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/scripts/dev.ps1), [UI identity guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/ui/README.md) and [UI routes](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/ui/src/App.tsx).
