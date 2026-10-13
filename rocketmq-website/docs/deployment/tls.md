---
title: "Configure TLS and mTLS across components"
description: "Certificate preparation, Kubernetes Secrets, component configuration, verification, and rotation for RocketMQ-Rust."
---

Configure TLS for each connection separately. This guide covers the current service executables, Rust clients, Proxy, Admin, Dashboards, MCP, and AI SRE. TLS encrypts a connection and verifies the server; mTLS also verifies a client certificate. Neither grants RocketMQ ACL permissions, OAuth scopes, or SRE execution authority. Keep [authentication and authorization](security.md) configured independently.

Examples use Bash on Linux, macOS, or WSL, OpenSSL 3, and repository-root paths unless a command explicitly changes directory. Replace example DNS names, addresses, namespaces, release names, and file paths with the actual deployment. TOML excerpts must be merged into the matching service configuration; they are not complete startup files. Container paths refer to mounted files inside the container.

## Connection and component map {#component-matrix}

| Connection / component | Configuration entry | Current boundary |
| --- | --- | --- |
| Client → NameServer remoting, normally 9876 | NameServer root-level `tls.*` properties in TOML | NameServer defaults include the `tls` build feature; configure the listener and all callers separately. |
| Client → Broker remoting, normally 10911 and VIP 10909 | `broker.brokerServerConfig.tlsConfig` | Build Broker with `tls`; the main and fast/VIP listeners inherit the server TLS configuration. |
| Producer, Push, LitePull, embedded Rust Admin | `ClientConfig` or the Admin builder's `use_tls` | Client TLS applies to its remoting connections; CA trust and optional client identity still need configuration. |
| Client → Proxy v2 gRPC, normally 8081 | `grpc.tls` | Proxy's `tls` feature is optional; this listener supports TLS and mTLS. |
| Client → Proxy remoting; Proxy Cluster → NameServer/Broker | Separate remoting/backend configuration | Current Proxy remoting settings expose no TLS identity fields; the default Cluster client does not enable TLS. `grpc.tls` does not configure either path. |
| Broker → NameServer/Controller/other Broker; NameServer internal client | Internal remoting clients | Current launchers create these with default plaintext client configuration. Listener `tls.client.*` fields do not configure those separately constructed clients. |
| Broker HA replication; Controller remoting and Raft | Separate HA / Controller transports | No complete executable-level TLS configuration is wired for these paths. Controller's Raft client uses `http://`; embedded Controller has the same boundary. |
| MCP client → query MCP | `server.http.tls` | Streamable HTTP is served over HTTPS; stdio has no network TLS. The current query MCP cluster schema has no outbound `use_tls` option. |
| MCP client → MCP Control; Control → RocketMQ | `server.tls`; `clusters[].use_tls` | Independent HTTPS and outbound remoting policies; inbound TLS does not enable or authorize mutation tools. |
| SRE Connector → MCP / Control Plane / Kubernetes | Separate HTTPS CA and mTLS identity settings | The Control Plane channel terminates mTLS at a dedicated proxy; HTTP backend isolation is required. |
| SRE Executor / Agent / Model Gateway | Workload mTLS proxy or provider transport API | Executor/Agent Axum listeners do not terminate TLS. Model Gateway HTTP and Provider SPI clients have their own TLS configuration. |
| Web / Tauri / GPUI Dashboard → RocketMQ | Connection TLS setting; Web uses `DASHBOARD_WEB_USE_TLS` | Does not configure browser HTTPS or SQL TLS. Desktop apps use the local process's trust configuration. |
| Browser → Web Dashboard / SRE UI; SQL; telemetry | HTTPS gateway, database driver, collector configuration | Separate trust and Secret ownership; no shared RocketMQ TLS switch configures these endpoints. |

A fully encrypted deployment must account for **every** row it uses. Enforcing TLS on NameServer while the standard Broker registration client is plaintext breaks registration. Enforcing it on Broker can break the standard Proxy Cluster backend and query MCP. For these combinations, supply a deliberately integrated outbound TLS adapter or an external encrypted transport boundary, with matching routing and access restrictions. A private network restricts exposure but does not itself encrypt traffic. Do not describe the supplied Core Helm TLS profile as full-cluster TLS.

Sources: [NameServer launcher](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs), [Broker listeners](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/broker_runtime/request_pipeline/startup.rs), [Broker outbound composition](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/broker_runtime/composition.rs), [Proxy Cluster client](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-cluster/src/cluster.rs), [Controller composition](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/src/controller/controller_manager.rs).

## Prepare certificates and trust roots {#certificates}

Prepare these materials for each workload:

| Material | Consumer | Requirement |
| --- | --- | --- |
| Server certificate chain and private key | TLS listener | PEM chain, leaf first followed by intermediates; matching private key; `serverAuth` EKU and the actual endpoint DNS/IP SANs. |
| Server CA bundle | Client | Public CA certificates that validate the server; no CA private key. A configured remoting trust bundle replaces native roots. |
| Client certificate chain and private key | mTLS client | Separate workload identity with `clientAuth` EKU; keep the pair together. |
| Client CA bundle | mTLS server | Public CA certificates permitted to issue client identities. `optional` still validates any presented certificate. |

SANs must match the address the client actually verifies: NameServer discovery DNS, route-advertised Broker IP/DNS, external Proxy DNS, or Kubernetes Service/peer DNS. A certificate for `localhost` does not cover a Service name. A certificate for a DNS name does not cover connections using its IP address. For the Core chart, inspect the rendered Service and peer names under the selected release and namespace before requesting certificates.

Use your organization CA or certificate controller in production. Keep CA signing keys outside Pods and application Secrets. Use separate server/client roots or issuance policies where the trust boundary requires them. Do not reuse SRE development certificates or example credentials.

### Generate a disposable local CA and identities

Run this only in a new, private laboratory directory. It creates unencrypted private keys protected by file permissions; it is not a production CA procedure. Change each SAN before using the output against a real endpoint.

```bash
umask 077
mkdir -p private
openssl req -x509 -newkey rsa:3072 -noenc -sha256 -days 365 -keyout private/ca.key -out private/ca.crt -subj "/CN=RocketMQ Local TLS CA" -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"

for component in namesrv broker proxy mcp control; do
  openssl req -new -newkey rsa:3072 -noenc -keyout "private/$component.key" -out "private/$component.csr" -subj "/CN=$component.mq.example.test"
  cat > "private/$component.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:$component.mq.example.test
EOF
  openssl x509 -req -in "private/$component.csr" -CA private/ca.crt -CAkey private/ca.key -CAcreateserial -days 30 -sha256 -extfile "private/$component.ext" -out "private/$component.crt"
done

openssl req -new -newkey rsa:3072 -noenc -keyout private/client.key -out private/client.csr -subj "/CN=docs-messaging-client"
cat > private/client.ext <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth
EOF
openssl x509 -req -in private/client.csr -CA private/ca.crt -CAkey private/ca.key -CAcreateserial -days 30 -sha256 -extfile private/client.ext -out private/client.crt
openssl verify -CAfile private/ca.crt -purpose sslserver private/proxy.crt
openssl verify -CAfile private/ca.crt -purpose sslclient private/client.crt
```

Issue separate client identities for applications, administrators, and infrastructure; the single client above is only a lab example. Store chains and keys in private files and never commit them, generated Secret YAML, or diagnostic dumps containing them. Transport supports encrypted PKCS#8 keys through its password fields; Proxy gRPC exposes `privateKeyPassword`. MCP's dedicated TLS sections expose no key-password field. Do not put passwords into a public ConfigMap.

## Create and mount Kubernetes Secrets {#secrets}

Choose the correct kubectl context first. The following commands create resources in that selected cluster. Use `kubectl create namespace rocketmq` only if the namespace does not exist. Choose **one** Proxy TLS Secret creation command:

```bash
kubectl create namespace rocketmq

# Server authentication only; pair with clientAuth: none.
kubectl -n rocketmq create secret tls rocketmq-proxy-tls --cert=private/proxy.crt --key=private/proxy.key
```

For mTLS, create the same name with the additional client CA instead of the preceding `create secret tls` command:

```bash
kubectl -n rocketmq create secret generic rocketmq-proxy-tls --type=kubernetes.io/tls --from-file=tls.crt=private/proxy.crt --from-file=tls.key=private/proxy.key --from-file=ca.crt=private/ca.crt
```

`ca.crt` in this server Secret trusts **client** certificates. The client's server trust bundle can be a different CA. `kubectl create secret tls` supplies `tls.crt` and `tls.key`; it does not generate a certificate, add a client CA, or enable any listener.

Create the other Secrets only for components you actually deploy:

```bash
kubectl -n rocketmq create secret generic rocketmq-namesrv-tls --type=kubernetes.io/tls --from-file=tls.crt=private/namesrv.crt --from-file=tls.key=private/namesrv.key --from-file=ca.crt=private/ca.crt
kubectl -n rocketmq create secret generic rocketmq-broker-tls --type=kubernetes.io/tls --from-file=tls.crt=private/broker.crt --from-file=tls.key=private/broker.key --from-file=ca.crt=private/ca.crt
kubectl -n rocketmq create secret generic rocketmq-client-tls --from-file=ca.crt=private/ca.crt --from-file=client.crt=private/client.crt --from-file=client.key=private/client.key
kubectl -n rocketmq create secret tls rocketmq-mcp-tls --cert=private/mcp.crt --key=private/mcp.key
kubectl -n rocketmq create secret tls rocketmq-control-tls --cert=private/control.crt --key=private/control.key
kubectl -n rocketmq create secret generic rocketmq-issuer-ca --from-file=ca.crt=private/issuer-ca.crt
```

Provision `private/issuer-ca.crt` from the real OAuth issuer first; the laboratory CA is not automatically its issuer. `rocketmq-mcp-tls` and `rocketmq-control-tls` are names for a custom workload, not resources automatically mounted by the Core chart.

ACLs and internal RPC credentials are separate Secrets. After preparing real restricted ACL and JSON credential files as described in [deployment security](security.md):

```bash
kubectl -n rocketmq create secret generic rocketmq-namesrv-auth --from-file=plain_acl.yml=private/namesrv-acl.yml
kubectl -n rocketmq create secret generic rocketmq-broker-auth --from-file=plain_acl.yml=private/broker-acl.yml --from-file=inner-client.json=private/broker-inner-client.json
kubectl -n rocketmq create secret generic rocketmq-proxy-auth --from-file=plain_acl.yml=private/proxy-acl.yml --from-file=inner-client.json=private/proxy-inner-client.json
kubectl -n rocketmq create secret generic rocketmq-controller-auth --from-file=plain_acl.yml=private/controller-acl.yml
```

Do not put actual secret values in `--from-literal` examples or shell history. Use files from your secret delivery workflow. The Core chart's internal JSON keys are `accessKey`, `secretKey`, and optional `securityToken`; the receiving service must grant that identity its required permissions.

### Custom workload mount

The following is a **Pod spec excerpt** for a Linux Broker container. Merge it into `spec.template.spec` for a Deployment or StatefulSet. Use an equivalent mount with the correct Secret name for NameServer, MCP, and Control. Client Pods mount their own client Secret, never the server private key.

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  fsGroup: 10001
containers:
  - name: broker
    volumeMounts:
      - name: remoting-tls
        mountPath: /etc/rocketmq/tls
        readOnly: true
volumes:
  - name: remoting-tls
    secret:
      secretName: rocketmq-broker-tls
      defaultMode: 0440
      items:
        - {key: tls.crt, path: tls.crt}
        - {key: tls.key, path: tls.key}
        - {key: ca.crt, path: ca.crt}
```

Use directory mounts for reloadable material. Secret volume updates are eventually projected; `subPath` mounts do not receive them. Environment variables do not change inside an existing process when a Secret changes. Confirm that the actual service UID can read its key; Windows volume ACLs need a platform-specific deployment. Limit Secret RBAC and Pod access, enable cluster encryption at rest, and remember that base64 encoding is not encryption. See [Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/).

## NameServer and Broker remoting {#remoting}

Build the required implementations from the repository root:

```bash
cargo build -p rocketmq-namesrv --bin rocketmq-namesrv-rust --features tls
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --features tls
```

### NameServer

Merge the following **root-level dotted assignments** into `namesrv.toml`, before any TOML table headers. The launcher also reads these lines through its Java-compatible TLS properties parser. A nested `[tls.server]` table or quoted `"tls.enable"` key is not an equivalent startup configuration for this loader.

```toml
tls.enable = true
tls.test.mode.enable = false
tls.server.mode = "enforcing"
tls.server.certPath = "/etc/rocketmq/tls/tls.crt"
tls.server.keyPath = "/etc/rocketmq/tls/tls.key"
tls.server.need.client.auth = "require"
tls.server.authClient = true
tls.server.trustCertPath = "/etc/rocketmq/tls/ca.crt"
```

Start with `-c /etc/rocketmq/namesrv.toml`. Existing `configStorePath` durable desired properties are applied after startup-file values, and CLI bind/port arguments override their corresponding fields. Inspect the effective configuration if the listener behaves differently from the file.

### Broker

Merge into canonical Broker TOML:

```toml
[broker.brokerServerConfig.tlsConfig]
enable = true
testModeEnable = false

[broker.brokerServerConfig.tlsConfig.server]
mode = "enforcing"
certPath = "/etc/rocketmq/tls/tls.crt"
keyPath = "/etc/rocketmq/tls/tls.key"
needClientAuth = "require"
authClient = true
trustCertPath = "/etc/rocketmq/tls/ca.crt"
```

Start with `-c /etc/rocketmq/broker.toml --config-format toml`. Both remoting listeners inherit this policy. Clients using the VIP port need the same certificate trust and endpoint SANs. This configuration does not encrypt HA replication or configure the Broker's separately created outbound client.

For either server, choose `mode = "enforcing"` to refuse plaintext; `permissive` accepts plaintext and TLS and is only suitable for an intentional transition; `disabled` does not serve TLS. To use server authentication without mTLS, select `needClientAuth = "none"` (NameServer: `tls.server.need.client.auth`) and omit client CA/auth fields. `optional` and `require` both need `authClient = true` and a valid client CA.

The transport's optional properties file defaults to `/etc/rocketmq/tls.properties`. Broker can select it with `configFile` inside `tlsConfig`; NameServer uses `tls.config.file`. It can override certificate, trust, verification, and protocol fields, but cannot override the base `enable` or server mode. Use `tls.protocols=TLSv1.3,TLSv1.2` there when an explicit protocol selection is needed; legacy protocol versions are rejected. Keep this override intentional, especially for clients.

The [secure deployment profile](security.md#select-the-process-bootstrap-profile) validates additional identity/policy material. Its `ROCKETMQ_SECURITY_TLS_CERT` and `ROCKETMQ_SECURITY_TLS_KEY` variables do not replace these listener settings. The NameServer secure-enforced launcher also requires an enforcing listener and application authentication/authorization.

## Rust messaging clients and Admin {#clients}

Ensure the final dependency graph includes `rocketmq-transport/tls`; `rocketmq-client-rust` has no feature named `tls`. Do not rely on dev-dependencies to provide TLS in a production build. Attach the common configuration to each actual client before starting it:

```rust
use rocketmq_client_rust::base::client_config::ClientConfig;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;

let common = ClientConfig::builder()
    .namesrv_addr("namesrv.mq.example.test:9876")
    .enable_tls(true)
    .tls_test_mode_enable(false)
    .tls_client_auth_server(true)
    .tls_client_trust_cert_path("/etc/rocketmq/client-tls/ca.crt")
    .tls_client_cert_path("/etc/rocketmq/client-tls/client.crt")
    .tls_client_key_path("/etc/rocketmq/client-tls/client.key")
    .build()?;

let producer = DefaultMQProducer::builder()
    .producer_group("docs_tls_producer")
    .client_config(common)
    .build();
```

This excerpt configures a Producer; inject its normal owned runtime and call the normal start/send/shutdown APIs as in [client configuration](../configuration/client-config.md). Push accepts `client_config` too. LitePull accepts `client_options(ClientOptions::from(common))` and also needs `use_tls(true)`, because its own TLS value is applied when building. Omit **both** client certificate/key fields for server-only TLS. Always keep test mode off and server verification on for secure deployments.

The client must trust certificates for both NameServer and all route-selected Brokers. Use a CA bundle containing all required issuers, keep route addresses reachable, and include the route-advertised DNS/IP in Broker SANs. A TLS client does not fall back to plaintext for an unconfigured peer.

Admin Core's read/mutation builders expose `use_tls(true)`. Where a facade exposes only that boolean, mount a deliberate `/etc/rocketmq/tls.properties` in the Admin process to supply trust and optional client identity:

```properties
tls.test.mode.enable=false
tls.client.authServer=true
tls.client.trustCertPath=/etc/rocketmq/client-tls/ca.crt
tls.client.certPath=/etc/rocketmq/client-tls/client.crt
tls.client.keyPath=/etc/rocketmq/client-tls/client.key
```

These properties do not turn a facade's TLS boolean on. The current Admin CLI has no global `--tls`, CA, or client-certificate option; do not invent such a command or assume setting `tls.enable` in this file enables its sessions. Use an explicitly TLS-configured Admin integration for secured endpoints. See [Admin usage](../operations/admin.md).

## Proxy gRPC and the Core Helm chart {#proxy}

```bash
cargo build -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features cluster-mode,tls
```

Use `local-mode,tls` instead when building Local mode. Merge this excerpt into the matching [Proxy configuration](proxy.md):

```toml
[grpc.tls]
enabled = true
certificatePath = "/etc/rocketmq/tls/tls.crt"
privateKeyPath = "/etc/rocketmq/tls/tls.key"
clientAuth = "require"
clientCaPath = "/etc/rocketmq/tls/ca.crt"
reloadIntervalMs = 5000
```

`clientAuth` is `none`, `optional`, or `require`; the latter two need `clientCaPath`. `reloadIntervalMs` must be positive. The built-in listener negotiates HTTP/2. Clients need the server CA, the endpoint name, and their client identity for mTLS; the remoting Rust Producer above is not a Proxy v2 gRPC client.

For Kubernetes, prepare the Proxy TLS and auth Secrets first, choose an image actually built with `tls`, and place the following in your `core-site-values.yaml` alongside the real image overrides:

```yaml
services:
  proxy:
    tls:
      enabled: true
      secretName: rocketmq-proxy-tls
      clientAuth: require
      clientCaKey: ca.crt
```

Render before installing:

```bash
helm template core distribution/helm/rocketmq-rust-core -n rocketmq -f distribution/helm/rocketmq-rust-core/values-production-proxy-tls.yaml -f core-site-values.yaml > core-rendered.yaml
helm upgrade --install core distribution/helm/rocketmq-rust-core -n rocketmq -f distribution/helm/rocketmq-rust-core/values-production-proxy-tls.yaml -f core-site-values.yaml
```

The chart mounts `tls.crt`, `tls.key`, and, when mTLS is selected, the Secret key named by `clientCaKey` as `/etc/rocketmq/tls/ca.crt`. It renders `grpc.tls` for Proxy only. It has no equivalent NameServer/Broker TLS Secret values or generic extra-volume switch; use maintained custom manifests/overlays for those mounts and configurations. Do not add imaginary `services.broker.tls` values and expect them to take effect. Keep rendered files private because other values may contain sensitive material. See [Kubernetes deployment](kubernetes.md).

## MCP HTTPS and outbound trust {#mcp}

### Query MCP

From `rocketmq-ai/rocketmq-mcp`, build with `streamable-http`. Merge into its existing configuration:

```toml
[server.http.tls]
cert_path = "/etc/rocketmq/tls/tls.crt"
key_path = "/etc/rocketmq/tls/tls.key"

[server.http.auth]
mode = "oauth-jwt"
issuer = "https://identity.example.test"
audience = "rocketmq-mcp"
required_scopes = ["rocketmq:read"]
jwt_algorithm = "rs256"
jwks_url = "https://identity.example.test/.well-known/jwks.json"
jwks_ca_path = "/etc/rocketmq/issuer/ca.crt"
```

Also set `server.http.public_base_url` and allowed origins to the real HTTPS deployment, preserve its permissions/audit configuration, and launch with `--config /etc/rocketmq/mcp.toml --transport streamable-http`. The CLI's default transport is stdio, even when a file selects HTTP. TLS and JWKS CA paths are resolved relative to the config file; absolute container paths avoid ambiguity. Omit `jwks_ca_path` when the issuer uses the supported public trust path.

Its dedicated TLS section configures a server certificate, not client-certificate authorization. Production authentication uses OAuth; the loopback development-token fixture is not a production identity provider. Query MCP's cluster config has no `use_tls` field, and its default Admin session does not enable outbound TLS. Securing its HTTPS endpoint does not secure its RocketMQ query connection. See [query MCP](../ecosystem/mcp.md).

### MCP Control

From `rocketmq-ai/rocketmq-mcp-control`, use its standalone build. Merge into the real configuration:

```toml
[server.tls]
cert_path = "/etc/rocketmq/tls/tls.crt"
key_path = "/etc/rocketmq/tls/tls.key"

[oauth]
issuer = "https://identity.example.test"
audience = "rocketmq-mcp-control"
jwks_url = "https://identity.example.test/.well-known/jwks.json"
jwks_ca_path = "/etc/rocketmq/issuer/ca.crt"
```

Set `use_tls = true` in each existing `[[clusters]]` entry and supply remoting trust/client identity using the Admin properties file above. Control uses `ROCKETMQ_MCP_CONTROL_CONFIG`, has no `--config` route, and does not rebase its TLS paths to the file directory. Its default URL/JWKS policy requires public canonical HTTPS; private deployments need the documented `server.allow_internal_base_url`, `server.allow_wildcard_bind` where necessary, and `oauth.allow_private_network` decisions. These do not disable certificate verification or OAuth authorization. Keep mutations disabled until the separate feature, policy, audit, credentials, and claims are ready. See [MCP Control](../ecosystem/mcp-control.md).

Both HTTPS listeners use the transport TLS runtime. A mounted `/etc/rocketmq/tls.properties` can affect their effective TLS policy; keep it deliberate. OAuth signing/JWKS keys and HTTPS TLS certificates have different purposes and must be rotated independently.

## AI SRE connections and Secrets {#sre}

Keep each SRE trust boundary separate:

| Path | Configuration / material | Usage notes |
| --- | --- | --- |
| Connector → MCP | `ROCKETMQ_SRE_MCP_URL`, `ROCKETMQ_SRE_MCP_CA_PATH`; OAuth client credentials | Use the MCP endpoint's CA, matching HTTPS URL, scopes, tenant/cluster and read capability negotiation. OAuth token endpoint trust is a separate connection. |
| Connector → Control Plane | `ROCKETMQ_SRE_CONTROL_PLANE_URL`, `ROCKETMQ_SRE_CONTROL_PLANE_CA_PATH`, `ROCKETMQ_SRE_CONTROL_PLANE_CLIENT_IDENTITY_PATH` | HTTP/2 HTTPS; client identity file contains the client certificate chain **and private key**. A separate Bearer token remains required. |
| Connector / Agent → Kubernetes | Connector `ROCKETMQ_SRE_KUBERNETES_CA_PATH`; Agent's Kubernetes settings | Mount the API's public CA and only the service-account token/RBAC required for that workload. Do not use the messaging CA as the Kubernetes CA. |
| Executor / Agent inbound | Dedicated workload mTLS gateway and separate Bearer identities | Isolate the Axum listener; gateway must replace `x-forwarded-client-cert` with verified identity. Agent expects `spiffe://rocketmq-sre/executor`; Executor expects `spiffe://rocketmq-sre/control-plane`. |
| Connector / Agent → RocketMQ Admin | `ROCKETMQ_SRE_ADMIN_USE_TLS` / `ROCKETMQ_SRE_AGENT_BROKER_ADMIN_USE_TLS` | Independent of HTTPS channels; use the Admin trust/identity properties and actual endpoint support. Development manifests explicitly use plaintext here. |
| Model Gateway → HTTP model provider / gRPC Provider SPI | `HttpTlsConfig` / `GrpcSpiClientTlsConfig::mutual_tls` | Inject root PEM and optional HTTP client identity; SPI needs CA, client cert/key, server domain and expected adapter identity. These are library APIs, not automatic global env settings. |
| SRE UI/API, PostgreSQL, OTLP | Deployment gateway / database / collector settings | Existing Compose/Kind fixtures include development HTTP paths; provision and verify production boundaries separately. |

The Kind Connector channel manifests use these exact namespace-separated Secret contracts. Obtain **separate** server/client CA and identity files through the workload PKI first; they are not generated by the earlier messaging-client example. Create the namespaces if needed:

Assemble the combined client identity from an issued certificate chain and key in Bash:

```bash
umask 077
cat private/sre/connector-client-cert.pem private/sre/connector-client-key.pem > private/sre/connector-client-identity.pem
```

Then create the channel Secrets:

```bash
kubectl -n rocketmq-sre create secret generic rocketmq-sre-control-plane-channel-server --from-file=control-plane-server-cert.pem=private/sre/control-plane-server-cert.pem --from-file=control-plane-server-key.pem=private/sre/control-plane-server-key.pem --from-file=connector-client-ca-cert.pem=private/sre/connector-client-ca-cert.pem
kubectl -n rocketmq-system create secret generic rocketmq-sre-control-plane-channel-client --from-file=control-plane-server-ca-cert.pem=private/sre/control-plane-server-ca-cert.pem --from-file=connector-client-identity.pem=private/sre/connector-client-identity.pem
```

In the supplied Kind stack, the Control Plane sidecar reads its server Secret at `/etc/rocketmq/sre-channel` and the Connector reads its client Secret at `/var/run/secrets/rocketmq-sre-channel`. The HTTPS channel is `https://sre-control-plane.rocketmq-sre.svc.cluster.local:8444`; include that DNS SAN. Its Connector-only Axum backend listens on loopback 8093, and NetworkPolicy prevents bypass.

The supplied Nginx fixture pins `CN=rocketmq-sre-connector` and `CN=RocketMQ SRE Connector Development CA`. For production, align the issued subject/issuer, Nginx allowlist, Connector configuration, and Control Plane binding together. Do not reuse the development CA. Mount only MCP's public CA into Connector, never its server private key. Existing SRE scripts create additional OAuth/Bearer/ACL/bootstrap Secrets; the two channel Secrets do not replace them. Follow [AI SRE deployment](../ecosystem/ai-sre-deployment.md) and the [channel contract](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/docs/control-plane-connector-mtls.md).

## Dashboards, databases, and telemetry {#dashboards}

For Web Dashboard, configure three separate paths:

1. **Browser HTTPS:** terminate TLS at an HTTPS reverse proxy/Ingress serving the frontend and forwarding `/api` to the private backend. Keep `DASHBOARD_WEB_SESSION_COOKIE_SECURE=true` and the exact allowed origin. The Axum backend does not gain HTTPS from `DASHBOARD_WEB_USE_TLS`.
2. **RocketMQ remoting:** set `DASHBOARD_WEB_USE_TLS=true`, configure the process's Admin trust/client identity properties, and provide separate RocketMQ ACL credentials. GPUI/Tauri use their own connection TLS settings and the local OS trust store or transport properties file; a TLS status badge alone does not prove a handshake.
3. **SQL:** select the storage backend and use `DASHBOARD_WEB_DATABASE_URL_FILE` for a mounted URL. MySQL requires `ssl-mode=verify_identity` plus `ssl-ca`; PostgreSQL requires `sslmode=verify-full` plus `sslrootcert`. The URL host must match the database certificate. File/SQLite storage has no database TLS connection.

Example Secret delivery for a custom Dashboard workload, after obtaining its HTTPS certificate, database CA, and a private URL file:

```bash
kubectl -n rocketmq create secret tls rocketmq-dashboard-https --cert=private/dashboard.crt --key=private/dashboard.key
kubectl -n rocketmq create secret generic rocketmq-dashboard-db --from-file=database-url=private/dashboard-database-url --from-file=ca.crt=private/database-ca.crt
```

Reference `rocketmq-dashboard-https` from the actual Ingress/gateway configuration. Mount the database Secret into the backend, for example at `/run/secrets/dashboard-db`, and set `DASHBOARD_WEB_DATABASE_URL_FILE=/run/secrets/dashboard-db/database-url`. The private URL must reference `/run/secrets/dashboard-db/ca.crt` through the driver's CA parameter. Do not define both URL and URL_FILE, and never place secrets in frontend `VITE_` variables. See [Web Dashboard deployment](../ecosystem/dashboard-web.md) and [SQL TLS examples](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-web/docs/storage-deployment.md).

Prometheus, health endpoints, runtime diagnostics and OTLP have separate listeners/exporters. Protect exposed HTTP endpoints through a configured gateway or private workload boundary. The shared observability config exposes endpoint/protocol/headers, not a universal custom-CA/client-key TLS block. Do not invent `observability.otlp.tls` fields or assume every `https://` exporter is supported by every build. A local collector behind a verified TLS boundary can provide remote export; configure its own CA/identity and verify it independently. Consult [observability configuration](../configuration/observability.md).

## Verify, rotate, and troubleshoot {#verification}

Verify the certificate handshake first, then the actual protocol and permissions. For Proxy gRPC mTLS:

```bash
openssl s_client -connect proxy.mq.example.test:8081 -servername proxy.mq.example.test -CAfile private/ca.crt -cert private/client.crt -key private/client.key -verify_hostname proxy.mq.example.test -verify_return_error -alpn h2 </dev/null
```

Check the exit status, certificate verification result and negotiated `h2`. Use the corresponding NameServer/Broker endpoint for a remoting handshake check, without the gRPC ALPN requirement. Use `-verify_ip` for an IP endpoint. A successful TLS handshake does not prove Broker registration, route discovery, a gRPC API call, message delivery, or authorization. Run a real permitted operation and confirm that plaintext, an untrusted CA, a wrong SAN, and a missing/untrusted client certificate are rejected where policy requires. For OAuth endpoints, verify missing/wrong-scope tokens fail too.

| Component | Rotation behavior |
| --- | --- |
| Transport-backed NameServer/Broker and MCP HTTPS | Polls server certificate/key/CA and configured TLS properties, retaining the previous acceptor on failure. Base enable/mode require process reconfiguration/restart. New connections use the new acceptor; existing connections do not re-handshake. |
| Proxy gRPC | Polls file metadata at `reloadIntervalMs`; rejected candidates retain the previous generation. An unchanged rejected candidate is not continuously retried; correct/update its files. |
| Remoting client | New connection builds TLS from current material; established connections retain their handshake. Reconnect or restart through the normal owner; there is no server-style client polling task. |
| SRE proxy, provider clients, SQL pools and HTTPS gateways | Use that component's reload/restart procedure. Existing HTTP clients, pooled database connections, or environment-injected credentials may need recreation. |

Rotate with trust overlap when the component supports it: add the new CA to receiving trust bundles, deliver the new identities, confirm volume projection and successful reload/restart, establish new connections, test allowed and rejected operations, then remove the old CA/identity. Keep CA/key/cert updates coherent and monitor rejected reload candidates. Do not delete the only working identity before proving the replacement.

| Symptom | Check |
| --- | --- |
| TLS configured but service fails startup | Correct feature, PEM/key pair, permissions, server mode, client CA, and any properties override. |
| Unknown issuer / wrong certificate name | Actual CA bundle, full intermediate chain, route-advertised address and DNS/IP SAN. |
| TLS Proxy works but Broker query fails | Different protocols and outbound connections; inspect the component map before assuming encryption or compatibility. |
| Pod stays Pending / ContainerCreating after Secret setup | Namespace, Secret name, required key names and volume items; no key material needs to be printed. |
| Secret changed but old certificate remains | Projection delay, `subPath`, failed candidate, existing connection, or component needing restart. |
| SRE channel registration fails | Separate server/client CA, combined identity, DN allowlist, Bearer token, DNS SAN, HTTP/2 and backend isolation. |
| Dashboard UI works but login/SQL fails | HTTPS Secure cookie/origin and independent database trust/URL settings. |

This guide documents source-defined wiring and deployment examples; it does not certify a live full-cluster TLS or certificate-rotation exercise. Implementation references: [transport TLS](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-transport/src/tls.rs), [transport configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-transport/src/config/tls_config.rs), [Proxy TLS reload](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/grpc/tls_acceptor.rs), [Core chart](https://github.com/mxsm/rocketmq-rust/tree/main/distribution/helm/rocketmq-rust-core), [MCP configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp/conf/mcp.example.toml), [Control configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp-control/conf/mcp-control.example.toml), and [OpenSSL verification options](https://docs.openssl.org/3.5/man1/openssl-s_client/).
