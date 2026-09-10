---
title: "Configure deployment security"
---

Treat each connection as a separate deployment boundary: client to NameServer, client to Broker, client to Proxy, Proxy to Broker, and the Controller management and peer connections. Choose encryption, authenticated identity, and operation permissions for each exposed path. Read [security design](../architecture/security.md) for evaluation and provider semantics.

## Inventory the actual listeners

| Path | Configuration responsibility | Observation |
| --- | --- | --- |
| NameServer remoting | NameServer auth fields and the actual listener/transport deployment | Authorized route query succeeds; invalid credentials fail |
| Broker remoting | Broker auth section, client signer, reachable advertised address, actual transport TLS where selected | Authorized send/consume succeeds; another Topic/group is denied |
| Proxy gRPC | `grpc.tls` plus `auth` and Proxy method mapping | Certificate verification and application authorization both behave as intended |
| Proxy / Broker inner client | Outbound credentials and receiver ACLs | Registration, route discovery, and downstream requests succeed with the intended service identity |
| Controller remoting / Raft | Separate endpoints; management credentials and the actual peer transport/access boundary | Management access works for operators; untrusted networks cannot reach peer traffic |
| Admin and observability endpoints | Operator credentials, network exposure, restricted telemetry access | Read-only operators cannot perform mutations; telemetry is reachable only from intended collectors |

A dependency's TLS feature makes code available; it does not select or configure every listener. Do not infer remoting, HA, or Raft encryption from a successful Proxy TLS handshake. Protect paths without demonstrated TLS integration using an explicitly designed private network or external transport boundary, and validate that deployment separately.

## Configure service authentication and authorization

Use camelCase fields in TOML. The common settings below belong at the root for NameServer and Controller, under `[broker]` for Broker, and under `[auth]` for Proxy:

```toml
authenticationEnabled = true
authorizationEnabled = true
aclFile = "/etc/rocketmq/acl/plain_acl.yml"
authConfigPath = "/var/lib/rocketmq/auth"
```

Merge these fields into the existing service configuration at that scope; this fragment is not a complete standalone configuration. Give each process its own writable auth metadata path. Protect both the input ACL and generated `users.json`/`acls.json`. User snapshots retain the HMAC secret in plaintext.

An application ACL can begin with deny-by-default resource permissions:

```yaml
globalWhiteRemoteAddresses: []
accounts:
  - accessKey: docs-publisher
    secretKey: replace-with-a-private-secret
    admin: false
    defaultTopicPerm: DENY
    defaultGroupPerm: DENY
    topicPerms:
      - DocsProxyMessage=PUB
```

This example grants publishing only; it does not create a consumer, management, or internal replication identity. Define those identities separately from their actual request/resource mapping. Replace the placeholder using your private secret distribution process. Do not reuse a super-user credential in applications.

An IP whitelist match in the ordinary remoting path bypasses both authentication and authorization. A request-code whitelist bypasses its corresponding check. Keep these lists empty unless the intended bypass is understood. Timestamp skew validation alone does not provide replay protection.

## Wire outbound identities

For chart-managed Broker and Proxy processes, the inner-client Secret contains:

```json
{
  "accessKey": "site-service-identity",
  "secretKey": "replace-with-a-private-secret"
}
```

`ROCKETMQ_INNER_CLIENT_CREDENTIALS_FILE` selects that mounted JSON. An optional `securityToken` may accompany the key pair. The receiving service needs the matching identity and required permissions. Existing inline `innerClientAuthenticationCredentials` take precedence outside the chart, so remove a stale inline override when switching sources.

For Cluster-mode Proxy, enable root-level `enableAclRpcHookForClusterMode = true` as well as inbound auth. Missing or invalid outbound credentials cause initialization failure. Inbound client authorization does not supply the Proxy's downstream credentials.

Admin CLI reads `ROCKETMQ_ACL_ACCESS_KEY` and `ROCKETMQ_ACL_SECRET_KEY` together, with optional `ROCKETMQ_ACL_SECURITY_TOKEN`. Supply them through a protected process environment, then use the commands in [Admin operations](../operations/admin.md). Avoid embedding secrets in shell history or copying complete environments into support reports.

## Configure Proxy gRPC TLS

Build the selected backend with TLS, for example:

```bash
cargo build -p rocketmq-proxy --bin rocketmq-proxy-rust --no-default-features --features cluster-mode,tls
```

Merge this into the [Cluster-mode configuration](proxy.md), using real private paths and a certificate matching the endpoint's DNS name:

```toml
[grpc.tls]
enabled = true
certificatePath = "/etc/rocketmq/tls/tls.crt"
privateKeyPath = "/etc/rocketmq/tls/tls.key"
clientAuth = "require"
clientCaPath = "/etc/rocketmq/tls/ca.crt"
reloadIntervalMs = 5000
```

`none` does not request a client certificate. `optional` verifies a certificate when supplied. `require` rejects a connection without a certificate signed by the configured CA. The latter two require `clientCaPath`. A TLS identity is not automatically permission to publish, subscribe, or administer; keep application authentication/authorization configured.

Clients must trust the service CA and validate the endpoint name. Use a client certificate/key for required mTLS. The plaintext `grpcurl` probe in the loopback tutorial must be adapted to the client's TLS and authentication settings; do not retain `-plaintext` for this endpoint.

The built-in TLS acceptor polls certificate/key/CA file metadata at `reloadIntervalMs`, which must be positive. A valid changed generation is swapped for new connections. A rejected generation keeps the last working acceptor and logs a warning. Existing connections do not redo their handshake. A failed candidate is observed before parsing; unchanged bad files are not continuously retried. Correct the mounted files and verify a new generation and a fresh client connection, rather than waiting indefinitely on an unchanged failed candidate.

## Select the process bootstrap profile

The [local tutorials](../getting-started/local-source.md) explicitly use `ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback`. All listeners supplied to its validation must be loopback. Changing a bind address to `0.0.0.0` requires a deliberate shared-network deployment design; this development profile is not a production shortcut.

`secure-enforced` requires these environment inputs:

| Variable | Required material |
| --- | --- |
| `ROCKETMQ_SECURITY_TRUST_ANCHOR` | Trust-anchor file |
| `ROCKETMQ_SECURITY_TLS_CERT` / `ROCKETMQ_SECURITY_TLS_KEY` | Certificate and private-key files |
| `ROCKETMQ_SECURITY_SECRET_PROVIDER` | `mounted-files` |
| `ROCKETMQ_SECURITY_ADMIN_IDENTITY` | Administrator identity file |
| `ROCKETMQ_SECURITY_REQUEST_POLICY` | Request policy file |

Use the files expected by your integrated security boundary; empty placeholder files are not an enrollment procedure. Secure mode also rejects disabled service authentication or authorization. With no profile and no bootstrap material, bootstrap is disabled; supplying material without an explicit profile is rejected.

This startup check validates material and configuration. It does not construct a TLS listener or install all request-policy adapters. The core Helm chart's `securityProfile` is a distinct selector for auth defaults. Persistent one-time administrator bootstrap and the encrypted-file development-secret adapter currently fail closed on Windows because their equivalent filesystem ACL verification is not implemented; this does not imply that ordinary remoting auth is unavailable on Windows.

## Rotate material and verify behavior

1. Identify every receiver and outbound caller using the old identity or certificate, including Admin and internal clients. Determine whether the deployed path supports overlapping identities/trust roots.
2. Distribute the next material privately. Update receiver permissions/trust before switching callers when the deployment supports overlap. Do not remove the only working operator credential prematurely.
3. Apply the path's actual reload or restart behavior. ACL watching is optional and disabled by default; startup seeds are not a rotation interface. The core chart's documented rotation procedure uses controlled restarts of affected processes. Proxy TLS has the file reload behavior above, but observing a Secret update alone does not prove it activated.
4. Test a fresh connection with new credentials, a permitted operation, a forbidden resource, and a rejected identity. Inspect downstream calls as well as ingress. Remove old access after callers have transitioned, then verify it is rejected.

ACL read/parse/validation failures preserve previous imported state, but the actual multi-record import is sequential rather than an atomic transaction. Inspect reload errors and actual permissions before declaring rotation complete. Keep protected request work alive until drained, then close auth providers before the owning runtime exits.

These are deployment instructions based on current code; they are not a claim of a completed security or credential-rotation exercise.

## Source map

[Auth configuration and semantics](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-auth/README.md), [bootstrap profile](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-security-api/src/secure_deployment.rs), [Proxy TLS configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/src/config.rs), [TLS reload implementation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/grpc/tls_acceptor.rs), [core chart credential wiring](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/helm/rocketmq-rust-core/README.md).
