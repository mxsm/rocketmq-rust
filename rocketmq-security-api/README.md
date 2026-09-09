# rocketmq-security-api

Shared security contracts used by transport, authentication, storage, and deployment entry points. This crate has no Tokio dependency; it defines policy/value boundaries and synchronous bootstrap checks rather than a running authentication service.

## Responsibilities

| API | Purpose |
| --- | --- |
| `PeerInfo`, `SecurityRequestView`, `RequestContext`, `AuthenticatedRequestContext` | Borrowed request/peer projections and authenticated context. |
| `Principal`, `Resource`, `ResourceKind`, `Action` | Security identities and action/resource vocabulary. |
| `RequestPolicy`, `OutboundSigner` | Inbound policy and outbound signing contracts implemented by integrations. |
| `combine_layered_authorization` | Combine ingress and detailed authorization decisions with explicit required-layer failures. |
| `SecretProvider`, `SecretMaterial`, `VersionedSecret` | Provider capabilities and secret access/versioning contracts. |
| `MaintenancePolicy`, `ValidatedMaintenancePolicy`, `MaintenanceAuthorizer` | Validate and authorize privileged maintenance capabilities, principal bindings, and budgets. |
| `SecurityBootstrap` and deployment validation types | Validate selected deployment profiles, required material, and listener constraints. |

`Secret<T>` redacts formatting; it does not promise zeroization for arbitrary `T`. `SecretMaterial` owns a zeroized byte buffer and redacts its debug representation. Callers must still protect values obtained through explicit accessors.

## Deployment bootstrap

`ROCKETMQ_SECURITY_PROFILE` selects `development-insecure-loopback` or `secure-enforced`. The development profile requires supplied listeners to use loopback addresses. Secure bootstrap requires trust-anchor, TLS certificate/key, mounted-file secret-provider, admin-identity, and request-policy material. See [`secure_deployment.rs`](src/secure_deployment.rs) for the exported environment variable names and checks.

With no profile and no bootstrap material, bootstrap returns the disabled outcome. Material without an explicit profile is rejected. Bootstrap validation does not install TLS or implement request authentication; applications must connect their transport and policy implementations. [rocketmq-auth](../rocketmq-auth/README.md) provides the broker/proxy auth runtime.

## Validation

Default features are empty. Run from the root workspace:

```bash
cargo test -p rocketmq-security-api
```

The source modules include tests for layered decisions, maintenance policy validation, secret contracts, and secure deployment checks. Licensed under [Apache-2.0](../LICENSE-APACHE).
