# rocketmq-sre-client

Read-only Rust HTTP client for the AI SRE Control Plane. This package belongs
to the [standalone SRE workspace](../../README.md), not the root workspace.

## API and boundaries

Use the fallible `Client::builder(base_url)`, configure `bearer_token`,
optional `allowed_clusters`, `timeout` and `max_response_bytes`, then call
`build()`. Async methods require the caller's Tokio runtime; the library
does not create or own a service runtime.

| Method | Read |
| --- | --- |
| `status` / `readiness` | Process liveness / dependency readiness |
| `openapi` | Versioned OpenAPI document |
| `clusters` / `cluster` | Authorized cluster list / one cluster |
| `incident` | Incident with bounded investigation context |
| `inspection` | Inspection and recommendations |
| `plan` | Typed Action Plan and its status |

All remote requests are GETs. There is no arbitrary request, approval,
execution or target mutation API. The optional client-side cluster allowlist
narrows scope; an explicitly empty set denies all cluster-scoped reads.
Server authorization remains authoritative.

The default timeout is 15 seconds and the decoded response limit is 4 MiB.
Redirects are disabled. Base URLs accept HTTP(S) and reject embedded
credentials, query parameters and fragments. Use HTTPS for remote deployments;
the builder does not restrict HTTP to loopback. Bearer headers are marked
sensitive, and `ClientFailure` exposes stable public failure information.

See [src/lib.rs](src/lib.rs) for methods and response projections.

## Validation

Run from `rocketmq-ai/rocketmq-sre`:

```bash
cargo fmt -p rocketmq-sre-client -- --check
cargo test --locked -p rocketmq-sre-client
```

[Apache License 2.0](../../../../LICENSE-APACHE).
