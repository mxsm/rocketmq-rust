# Remoting GO_AWAY Retry Policy

`GO_AWAY` tells a response-aware client that the current peer-bound session
must not accept another request. `TransportClient` can perform one bounded
retry on a replacement session when its owner installs an explicit
`GoAwayPolicy`.

## Compatibility default

The policy is disabled by default. A disabled client returns `GO_AWAY` to its
caller exactly as it did before retry support was added. Enabling the policy
still does not retry every request: owners must provide a request-code
allowlist. Side-effecting operations should remain outside that allowlist
unless their application-level idempotency contract makes a duplicate attempt
safe. One-way requests never enter the response retry path.

## Attempt and deadline contract

A logical invocation has at most two wire attempts: the initial request and
one replacement-connection retry. Both attempts use the same immutable
`RequestDeadline`; reconnecting does not create a fresh timeout budget. A
second `GO_AWAY` is returned as a typed unexpected-response error. Failure to
reconnect, write, or receive within the remaining budget is returned directly.

The retry preserves the logical command's code, language, version, flags,
remark, serialization type, typed header, extension fields, and body. It uses
a newly allocated opaque so a late frame from the retired session cannot
complete the replacement session's pending future. Connection-bound signing
runs for each physical attempt, while business RPC hooks run once before the
logical invocation and once after its final successful response. The existing
failure contract does not run the after hook when the retry fails.

## Session ownership

After the first `GO_AWAY`, the producing session stops accepting new requests.
The endpoint registry uses compare-and-remove against an exact client-local session token,
so it cannot evict a replacement installed by concurrent work. The retired
session drains through the client's existing lifecycle-owned worker task group;
no detached runtime task is created. Its bounded drain window comes from the
pending table's maximum request age, not from the triggering request's shorter
deadline, so older in-flight requests keep their own budgets. Pending entries
remain scoped to their physical connection owner and are released on success,
timeout, retirement, or shutdown.

Transport telemetry emits low-cardinality `go_away` lifecycle outcomes:
`received`, `retry_success`, and `retry_failed`. It does not record bodies,
extension-field contents, credentials, or channel-sensitive data.
