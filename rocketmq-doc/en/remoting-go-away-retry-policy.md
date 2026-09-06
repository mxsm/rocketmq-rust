# Remoting GO_AWAY Retry Policy

`GO_AWAY` tells a response-aware client that the current peer-bound session
must not accept another request. Transport returns that response to its caller,
retires the producing session, and starts bounded lifecycle-owned draining. It
does not issue a replacement request or make a retry decision.

## Retry ownership

The Client retry policy receives `GO_AWAY` as a typed response at
`ResponseReceived`. It may start a new attempt only for an operation whose
idempotency contract permits replay and whose absolute deadline and attempt
budget still have capacity. Non-idempotent operations stop because the remote
business completion state is unknown.

Every retry is a new Client-owned attempt. Attempt accounting begins before
endpoint selection, DNS, and admission, and each attempt can issue at most one
primary wire request. Transport has no hidden replay path, request-code
allowlist, or independent retry budget.

## Deadline and request identity

All attempts and auxiliary refresh operations share the caller's immutable
absolute deadline. Reconnecting, refreshing routes, or switching brokers does
not reset that deadline. Auxiliary refresh requests are separately typed
operations and do not recursively invoke the primary retry executor.

When policy authorizes another attempt, the Client constructs and submits it
independently. Correlation ownership and connection-bound signing are
established for the selected session. The original `GO_AWAY` response remains
the terminal result when policy stops.

## Session ownership

After the first `GO_AWAY`, the producing session stops accepting new requests.
The endpoint registry uses compare-and-remove against the exact client-local
session token, so it cannot evict a replacement installed by concurrent work.
The retired session drains through the client's existing lifecycle-owned task
group; no detached runtime task is created. Its bounded drain window comes
from the pending table's maximum request age, so older in-flight requests keep
their own deadlines. Pending entries remain scoped to their physical
connection owner and are released on success, timeout, retirement, or
shutdown.

Transport telemetry records only the low-cardinality `received` lifecycle
outcome. Retry actions and results belong to Client retry observations. Neither
layer records bodies, extension-field contents, credentials, or
channel-sensitive data.
