# Desktop audit history

Audit records cover login, logout, password changes, account session revocation,
NameServer/Proxy changes, Topic and Consumer mutations, sends, direct consumption,
and DLQ resends. Read-only business queries are not recorded individually.

The Audit navigation entry supports an inclusive time range, exact actor/action/
outcome/environment filters, refresh, and cursor pagination ordered by terminal
time and event ID. Environment IDs are absent until connection identity support
is introduced; old records are not assigned a guessed environment.

Each accepted mutation belongs to the audit service task group. Closing a page or
dropping the IPC waiter does not drop the operation or its terminal record.
Shutdown first stops audit admission and drains accepted mutations, then drains
storage and closes administrative sessions. Incomplete shutdown is reported.

Records contain generated event/request IDs, the authoritative authenticated actor
(or no actor for failed authentication), action, resource type/name where known,
outcome, terminal time, and an explicit allowlist of receipt counts and safe error
codes. Passwords, keys, tokens, message bodies, raw errors, and complete requests or
responses are never serialized into audit detail. Endpoint text is omitted until
stable endpoint identifiers are available.

Outcomes distinguish success, rejected, failed, partial, and unknown. Remote errors
that do not establish a validation/authentication rejection are recorded as unknown;
inspect the resource before deciding whether to resubmit. Batch receipts preserve
success/failure counts rather than equating every successful IPC response with a
fully successful operation.

Account mutations commit their success record in the same SQLite transaction. If
recording fails, the account mutation rolls back. Connection configuration still
uses its existing store; its versioned transaction integration follows with the
connection settings work. For separately committed configuration or remote changes,
a failed audit write preserves the actual command result and adds `auditWarning`.
The frontend displays a persistent, dismissible warning. It does not retry the
operation. An operation error also remains the original error if its audit write
fails.

Retention runs at startup and hourly, deleting at most 1,000 terminal records older
than 30 days per pass. The cleanup uses the storage-owned background task group.
The current schema is version 3; older development schemas are rejected without
modification. Select a fresh data directory when moving between these development
formats.

## Focused validation

From `src-tauri`: `cargo test --lib audit::`, plus affected auth/storage/error tests.
From the Tauri frontend: `npm run build` and
`npm test -- src/services/auth-session.test.ts src/services/invoke.test.ts`.
