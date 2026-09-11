# Consumer query scope

Consumer catalog, refresh, connection, progress, and topic-detail requests carry
an explicit `scope`: `{ mode: 'name_server' }` or
`{ mode: 'proxy', endpointId }`. Proxy mode reads the selected endpoint from
shared connection settings; there is no page-local address default or editable
query address. The backend verifies the endpoint is the current configured
Proxy before resolving its address for admin-core. A stale, unknown, or
NameServer endpoint ID is rejected. Revision checks still surround the query.

The mode preference survives module navigation and connection revision refreshes.
Each history entry retains its mode. Changing scope remounts the catalog and
its inline progress, connection and configuration views. Read resources reject callbacks from previous
scopes and disposed pages while coalescing concurrent reads. A missing Proxy prevents Proxy
queries and links to connection settings; NameServer mode remains available.

Consumer configuration reads still take a separate direct Broker address. The
DLQ group picker explicitly uses NameServer discovery. Neither treats a direct
Broker target as a Proxy scope.

The catalog preserves an explicitly selected group when filters or refreshes
remove it, showing a filtered-out or unavailable state instead of selecting a
different group automatically. Its zero progress/client defaults are not proof
of zero lag or an offline client: inspect the dedicated progress or connection
result for the current group. Refresh failures retain the last successful
observation time and block mutations based on stale catalog data.

Accepted write dialogs belong to the authenticated session. A scope or connection
change freezes further work in the dialog while preserving the original write
receipt; it does not retarget the request or silently discard the acknowledgement.

## Proxy settings workflow

The Proxy page separates the current endpoint, the saved endpoint list, and the
Consumer query mode. Choosing **Use** changes the saved selection without
changing the mode. **Open Consumers** starts a new catalog entry in the selected
mode, using the current endpoint identity for Proxy queries. The Consumer page
labels the actual source; a saved Proxy address alone does not imply Proxy mode.

Proxy refresh reloads connection configuration, not endpoint health. The page
coalesces configuration reads and rejects stale responses. An external revision
or a configuration conflict blocks further changes until the user reloads and
reviews the current settings. Add/delete dialogs preserve the proposed target;
reviewing settings does not automatically retry a rejected mutation. An accepted
change keeps its revision and receipt even when the following read fails.

Deleting the current Proxy uses the backend's returned selection (the first
remaining saved endpoint, or no selection). The confirmation identifies this
effect. Removing the last endpoint keeps the user's mode preference, disables
Proxy queries, and offers an explicit switch to NameServer mode. It does not
stop a Proxy process. Submission disables repeated actions and keeps the dialog
open until the mutation resolves.

Validation: `cargo test --lib consumer::` from `src-tauri`, and `npm run build`
plus focused Consumer scope, navigation, and authenticated invocation tests from
the app root. The development cluster's Proxy is `127.0.0.1:8080`; select it in
Proxy settings before enabling Proxy mode in Consumers.
