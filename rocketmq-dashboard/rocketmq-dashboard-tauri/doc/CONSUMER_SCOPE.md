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
closes old detail dialogs. Request generations reject callbacks from previous
scopes, superseded refreshes, and disposed pages. A missing Proxy prevents Proxy
queries and links to connection settings; NameServer mode remains available.

Consumer configuration reads still take a separate direct Broker address. The
DLQ group picker explicitly uses NameServer discovery. Neither treats a direct
Broker target as a Proxy scope.

Validation: `cargo test --lib consumer::` from `src-tauri`, and `npm run build`
plus focused Consumer scope, navigation, and authenticated invocation tests from
the app root. The development cluster's Proxy is `127.0.0.1:8080`; select it in
Proxy settings before enabling Proxy mode in Consumers.
