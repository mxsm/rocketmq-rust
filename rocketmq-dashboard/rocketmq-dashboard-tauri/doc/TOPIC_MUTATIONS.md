# Topic mutations and send receipts

The desktop `create_or_update_topic` command requires an explicit `mode` of
`create` or `update`. No implicit upsert path remains. A fresh, unfiltered
authoritative catalog rejects duplicate creates, missing updates, and all system
Topic changes. Reserved system names are also protected before creation. Edit,
send, offset reset/skip, whole-Topic deletion, and Broker-specific deletion share
this policy. Current targets are revalidated before dispatch.

A dedicated mutation session serializes the check and execution sequence within
this app while the existing read session remains independent. The connection
revision lease prevents changing environments during an accepted mutation.
External administration can still race with a catalog query; this is not a
distributed transaction or a cross-Broker rollback guarantee.

Send receipts include `success` and a normalized `sendStatus`. Only `SEND_OK`
counts as success. Timeout, unavailable-replica, and unknown statuses preserve
message ID, Broker, queue, offset, and transaction metadata for review. The UI
keeps the receipt visible and does not automatically send again. Closing the
dialog or changing its Topic invalidates its pending UI callbacks.

Admin-core's normal and transaction test Producers inherit the administration
session's signing hook. Both disable automatic send retries and switching Brokers
after a non-store-success response. Their session owner still awaits Producer
shutdown. Credentials stay in backend memory.

Validation includes fake-catalog mutation rejection, stale target checks,
normalized send-result tests, and the isolated ACL fixture's signed normal and
transaction sends. Run `cargo test --lib topic::` and the opt-in
`cargo test --lib local_acl_query -- --ignored` from `src-tauri`, plus the frontend
build. The ACL test creates unique temporary Topics and deletes them after sending.
