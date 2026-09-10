# Consumer diagnostics

The client connection view exposes a manual diagnostic panel with separate Running information and Request thread stack actions. Opening the panel never requests JStack. Switching client, group, scope, or closing the dialog discards old results and invalidates late callbacks. Results remain in component memory only.

Both authenticated commands validate the current connection revision and the configured Consumer scope. NameServer mode checks the client in current connections before calling `ConsumerDiagnosticAdmin`. Proxy mode reports unsupported: that trait cannot express a forwarding address, so the application does not silently use NameServer routing for a Proxy selection.

Responses distinguish available, offline, unsupported, and unavailable. Broker response codes and canonical unsupported conditions are classified without exposing backend error detail. Absence from the fresh connection list is reported as offline. A returned running-info response without a requested thread stack retains the other sections and explains that JStack may be unsupported.

Properties retain the admin-core allowlist. Each of properties, subscriptions, process queues, and thread stack has a 60 KiB serialized budget, leaving envelope room within 256 KiB. Thread-stack truncation respects Unicode character boundaries and JSON escaping. The `truncated` marker is visible and also preserves upstream truncation or missing-section evidence. The IPC limit does not replace the transport decoder's own frame limit.
