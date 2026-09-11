# Consumer configuration summary

Configuration reads discover the current group through NameServer and query each Broker separately. This is a Broker configuration operation even when the Consumer list uses a Proxy. The existing single-Broker command remains available.

The overview retains every discovered target, its configuration or a safe error, and any Broker whose group inventory could not be discovered. `complete` is false for empty, failed, or incomplete discovery. `effective` contains only fields common to successful reads; these are not evidence of cluster-wide consistency when coverage is incomplete. `inconsistentFields` compares successful configurations, including subscriptions and attributes, excluding Broker identity. Subscription and attribute ordering does not create false differences.

The Configuration tab keeps the comparison table and the selected Broker's details in the page. Select a successful Broker to inspect its switches, numeric limits, retry policy, subscriptions and attributes, or filter its fields. A missing or failed selected target remains explicit instead of falling back to another Broker. Returned configuration must match the selected group, Broker name and address before editing is enabled.

The editor displays its source, requires a successful source read, and initially selects only that Broker with no cluster expansion. Changing the source replaces the draft. Any added target receives the source-derived edited values, and the target names remain visible before submission. Failed source reads cannot submit default values.

Validation covers differing Broker retry values with a failed third Broker, preservation of common values and individual configurations, empty and incomplete discovery, and a complete single-Broker summary.
