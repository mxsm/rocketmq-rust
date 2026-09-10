# Producer discovery

`list_producer_groups` is an authenticated, connection-revision-checked query backed by `DashboardAdmin::dashboard_list_producers`. Each item contains a group name and a reported connection count. The command does not manufacture Topic associations or client rows.

The directory loads on page entry and supports search, pagination, refresh, and group selection. Selecting a group fills the existing Topic + Group connection lookup; manual group entry starts empty and remains available. Changing either query field invalidates the prior result and any outstanding callbacks or delayed loading indicator. Closing the page also invalidates requests.

The directory and connection lookup have independent loading, empty, and error states. An item with a reported connection count of zero remains visible. Connection statistics stay unset until a scoped lookup returns.

## Coverage limitation

The current core discovery API aggregates Broker-reported connection counts and does not return coverage evidence. It can omit Brokers whose producer-table queries fail, and counts can overlap across Brokers. The UI therefore says "reported connections" and "no groups were reported" and explicitly states that coverage is unavailable. It does not claim a complete cluster client count or fabricate a partial-coverage percentage. Client details come only from the separate scoped lookup.
