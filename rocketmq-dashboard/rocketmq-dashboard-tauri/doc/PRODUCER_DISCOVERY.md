# Producer discovery

`list_producer_groups` is an authenticated, connection-revision-checked query backed by `DashboardAdmin::dashboard_list_producers`. Each item contains a group name and a reported connection count. The command does not manufacture Topic associations or client rows.

The dark desktop page keeps the searchable group directory beside the Topic + Group query and connection inspector. The directory loads on page entry and supports search, pagination, refresh, and group selection. Selecting a group fills the query without dispatching it. Both fields allow manual entry, including when directory discovery or Topic suggestions fail. Topic suggestions never replace an entered value. Query inputs and directory filters are saved with the navigation entry.

Changing either query field immediately clears the prior result and invalidates outstanding callbacks. Duplicate submits share one pending request. Closing the page or changing the connection context invalidates the lookup; a response must match the exact queried Topic and group before it is displayed. A same-input refresh failure preserves the last successful observation and its timestamp with an explicit error. An empty successful lookup and an unavailable lookup have different states.

The directory and connection lookup have independent loading, empty, and error states. An item with a reported connection count of zero remains visible. Connection statistics stay unset until a scoped lookup returns. The page refresh updates discovery, Topic suggestions and any previously submitted input pair; it does not start a new manual lookup. Client selection remains explicit if a refresh removes the selected client. Details expose returned language, version, address and scope; no client Last seen value is invented from the local read time.

## Coverage limitation

The current core discovery API aggregates Broker-reported connection counts and does not return coverage evidence. It can omit Brokers whose producer-table queries fail, and counts can overlap across Brokers. The UI therefore says "reported connections" and "no groups were reported" and explicitly states that coverage is unavailable. It does not claim a complete cluster client count or fabricate a partial-coverage percentage. Client details come only from the separate scoped lookup.
