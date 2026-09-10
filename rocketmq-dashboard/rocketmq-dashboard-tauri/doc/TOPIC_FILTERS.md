# Topic filtering

Broker, Cluster, and message type selectors come from the current catalog. Broker and Cluster use exact, case-sensitive array membership; free-text search retains case-insensitive substring matching across the existing name, category, message type, Broker and Cluster fields. All conditions compose with the category toggles.

Changing a filter returns to page one and selects its first matching Topic for an ordinary catalog view. A navigation target keeps its exact identity; if filtered out, its details are hidden and an explicit message offers clearing filters. A removed filter value remains visible as absent from the current catalog instead of silently broadening the query. Navigation history preserves filter state.
