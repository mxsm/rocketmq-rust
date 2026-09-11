# Topic filtering

Broker, Cluster, and message type selectors come from the current catalog. Broker and Cluster use exact, case-sensitive array membership; free-text search retains case-insensitive substring matching across the existing name, category, message type, Broker and Cluster fields. All conditions compose with the category toggles.

Changing a filter returns to page one and preserves the selected Topic identity. If it is filtered out, details are hidden and an explicit message offers clearing filters or selecting a different row. A removed Topic is not silently replaced by the first remaining row. A removed filter value remains visible as absent from the current catalog instead of silently broadening the query. Navigation history preserves filter state. Changing Cluster explicitly resets the dependent Broker filter.

The directory and selected details share one page. Overview, Routes, Statistics,
Consumers, and Configuration read only their current target. Queue statistics
have a local Broker/queue filter; configuration can compare available Brokers or
read a selected Broker. Slow reads from an old Topic, tab, or connection cannot
replace the current detail region. A failed refresh retains the last successful
data and does not advance the successful page refresh time.
