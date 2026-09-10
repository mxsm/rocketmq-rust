# DLQ query modes and resend receipts

Choose one explicit mode. Message ID performs a detail lookup. Key takes precedence over time paging when a nonblank `key` is supplied to the DLQ page command; it returns at most 64 matches across the DLQ topic, ignores time/page cursor inputs, and returns no task ID. Blank/absent Key retains the existing time-range page contract. A new time search starts with no cursor; only subsequent time pages reuse it. Changing query inputs invalidates older query responses.

Single and batch resend accept optional `clientId`. Missing or blank values preserve automatic client selection. Before direct consumption the backend loads the actual DLQ message, checks that it belongs to the selected group's DLQ, and derives the business Topic and original message ID from its properties. Missing origin metadata or a retry/DLQ original Topic is rejected. Requests cannot nominate an arbitrary original business Topic. A batch is limited to 256 unique group/message identities.

Each receipt retains `requestMessageId` (the selected DLQ identity) separately from the original `msgId`, plus group, Topic, success, consumeResult and safe remark. Refreshing query results does not clear the receipt. Select only failed messages for review selects failed identities currently visible in the same group, excluding successful identities. It sends nothing; Batch resend still requires explicit confirmation including the chosen ClientId. A group change discards that group's display state and invalidates old mutation callbacks.

Single and selected-message batch CSV exports remain available, including Key and ID result modes. Export scope is the selected messages, not an unbounded full DLQ export.
