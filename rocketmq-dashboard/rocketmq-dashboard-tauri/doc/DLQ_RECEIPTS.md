# DLQ query modes and resend receipts

Choose one explicit mode. Message ID performs a detail lookup. Key takes precedence over time paging when a nonblank `key` is supplied to the DLQ page command; it returns at most 64 matches across the DLQ topic, ignores time/page cursor inputs, and returns no task ID. Blank/absent Key retains the existing time-range page contract. A new time search starts with no cursor; only subsequent time pages reuse it. Changing query inputs invalidates older query responses.

Single and batch resend accept optional `clientId`. Missing or blank values preserve automatic client selection. Before direct consumption the backend loads the actual DLQ message, checks that it belongs to the selected group's DLQ, and derives the business Topic and original message ID from its properties. Missing origin metadata or a retry/DLQ original Topic is rejected. Requests cannot nominate an arbitrary original business Topic. A batch is limited to 256 unique group/message identities.

Each receipt retains `requestMessageId` (the selected DLQ identity) separately from the original `msgId`, plus group, Topic, success, consumeResult and safe remark. The session-owned action provider preserves the latest completed receipt across query, group, page and connection changes. It records the original environment, connection revision, requested IDs and Client ID. A new completed resend replaces the latest receipt; signing out or changing session clears it. Receipts are not written to browser storage.

Before opening an operation, the selected records' connection context is checked against current settings. Before submitting, the dialog checks that context again. Once a resend is accepted, its result remains owned by that dialog even when the connection changes. Submitting disables duplicate attempts and prevents closing the pending dialog. A network failure does not trigger a retry.

The result table matches each requested group and DLQ ID to exactly one `requestMessageId` acknowledgement. A different original ID is expected; it cannot substitute for the request identity. Duplicate, missing, wrong-group or contradictory acknowledgements remain unknown. A success requires a successful flag, a recognized success consume result and a non-DLQ original identity. Recognized negative consume results are shown as failed. Transport failures and missing consume results remain unknown even when the backend aggregate counts them as failures. The reported safe response is separately inspectable.

**Review failed targets** selects only confirmed failed identities currently visible in the same group and environment. It excludes successes and unknown outcomes and sends nothing. Resend still requires explicit confirmation including the chosen Client ID. Changing a query or page clears selection; manually changing selection clears its review note. Accepted receipts are independent of this query state.

Single and selected-message batch CSV exports remain available, including Key and ID result modes. Export scope is the selected messages, not an unbounded full DLQ export. The export dialog displays partial counts, then offers the returned CSV for download. A connection change invalidates an unfinished export read. **Export results** downloads the latest resend receipt without contacting a Broker; CSV cells are quoted and formula-prefixed values are neutralized. The UI reports a download request, not proof that an operating-system save completed.

## Layout and identity

The dark page uses a query toolbar, selectable results table, batch action bar and persistent result panel. Consumer group input remains available when catalog discovery fails. Time queries use the existing local-date validation and page controller; changing scope invalidates old responses, while a failed refresh preserves the last successful observation and disables stale row actions.

Summary DTOs do not report original Topic or reconsume count. The table therefore labels its actual DLQ Topic and request ID; it does not fabricate the corresponding mockup fields. Inspection preserves summary tags, keys and displayed ID even if a fresh detail read fails. Body, Properties and Delivery use the shared message inspector with a DLQ-specific detail lookup. Original Topic/ID properties and reconsume count are shown only when returned. Generic direct-consume and Trace actions are omitted from this inspector so a DLQ record cannot bypass origin resolution through those entry points.

## Validation

- `npm run build`
- `npm test -- --run`
- Focused regressions cover query normalization, time cursor reuse, stale responses, physical IDs, target-context capture, receipt identity, unknown outcomes, failed selection and CSV quoting.
- Browser checks cover query modes, catalog/read failures, selection, fixed-target confirmation, per-message results, navigation/session isolation and exports. Final visual comparison, Windows WebView2, native downloads and real RocketMQ-Rust DLQ consumption must be recorded separately; a mock response cannot prove delivery to a real Consumer.
