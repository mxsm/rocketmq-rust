# Message trace inspection

The Trace page uses the shared desktop theme and presents a query above an event
timeline and the selected event's detail. It preserves the existing authenticated
Tauri commands; no tracing collector or retry operation is introduced.

## Query identity

- ID mode calls `query_message_trace_by_id` with the selected Trace Topic and the
  producer unique message ID.
- Key mode calls the existing business Topic/Key lookup, then lets the operator
  inspect each returned unique identity. Physical copies with the same Topic and
  unique ID share a trace candidate. The Key lookup retains its existing limit of
  64 indexed messages.
- Event detail calls `view_message_trace_detail` with the candidate's `msgId`,
  not its physical `queryMsgId`. The returned unique ID, Trace Topic and any
  reported business Topic must match the selection.
- Topic discovery supplies suggestions only. Manual input remains available when
  discovery is loading, fails, or does not include a custom Trace Topic.
- Changing mode, inputs or connection context invalidates previous reads.
  Duplicate candidate requests are coalesced. A failed same-query refresh retains
  its last successful observation with a visible error and paused message navigation.

## Events and navigation

All events, Consumer groups and Transactions use the returned timeline, grouped
consumer nodes and transaction nodes respectively. There is no inferred delivery
result when a view is empty. Unrecognized status values remain explicitly unknown.
Event timestamps retain milliseconds and identify the local time zone; absent or
invalid timestamps are not replaced with the current time. Reported zero costs
and retry counts remain zero.

The DTO has no event ID. Selection uses the reported event fields and an occurrence
number for identical duplicates. Inserting unrelated events does not move selection.
If the selected record disappears or its fields change, the operator must choose
an event again. Identical duplicates cannot be distinguished beyond their returned
occurrence order.

Message and producer metadata remain available in the expandable summary.
Observed span is the span of returned timestamps, not end-to-end delivery latency
or evidence of complete trace coverage.

View message prefills the Messages ID query with the business Topic and unique ID;
it does not submit automatically. The action is unavailable without a reported
business Topic or while a read is pending or failed. Back restores the source
query draft. Copy ID reports success only after the clipboard write resolves.

## Validation

`traceModel.test.ts`, `traceQuery.test.ts` and navigation tests cover query fields,
identity validation, duplicate physical candidates, millisecond timestamps, unknown
states, selection stability, request coalescing, stale responses and navigation.
Browser checks exercise the real React components through isolated services.
Native Windows/WebView2 and real RocketMQ-Rust trace ingestion remain part of the
full dashboard integration acceptance; isolated browser data does not prove them.
