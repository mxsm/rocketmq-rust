# Message inspection

The Messages page uses separate Key, ID, and local-time queries. Topic discovery
provides suggestions; a failed discovery does not disable manual Topic input.
Only the selected mode's parameters are submitted. Changing an input invalidates
the previous result and pending response. Returning from Trace restores the query
draft; run it again to obtain a current result.

Key lookup returns up to 64 indexed messages. ID lookup calls the backend instead
of constructing a result row from the input. Time lookup uses the backend's page
and task ID; the task is reused only within the same Topic and time window.
Refreshing starts a new scan. A failed same-query refresh retains its previous
result and successful-read time with an error, and disables actions on old data.

## Record identity and content

Summary `msgId` may be a producer unique ID; `queryMsgId` identifies the physical
record used for detail lookup. Selection includes both IDs so separate physical
copies remain distinguishable. The detail must match the Topic and queried
physical ID, or carry the queried unique ID in `UNIQ_KEY` when resolving a unique
lookup. A mismatched response cannot become a direct-consume target.

Body, Properties, and Delivery tabs retain the returned payload, properties,
hosts, times, queue coordinates, flags, and Consumer delivery records. Long
content scrolls inside the detail. Payloads render as text; control and directional
characters are escaped for display. Copy writes the original value and reports
success only after the clipboard operation completes. Binary payloads use the
returned Base64 value; missing payloads and numeric zero remain distinct.

View trace prefills the producer unique ID when available and preserves the Topic
context. The operator still chooses the Trace Topic and starts the query. Physical
message IDs embed a Broker address that must be reachable from the application.

## Direct consumption

Opening Direct consume reads the current message and its reported Consumer
groups. Review shows the exact physical message ID, group, optional client, and
connection context. An empty client ID lets the Broker select an eligible client.
The operation can invoke a Consumer again for an already processed message.

The dialog prevents duplicate submission and cannot close while a request is
running. An accepted write and its receipt remain attached to the original
environment when connection settings change. Signing into another session
disposes that session's dialog. A missing acknowledgement is shown as unconfirmed
and is never automatically retried. Inspect the Consumer before starting another
operation. Returned target identity and consumption result remain visible even
when the response does not confirm success.

Authentication, connection revisions, mutation leases, and audit handling remain
in the existing IPC/backend paths. This layout change does not change wire or
storage contracts.
