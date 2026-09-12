# Audit log

The Audit page queries the local audit log. Actor, action and environment are exact-value inputs; the connection toolbar does not implicitly restrict the results. The page has no fuzzy search or fabricated actor/action directory. Five supported outcomes can be filtered: success, rejected, failed, partial and unknown.

Times are entered in the computer's local time zone and converted to milliseconds. Invalid calendar values and reversed ranges are rejected. Boundaries are inclusive. Draft filter changes do not change the displayed query until Apply filters is selected.

Each applied query owns its result and opaque cursor history. New filters, Reset and toolbar Refresh start from page one. Previous uses the stored cursor history; Next uses only the cursor returned by the backend. The page displays its current page number without inventing a total. Applying filters during a pending read invalidates the previous request. Failed queries can be retried, and a failed later page can return to the previous page.

The selected record displays request ID, time, actor, action, resource type/name, recorded environment, outcome and the explicit safe detail fields. Missing environment and counts remain Not recorded; explicit zero stays zero. Invalid counts are Unknown. Only an error code matching the bounded identifier format is displayed. Raw exceptions, request bodies, passwords, tokens and extra detail fields have no rendering path. Unknown flags and unrecognized outcomes cannot render a success badge.

Frontend validation covers filter parsing, cursor reset/history, safe projection and rendered detail redaction, together with the shared read-resource/toolbar/navigation tests. Existing native tests cover exact filtering, stable cursor order for identical timestamps, safe terminal outcomes and audit persistence. Chrome layout/keyboard comparison and live Tauri interaction are separate acceptance work.
