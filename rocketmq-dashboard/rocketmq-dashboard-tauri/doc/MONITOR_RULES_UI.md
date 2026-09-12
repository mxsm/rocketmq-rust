# Consumer monitor rules

The Monitors page edits local threshold configuration for the selected NameServer environment. It does not evaluate alerts or send notifications. The table reports consumer group, minimum online clients, maximum total lag, rule revision and last update time from the stored records.

## Editing and conflicts

Open one inline editor at a time. Cancel closes it before another rule is selected. Group names are immutable for existing rules. Thresholds accept whole numbers from zero through JavaScript's maximum safe integer; empty, fractional, exponential and out-of-range input is rejected. Group names use the backend's 255-byte UTF-8 limit and exclude whitespace and control characters.

Save sends the displayed expected rule revision. A version conflict retains the entered group, thresholds and old revision. The page invalidates any read started before the write, then reloads the current environment. Reviewing a successfully loaded current rule explicitly adopts its revision while keeping the entered thresholds. When the group has been deleted, the review action prepares a new rule with revision zero. Neither refresh nor review submits a write.

Delete requires a dialog showing the original environment, group and expected rule revision. It removes the saved rule only. The consumer group and messages are unaffected.

## Request ownership and outcomes

The signed-in session owns accepted mutations and the latest result. Leaving the route or changing the connection cannot relabel a receipt as belonging to another environment. Stale targets are rejected before dispatch; the existing authenticated command and backend mutation lease enforce the connection revision.

Only the documented local commit receipt is shown as committed. Conflicts are explicit; missing or failed acknowledgements remain unconfirmed and require reading the original environment before another attempt. Requests never retry automatically. Signing out clears the session's retained result.

Each mounted environment owns its list and observation time. Failed refreshes preserve the previous observation and disable editing until a current read succeeds. A completed write invalidates earlier list requests before refreshing. Drafts saved in navigation history are scoped by environment and connection revision.

## Validation

Run the frontend build and the focused monitor model/action tests, plus the existing read-resource, page-toolbar and navigation suites. Native storage coverage lives in `monitor::repository::tests::monitor_rules_reopen_isolate_compare_and_swap_and_audit_atomically`.

Chrome visual comparison, keyboard/focus checks, narrow-window layouts and real Tauri CRUD/conflict checks are separate acceptance evidence. Passing model tests does not establish those results.
