# Reviewed Consumer offset reset

Topic operations and the Consumer detail Reset offset tab share the date,
identity, and timestamp validation in `offsetReset.ts`. The Topic dialog has an
explicit Consumer group selection and a session-owned operation controller;
the Consumer detail flow uses `OffsetResetForm`, fixes the exact group, and gets
Topic choices from its progress. System groups are read-only. The existing
audited reset command retains connection revision and protected Topic checks,
and rejects protected/blank groups and negative reset timestamps before opening
an admin session.

Enter a local date and minute and review the Topic, group, displayed timezone,
millisecond timestamp, and force flag. Invalid dates (including nonexistent local
times) cannot be submitted. The Topic dialog locks inputs during review and
submission, prevents dismissal while an operation is pending, and retains
accepted write receipts across connection changes. A changed context prevents
progress reads against another environment. Session disposal invalidates pending
callbacks. The Consumer detail form remains keyed to its entity/scope.

After the reset returns, keep its receipt and query progress for the submitted group and Topic. A failed readback preserves the write receipt and explicitly reports the unavailable progress. The form does not automatically repeat a reset.

Skip accumulated messages uses the admin client's dedicated skip capability. It does not pass the old `-1` through an unsigned timestamp, which the checked client conversion rejects. Timestamp-based resets keep their nonnegative millisecond contract.
