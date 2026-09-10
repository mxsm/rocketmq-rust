# Reviewed Consumer offset reset

Topic operations and the Consumer detail Reset offset tab use the same form. The detail flow fixes the exact Consumer group and gets Topic choices from its progress. System groups are read-only. The existing audited reset command retains current connection revision and protected Topic checks, and now also rejects protected/blank groups and negative reset timestamps before opening an admin session.

Enter a local date and minute and review the Topic, group, displayed timezone, millisecond timestamp, and force flag. Invalid dates (including nonexistent local times) cannot be submitted. Changing inputs clears confirmation; changing the entity/scope or closing the form disposes pending callbacks. Accepted remote work can finish, but cannot refresh another group's page.

After the reset returns, keep its receipt and query progress for the submitted group and Topic. A failed readback preserves the write receipt and explicitly reports the unavailable progress. The form does not automatically repeat a reset.

Skip accumulated messages uses the admin client's dedicated skip capability. It does not pass the old `-1` through an unsigned timestamp, which the checked client conversion rejects. Timestamp-based resets keep their nonnegative millisecond contract.
