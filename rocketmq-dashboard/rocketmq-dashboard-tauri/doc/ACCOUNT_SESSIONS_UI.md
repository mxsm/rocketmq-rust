# Account sessions

Sessions shows the signed-in local account's safe session records, with a distinct Current badge. Created, expiry, last visit and revocation times come from the backend. Activity does not extend expiry; no fixed TTL is assumed. Expired and revoked records remain distinguishable. The table updates its expiry presentation without fetching or writing session data.

Only the public UUID record label is shown. Login tokens, token digests, devices, locations and other extra fields have no rendering path. Unknown or malformed metadata is not presented as an active session.

The list uses the backend's opaque cursors and reports only the current page and its record count. Toolbar Refresh returns to the first page. Failed reads remain retryable; a failed later page can return to the previous page. The same component works within Account with its own local refresh controls.

Sign out all sessions names the account and explicitly includes the current session. The dialog and accepted operation belong to the signed-in session, so page or environment changes do not retarget or duplicate the request. Pending confirmation cannot be dismissed. Failures retain a safe error in the dialog and do not perform an optimistic logout. A strict confirmed current-session revocation follows the existing authentication-invalid event. A late response for an older token cannot log out a newer login.

Validation covers expiry, redaction, cursor history and service-level success/failure/late-session behavior, plus shared read-resource, toolbar and navigation tests. Native session tests cover pagination, redaction, account isolation, expiry and authoritative revocation. External Chrome visual/keyboard comparison and the live Windows sign-out workflow remain separate acceptance evidence.
