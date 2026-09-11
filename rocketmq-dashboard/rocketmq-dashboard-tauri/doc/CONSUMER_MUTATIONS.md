# Consumer mutation results

Consumer writes reject protected raw and normalized names before opening an admin session. The backend uses the admin-core classifier; list and configuration views keep system groups read-only.

Create/update uses the validated Consumer batch API without changing configuration or retry defaults. Cluster selections expand to their Brokers and explicit Broker selections are added to that set. Deletion discovers current group membership through the Consumer workspace inventory. Inventory failures block deletion; missing client or progress data does not imply missing subscription metadata. Only selected Brokers are passed to the batch deletion API. Internal retry/DLQ Topic cleanup runs only after every authoritative Broker is selected and successfully deleted.

The IPC result contains `operation`, `consumerGroup`, `targets`, `targetCount`, and `success`. Targets preserve Broker and internal-Topic-cleanup outcomes independently. Raw backend errors are replaced with a stable safe code and message. Audit counts reflect partial results.

Editor and Delete dialogs require an explicit target review before submission and retain successful, partial, or failed outcomes until dismissed. A cluster selection expands to a visible list of explicit Broker names before review; the submitted request carries no cluster expansion. Clearing the last Broker is invalid. Editing starts from a successfully read, explicitly selected Broker configuration, never from fallback defaults after a failed read.

Reviewing failed Brokers refreshes current target state and selects only failures from the preceding submitted target list. Earlier receipts remain visible, successful Brokers are excluded, and another explicit review and confirmation are required. Cleanup-only failures do not offer a Broker retry. Unconfirmed transport outcomes lock the submitted form; inspect current state before opening another operation.

The authenticated session owns operation dialogs outside the page navigation lifetime. While a write is pending, dismissal and duplicate submission are blocked. Changing the connection or Consumer query scope freezes the original dialog but retains its accepted write acknowledgement. Further reads or writes against a new context are blocked. Session disposal invalidates old callbacks and removes the old receipt. Closing a dialog refreshes its original page only if that page is still mounted in the same connection revision.

## Validation

- Backend Consumer tests cover protected-name validation, incomplete inventory rejection, result mapping, and error redaction.
- Admin-core Consumer batch regressions cover unknown targets, subset deletion, partial failures, and cleanup results.
- Frontend receipt and model tests cover mixed results, cleanup isolation, protected groups, explicit target validation, integer limits, source selection and failed-subset review. Shared operation-controller regressions cover stale reads, accepted writes, disposal and concurrent submission. Browser fixture checks exercise the actual forms and focus behavior; real-cluster acceptance is a separate integration check.
