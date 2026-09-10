# Consumer mutation results

Consumer writes reject protected raw and normalized names before opening an admin session. The backend uses the admin-core classifier; list and configuration views keep system groups read-only.

Create/update uses the validated Consumer batch API without changing configuration or retry defaults. Cluster selections expand to their Brokers and explicit Broker selections are added to that set. Deletion discovers current group membership through the Consumer workspace inventory. Inventory failures block deletion; missing client or progress data does not imply missing subscription metadata. Only selected Brokers are passed to the batch deletion API. Internal retry/DLQ Topic cleanup runs only after every authoritative Broker is selected and successfully deleted.

The IPC result contains `operation`, `consumerGroup`, `targets`, `targetCount`, and `success`. Targets preserve Broker and internal-Topic-cleanup outcomes independently. Raw backend errors are replaced with a stable safe code and message. Audit counts reflect partial results.

Editor and Delete dialogs close normally only after full success. Partial or failed outcomes remain visible. Reviewing failed Brokers first refreshes current target state, then selects only those failed Brokers; another explicit submission is required. Cluster selections are cleared for an editor retry so a failed subset cannot silently expand. Cleanup-only failures do not offer a Broker retry. Closing the dialog, changing the entity, or switching connection settings invalidates old callbacks.

## Validation

- Backend Consumer tests cover protected-name validation, incomplete inventory rejection, result mapping, and error redaction.
- Admin-core Consumer batch regressions cover unknown targets, subset deletion, partial failures, and cleanup results.
- Frontend receipt tests cover mixed results, cleanup isolation, and read-only groups; the production frontend build type-checks the integration.
