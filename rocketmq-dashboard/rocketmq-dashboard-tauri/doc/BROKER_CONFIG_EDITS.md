# Broker configuration edits

The Cluster configuration inspector opens an editor that loads current string-valued properties. Review computes a patch containing only changed keys. Empty keys, non-string JSON values, unsupported key removal, and property-line injection are rejected. Confirmation shows the Broker identity and every submitted key/value change.

The authenticated command `update_cluster_broker_config` holds the current connection revision lease and verifies cluster name, Broker name, Broker ID, and address against current discovery before writing through `DashboardAdmin`. It submits the patch once, then reads configuration once. The result separates an acknowledged write from read-back states `confirmed`, `different`, and `unavailable`. A read-back failure never turns the acknowledged write into a failed write or triggers a retry. Audit records the write outcome and Broker address without configuration values.

The editor retains the result, requires a refresh after an unconfirmed outcome, and ignores callbacks after closure or Broker/environment changes. A successful read-back updates the inspector's displayed properties. Editing another change always requires a fresh confirmation.

Validation covers identity mismatch and invalid input without writes, changed-key projection, successful writes with failed read-back, and audit classification. Configuration is not a compare-and-swap API: another administrator may change the same key concurrently, and the editor reports differing read-back values when observed.
