# Consumer Monitor rules (S22)

The Monitors page manages nonnegative Consumer group thresholds in the selected
NameServer environment. Rules do not evaluate alerts, send notifications, or
perform automatic recovery. No Broker mutation is involved.

Creation expects rule revision zero. Updates and deletes must match the stored
revision. On conflict the page reloads the current list and retains the draft;
choose Edit on the current row to review that version before saving. Connection
revision leases prevent accepted changes from moving to another environment.
Each successful mutation and its audit event commit in the same transaction.

The fresh database schema includes the rules table. Older formats require a new
data directory; no migration or compatibility layer is provided.
