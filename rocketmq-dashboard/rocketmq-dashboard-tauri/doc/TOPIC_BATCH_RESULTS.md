# Topic batch results

Create/update calls admin-core `TopicBatchMutationAdmin`; whole-Topic deletion
calls `TopicBatchDeleteAdmin`. Broker deletion uses the existing single-Broker
operation and projects the same receipt shape. Local policy and the mutation
session still guard all checks and writes.

Receipts contain operation, Topic, target count, target identities and kinds,
success, safe errors, and a separate `orderConfig` result. Only all-target success
with successful required order configuration produces overall success. Raw
upstream error text is not exposed. Audit records retain the success/failure
counts rather than flattening a partial operation into a successful action.

The current admin-core deletion contract returns Cluster-level outcomes. Those
rows are explicitly labeled `cluster`; the dashboard does not invent individual
Broker acknowledgments from a failed Cluster operation. Create/update and direct
Broker deletion use Broker rows. A Cluster failure may have partially changed
its Brokers and requires state verification before another deletion.

The shared receipt component keeps every result visible, including successful
targets and independent `ORDER_TOPIC_CONFIG` failures. An editor can load only
failed Brokers into a review draft. A partially successful create becomes an
explicit update draft; an entirely failed create retains create intent. Nothing
is retried automatically. A failed whole-Topic deletion can be reviewed one
failed Cluster at a time without repeating successful Cluster targets. The
success receipt stays visible until the user closes it.

Validation: focused backend Topic and audit tests, production frontend build,
and the receipt rendering tests. The fake batch projection checks one successful
and one failed target, all-success behavior, order-only failures, and error
redaction. This is not a cross-Broker transaction or rollback promise.
