# Runtime contract corrections (unreleased)

These changes are part of the current workspace's compatibility migration.
Consumers must migrate with this batch; the budget accessor change is a source
compatibility break and must not be published as a compatible patch to an API
that returns a mandatory capacity dimension.

## Closed dynamic budgets

`BudgetRejection::dimension()` now returns `Option<BudgetDimension>`.
`Some(Count | Bytes | Rate)` still means capacity exhaustion; `None` means
admission is closed. Prefer the exhaustive `reason()` API:

```rust
use rocketmq_runtime::{BudgetDimension, BudgetRejection, BudgetRejectionReason};

fn rejection_label(rejection: &BudgetRejection) -> &'static str {
    match rejection.reason() {
        BudgetRejectionReason::Closed => "closed",
        BudgetRejectionReason::Capacity(BudgetDimension::Count) => "count",
        BudgetRejectionReason::Capacity(BudgetDimension::Bytes) => "bytes",
        BudgetRejectionReason::Capacity(BudgetDimension::Rate) => "rate",
    }
}
```

Do not map `None` to a capacity dimension, panic, or retry until capacity changes.
`BudgetRejection::is_closed()` is available for early closure handling.
`DynamicKeyRegistrationFailure` also gains `Closed`; exhaustive matches must
handle it. A static `child()` of a closed generation may still be constructed,
but inherits the permanently closed admission gates.

Closing or retiring a dynamic key now closes every escaped budget clone and
descendant. Acquisition and closure are serialized: work admitted first remains
charged until its actual permit release, and work arriving after closure is
rejected. Retirement releases the name only after all reservations drain. Old
handles cannot admit work beside a replacement generation.

Rebinding checks the entire target chain, including shared ancestors and the
same-node fast path. A rejected rebind preserves the source permit. Moving an
existing permit out to an open budget remains supported. Queues return their
existing `QueuePushRejection::Closed`, wake waiting producers and receivers, and
allow accepted entries to drain. Closed admission does not invoke a destructive
full or age policy. `try_push_budgeted` still releases a permit on a normal
rejection; only `ForeignPermit` returns the item and unchanged charge together.

The workspace consumers migrate in the same change. Transport pending requests
report `SessionClosed`; `DeferredAdmissionAcquireOutcome` gains `Closed` rather
than reporting parent capacity exhaustion. Telemetry reports a closed buffer.
Broker capacity rejection messages retain their existing dimension text and
can accurately describe closure.

## Shutdown and failure settlement

Active descendants retain their ancestor groups after intermediate context
handles are dropped. Shutdown seals and retains the accepted subtree before
cancelling it, so fast leaf completion cannot disappear from the report. Empty,
unowned subtrees still unregister; there is no permanent strong child registry.

Critical monitors stop cooperatively with their owner. Cancellation selected
before failure handling leaves the pending record available to another handler.
Handlers process the record they actually take, even if a previous notification
referred to an older failure. Callbacks remain synchronous and should be short.

Critical task settlement includes destructor panics before publishing task
completion. Ordinary cancellation is not a critical failure. Panic propagation
and poisoned task-group behavior remain unchanged. These guarantees apply to
unwinding panics, not `panic=abort` or process termination.

Scheduled run reservations now settle exactly once on every exit path.
`ScheduledTaskSnapshot::runs` continues to count normal returns, including a
controlled stop. `failures` includes timeouts and rejected submissions, and now
also records panics and cancelled/aborted reservations that previously leaked
their active count. A reservation dropped before its first poll is a failed
start. The internal outcomes distinguish completion, timeout, panic,
cancellation, and rejection before start; the published snapshot shape stays
unchanged.

Lifecycle shutdown requests preserve `Failed` and `Stopped` through conditional
atomic transitions. Repeated requests keep the first reason and absolute
deadline.
