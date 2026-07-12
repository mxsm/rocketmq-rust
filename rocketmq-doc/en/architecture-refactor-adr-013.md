# ADR-013: Correct ArcMut Removal Milestones Without Weakening the Guard

## Status

Accepted for the Phase 1 closeout candidate.

## Context

The M01 inventory assigned every Remoting, Runtime Foundation, and Store ArcMut finding a removal milestone of
M02. That assignment conflicts with the approved migration plan:

- M02 requires only the first safe shared-mutation slices to be replaced and explicitly preserves the existing
  Remoting and Store public facades.
- M05 and M06 extract the canonical Transport and Store boundaries while compatibility facades remain.
- The Phase 3 M11 gate is the first gate that requires unsafe ArcMut escapes to be absent from production and
  public compatibility APIs.

Treating all of this debt as expired at M03 would force a breaking facade deletion or make the guard report a
known false architecture failure. Keeping the guard at M01 or M02 would instead misstate project progress.

The Phase 1 implementation also showed that an occurrence fingerprint can change when adjacent owned-state code
changes, even when the governed item and semantic use do not move. The original baseline promotion checked only
the total occurrence count and could therefore accept a replacement occurrence when larger unrelated reductions
hid it.

## Decision

1. Phase 1 keeps the strict gate required by M02: ArcMut debt must decrease and no identity or occurrence may be
   added silently. The closeout inventory decreases from the frozen M01 baseline of 1,277 identities and 3,452
   occurrences to 1,266 identities and 3,430 occurrences.
2. The remaining Remoting, Runtime Foundation, and Store entries receive `remove_by: M11`. This is an ultimate
   compatibility deadline, not permission to postpone owner work: M05 and M06 must still remove debt from their
   canonical implementations, and later baselines must remain monotonic.
3. Baseline promotion now rejects any new occurrence ID, even when the total count decreases. A one-to-one
   occurrence relocation is accepted only through a versioned approval that names the old and new IDs, gives a
   concrete reason, and references this ADR. The governed item must be unchanged. A relocated ID requires the old
   occurrence to disappear; a retained ID may approve only a reviewed fingerprint change. Every approval must be
   consumed.
4. Twenty-seven Phase 1 fingerprint relocations or retained-ID context changes are recorded in
   the Phase 1 promotion history. Additional governed occurrences found during review were removed or folded into
   their existing governed test items instead of being approved.
5. The initial M05 review-fix promotion consumed sixteen one-to-one fingerprint relocations while the client and
   server connection loops moved to canonical Transport session callbacks. The final review replaces that
   consumable approval file with two strict one-to-one relocations. Creating a request-local remoting session
   snapshot moves the existing context constructor within the same `fn run` item; adding the private per-instance
   interceptor wrapper changes the fingerprint of the existing `ConnectionHandlerContext` field within the same
   `ConnectionHandler` struct. Neither change adds an identity or occurrence. Both approvals name the exact
   old/new IDs and are consumed during monotonic promotion.

## Consequences

- The ledger reflects the architecture plan without weakening the no-growth invariant.
- Phase 1 can close at M03 while preserving R0 public compatibility.
- M05, M06, and M11 retain measurable ArcMut burn-down obligations; M11 fails if any corrected entry remains.
- Future fingerprint churn requires explicit review rather than automatic baseline regeneration.
- The reviewed M05 baseline contains 1,233 identities and 3,378 occurrences, a reduction of 33 identities and 52
  occurrences from the Phase 1 closeout baseline. The canonical `rocketmq-transport` boundary contains no ArcMut
  occurrence.

## Rejected alternatives

- Delete the public Remoting and Store facades in M02 and break the documented compatibility contract.
- Leave `current_milestone` at M01 or M02 after M03 completes.
- Accept any replacement occurrence whenever the repository-wide total decreases.
- Add a permanent exception or an unbounded removal deadline.
