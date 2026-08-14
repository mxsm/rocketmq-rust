# OPT-11: lazy extension fields and redacted formatting

## Change

`RemotingCommand` formatting no longer calls `ExtensionFields::as_map()`. It emits only the representation, field count, and whether a raw field set already has a materialized compatibility map. JSON and ROCKETMQ raw values therefore stay cold and extension-field values are not written to logs.

When a raw field set was explicitly read before mutation, the mutable transition now takes its cached `Arc<HeaderMap>`. A uniquely owned cache reuses the map allocation with `Arc::try_unwrap`; a shared cache clones once to preserve isolation. A cold field set still materializes directly from its validated raw representation.

## Contract and evidence

The focused regression test decodes both raw formats, formats each command, and proves:

- the output reports `JsonRaw` or `RocketMqRaw`, count, and cold cache state;
- neither secret test value appears in the output;
- formatting leaves both compatibility maps unmaterialized.

The existing raw read, clone, mutation, typed-header collision, canonical encoding, and materialization tests continue to pass. They cover the isolation boundary used by the cached-map transition.

## Performance decision

**ACCEPT.** Display-only changes from one complete raw-map materialization to zero. The read-then-mutate path removes the second raw scan and can reuse the existing map allocation when it is unique. Shared clones retain copy-on-write isolation and pay at most one map clone at mutation.

No time-only A/B was needed: the optimized work is directly countable, and the time-boxed qualification prioritizes the allocation/materialization invariant. The representation and wire bytes are unchanged.

## Rollback

Revert the extension-field summary and cached-map take together if formatting compatibility requires full field values. Prefer a separate explicit diagnostic API for full values rather than restoring them to the default `Display` implementation.
