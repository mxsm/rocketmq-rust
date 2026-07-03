# Message CheetahString API Migration Evaluation

Date: 2026-07-03

## Decision

Do not migrate the public `rocketmq-common` message API from `CheetahString` to
`CheetahStr` at this time.

The completed migration work shows that the highest-confidence gains come from
internal hot-path cleanup while preserving the existing public contracts:

- Keep `MessageTrait`, `Message`, `MessageExt`, and public property maps on
  `CheetahString`.
- Use borrowed lookup and borrowed property reads for read-only paths.
- Use `CheetahStr` only behind private fields where immutability is already part
  of the model.
- Require targeted benchmark evidence before each additional type change.

## Evidence

The following stages have already landed on `main` with issue and PR tracking:

| Area | Evidence | Result |
| --- | --- | --- |
| Benchmark baseline | `message_cheetah_string` Criterion benchmark | Established properties encode/decode, broker lookup, message decode, metadata clone, and builder join baselines. |
| Broker property lookup | `broker_message_property_lookup` targeted comparison | Existing-key lookup improved from 38.950 ns to 12.188 ns; missing-key lookup improved from 35.709 ns to 11.801 ns. |
| Property codec consolidation | same-machine properties encode/decode comparison | Encode and decode paths improved in the measured same-machine A/B runs; public wire format stayed unchanged. |
| Private immutable metadata | `storage_metadata_cheetah_str` benchmark | `StorageMetadata::clone` improved from 33.210 ns to 11.666 ns, a 65.001% reduction. |
| Borrowed property reads | `message_property_read` benchmark | `ref_split_keys` 58.165 ns vs `owned_split_keys` 106.14 ns; `ref_to_string` 57.297 ns vs `owned_to_string` 71.431 ns; `ref_exists` 16.191 ns vs `owned_exists` 47.675 ns. |

These results satisfy the original migration goal without changing downstream
public types.

## Why No Major API Migration Now

Changing public fields and trait signatures to `CheetahStr` would be a breaking
change across broker, client, store, proxy, dashboard backends, and external
users. The current evidence does not show that those public contracts are the
dominant cost after the internal hot paths were improved.

The highest-risk candidates are:

- `MessageTrait::topic() -> &CheetahString`
- `MessageTrait::property() -> Option<CheetahString>`
- `MessageExt::broker_name: CheetahString`
- `MessageExt::msg_id: CheetahString`
- `HashMap<CheetahString, CheetahString>` property maps

These are either public API surfaces or mutable model boundaries. Migrating them
would require a separate compatibility design, deprecation plan, and downstream
update strategy.

## Reopen Criteria

Reopen a major API migration only if all of the following are true:

1. A profile from a broker/client/proxy workload shows public `CheetahString`
   clone costs as a top bottleneck after the current internal optimizations.
2. A prototype demonstrates at least a 15% median improvement on the affected
   workload without regressing properties encode/decode, message decode, or
   allocation contracts.
3. A compatibility plan exists for downstream crates and external users,
   including migration examples and deprecation timing.
4. The new API shape avoids replacing mutable fields with immutable types where
   setters or builders still require mutation semantics.

Until those criteria are met, further work should continue as compatible,
targeted internal improvements rather than a major public API change.
