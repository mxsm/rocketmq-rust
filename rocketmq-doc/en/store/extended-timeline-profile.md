# Extended Timeline timer profile

Extended Timeline is a Rust-native, opt-in timer storage profile. It extends the supported delay
horizon without changing the default Java-compatible timer behavior. It does not read, write, or
migrate Java TimerRocksDB directories.

## Modes and feature gate

`timerStoreMode=java_compat` remains the default. A binary built without the
`rocketmq-store/extended_timeline` feature rejects both shadow and formal Extended configuration
before Store initialization; it never silently falls back to JavaCompat.

Shadow mode requires the feature and `timerExtendedShadowEnable=true` while keeping
`timerStoreMode=java_compat`. It materializes the independent Timeline and records due
observations, but it cannot admit, claim, or deliver messages.

Formal mode requires all of the following:

- `timerStoreMode=extended_timeline`;
- the `extended_timeline` build feature;
- `timerExtendedShadowEnable=false`;
- `timerExtendedAdmissionEnable=true`;
- a non-zero `timerExtendedActivationEpoch`;
- an admission horizon between 3 and 400 days and no greater than the configured Timeline horizon.

The Store rejects invalid combinations before it starts background work or network listeners.

## Runtime ownership

JavaCompat and Extended Timeline implement the same internal `TimerEngine` contract. Source and
due work is bounded by message count, retained bytes, and a deadline, and blocking storage work is
owned by the Store runtime. Formal due promotion additionally requires the current durable timer
role epoch. A stale or inactive owner cannot create ready work or acknowledge completion.

The durable Timeline owns materialization checkpoints, ready state, delivery receipts, recall,
snapshot identity, promotion fences, and garbage-collection pins. Shadow records remain
non-deliverable and retain JavaCompat as their persisted owner.

## Cutover and rollback

The supported cutover is:

1. run JavaCompat normally;
2. enable Extended shadow and verify reconciliation and backlog metrics;
3. quiesce timer admission and drain in-flight work;
4. restart with formal Extended configuration and a new, non-zero activation epoch;
5. create and install a compatible snapshot before promoting another Broker.

Formal activation persists `config/timer-store-owner.meta`. Restart must use the same activation
epoch. A JavaCompat restart against that Store root fails closed because an older binary cannot
safely interpret Extended-owned pending, ready, or completion state.

Rollback is therefore legal before formal activation, or after an explicit offline conversion
performed while the Store is quiesced, drained, and checkpoint-clean. Removing or editing the
owner marker by hand is unsupported. The 1.0 profile intentionally provides no Java
TimerRocksDB-directory conversion.
