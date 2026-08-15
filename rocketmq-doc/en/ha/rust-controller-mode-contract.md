# Rust Controller Mode Write-Authority Contract

RocketMQ Rust Controller mode uses a Rust-only lease protocol between the OpenRaft Controller,
Broker control plane, and Store. It provides the fencing and failover behavior required by the
RocketMQ Controller feature without depending on Java Controller, JRaft, DLedger, or Java
AutoSwitchHA wire formats.

## Authority and lease

- A write authority is the pair `(brokerId, masterEpoch)` committed in the Controller state machine.
- A lease token adds a monotonically increasing `generation` for that broker set.
- A Controller heartbeat can grant a lease only when its cluster, broker set, registered broker ID,
  and reported epoch exactly match the committed current master.
- Slave, learner, unknown, removed, stale-epoch, and future-epoch requests receive no lease and do
  not advance the generation.
- The default Controller lease duration is 10 seconds. The Broker reserves a 2-second safety margin,
  which is greater than the one-second maximum append/ACK budget.

The heartbeat is replicated through OpenRaft before the response is returned. An ordinary one-way
heartbeat is not write authority.

## Clock and handover safety

The Broker records a monotonic timestamp before sending the heartbeat. Its local deadline is no
later than `send_time + lease_duration - safety_margin`; response delay therefore consumes the
lease instead of extending it. Controller and Broker wall clocks are never compared.

When the committed master authority changes, the Controller withholds the first lease for the new
authority until `lease_duration + safety_margin` has elapsed since the last old-authority grant.
This quarantine prevents the old and new masters from holding overlapping valid leases.

## Store fencing

Role changes fence Store before the Broker publishes the new role. Store remains fenced until a
valid lease for that exact authority is installed. CommitLog captures the token before both normal
and batch append, then validates the same token again before returning the final result. This check
also applies to local-ACK, asynchronous flush, `waitStoreMsgOK=false`, duplication, single-replica,
and internal Timer write paths.

An expired, explicitly fenced, mismatched, or superseded token returns `SERVICE_NOT_AVAILABLE` and
must never produce a successful client ACK. A delayed response with an older generation cannot
revive a fenced Broker. Broker readiness uses the same live lease state.

## Failure behavior

- Loss of all Controller responses causes the Broker to fence local writes.
- A direct client still connected to the isolated old master receives a retryable unavailable
  response once the lease is absent or expired.
- Rejoining with a stale epoch cannot acquire authority. Existing HA epoch recovery remains
  responsible for truncating divergent tail data before the Broker can receive a current lease.
- Controller snapshots persist lease generation and handover timing so restart cannot reset fencing
  state or reissue an older generation.
