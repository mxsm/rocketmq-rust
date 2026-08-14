# OPT-09: encode capacity strategy stop decision

## Evidence audit

The formal allocation baseline shows that canonical JSON request-header encoding normally performs one 1,024-byte allocation. Canonical ROCKETMQ encoding normally performs one 264-byte allocation, with several larger schemas using two allocations and 792 bytes. The broader ext-field probe ends with 1,412 bytes in a 2,156-byte JSON buffer and 1,309 bytes in a 2,112-byte ROCKETMQ buffer for 32 fields.

These points confirm that one global bucket cannot minimize both allocation count and retained bytes. They do not provide production traffic weights, final-length percentiles, or long remark and non-ASCII p99 tails. The formal timing corpus is intentionally uniform across schemas and field counts, so treating it as a capacity distribution would bias the decision.

## Decision

**STOP.** Keep the existing 1,024-byte JSON direct reserve and 256-byte ROCKETMQ initial reserve. Trying 256/384/512/1,024 buckets without a production-weighted distribution would optimize synthetic frequency and risk regressions for uncommon but large headers. An exact-size preflight scan is also rejected because it adds an O(H) traversal to every encode.

Reopen only when low-cardinality telemetry or an equivalent production trace provides final length, capacity, wire format, schema, long remark, and UTF-8 percentiles. A future candidate must report latency, allocation count, allocated bytes, retained bytes, and p99 separately for JSON and ROCKETMQ.

## Rollback

No production code changed. The rollback is the decision itself: if representative evidence becomes available, replace this stop report with an interleaved bucket evaluation and retain the current constants as the control.
