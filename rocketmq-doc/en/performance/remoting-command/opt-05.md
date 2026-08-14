# OPT-05: explicit body length during frame assembly

## Change and invariant

`EncodedFrame::from_command` previously took the body, cloned its `Bytes` handle back into the command, encoded the header so it could rediscover the body length, and finally dropped the clone. The new path takes the body once, reads `body.len()`, and passes that value to one crate-private header encoder.

The existing public encoder still derives the length from its in-memory body and delegates to the same implementation. External-body frame heads pass their validated length through the same entry point. If a command still contains an in-memory body whose length differs from the explicit value, encoding fails and restores the destination to its original bytes.

## Byte and error contract

A table-driven test covers JSON and ROCKETMQ with body lengths 0, 1, 128 B, 4 KiB, 64 KiB, 1 MiB, and 4 MiB. For every case, prefix plus header plus external body is byte-for-byte equal to the complete `EncodedFrame`. Existing checked-in golden and deterministic round-trip tests remain unchanged. The mismatch test starts with a non-empty destination and proves failure is atomic.

## Short same-session A/B

Candidate and baseline ran consecutively on the same machine with a 1 second warmup, 1 second measurement, and 10 samples. The candidate ran first and the baseline second, so this is a directional time-boxed A/B rather than the formal interleaved profile.

| Format | Body | Baseline median | Candidate median | Delta |
|---|---:|---:|---:|---:|
| JSON | 0 | 1.318 us | 1.157 us | -12.2% |
| JSON | 128 B | 1.337 us | 1.127 us | -15.7% |
| JSON | 4 KiB | 1.289 us | 1.090 us | -15.5% |
| JSON | 64 KiB | 1.282 us | 1.119 us | -12.7% |
| JSON | 1 MiB | 1.597 us | 1.257 us | -21.3% |
| JSON | 4 MiB | 1.921 us | 1.842 us | -4.1% |
| ROCKETMQ | 0 | 802.41 ns | 798.24 ns | -0.5% |
| ROCKETMQ | 128 B | 856.93 ns | 803.36 ns | -6.3% |
| ROCKETMQ | 4 KiB | 866.26 ns | 778.81 ns | -10.1% |
| ROCKETMQ | 64 KiB | 891.26 ns | 821.17 ns | -7.9% |
| ROCKETMQ | 1 MiB | 1.313 us | 1.019 us | -22.4% |
| ROCKETMQ | 4 MiB | 1.709 us | 1.389 us | -18.8% |

Every measured point improved and no small-body point exceeded the 2% regression budget. The exact percentages are diagnostic, but the consistent direction plus removal of a provable refcount round trip supports retaining the change.

## Scope, risk, and rollback

Only protocol-internal header assembly and `EncodedFrame` ownership change; transport callers keep the same APIs. The main risks are inconsistent announced lengths and a difference between contiguous and segmented output. Explicit mismatch rejection, byte-equality coverage, wire golden tests, and transport codec/segmented tests address those risks. Revert both protocol files together if any frame byte or length policy changes.
