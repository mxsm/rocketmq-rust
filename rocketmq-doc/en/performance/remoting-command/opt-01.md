# OPT-01: binary wire construction without outbound request IDs

## Problem and invariant

ROCKETMQ binary decode previously called `RemotingCommand::default()`, which generated an outbound request ID, before replacing every wire field including `opaque`. Inbound decode must preserve the wire value and must not advance the outbound correlation-ID sequence. JSON decode already assembles its wire representation directly and is unchanged.

The implementation adds a crate-private `from_binary_wire_parts` constructor. It receives every fixed wire field plus the lazy binary extension-field view, fixes the serialization type to `ROCKETMQ`, and initializes transient state without consulting defaults or the request-ID atomic. No public API or wire format changes.

## Test evidence

The test-only counter is thread-local, so unrelated parallel tests cannot perturb the observation. The focused test failed on the old decode path with `left: 1, right: 0`; it passes after routing binary decode through the wire constructor. The same test uses non-default code, language, version, opaque, flag, remark, empty raw extension fields, and serialization type.

Existing golden, deterministic round-trip, malformed-input, and protocol library tests protect body attachment, field values, lazy extension fields, and rejection behavior. Encoding is not modified, so the checked-in golden frame remains byte-for-byte identical.

## Diagnostic performance curve

The benchmark adds the requested 0/8/32/128 extension-field by 1/8/16/32-thread decode matrix. To avoid another long formal replay, the recorded candidate curve uses a diagnostic 1 second warmup, 1 second measurement, and 10 samples. Values are millions of decoded commands per second:

| Extension fields | 1 thread | 8 threads | 16 threads | 32 threads |
|---:|---:|---:|---:|---:|
| 0 | 6.50 | 20.14 | 22.86 | 25.08 |
| 8 | 4.04 | 13.05 | 15.77 | 16.61 |
| 32 | 2.14 | 7.43 | 9.02 | 10.34 |
| 128 | 0.77 | 2.86 | 3.61 | 3.98 |

The candidate single-thread ext-32/body-0 diagnostic measured 612.66 ns. The earlier formal baseline measured 509.48 ns in a different session, while an independent unchanged small-header replay in that later session drifted by +18.5%. These values therefore do not establish a performance gain or regression. A future release claim requires an interleaved same-session A/B; this PR makes no performance claim.

## Decision

Retain the change as a correctness and maintenance improvement: inbound decode no longer has an outbound side effect, all wire initialization is explicit in one constructor, two now-unused mutation helpers are removed, and the public surface stays unchanged. The diagnostic curve shows that the new path scales across the requested concurrency dimensions, but is not used as an acceptance claim.

## Scope, risk, and rollback

Changed production scope is limited to `remoting_command.rs` and `rocketmq_serializable.rs`; the benchmark and this report are supporting evidence. The main risk is omitting a decoded field or transient invariant. Whole-field assertions plus existing golden/malformed coverage address that risk. Revert the constructor and decode call together if any wire or malformed-input behavior differs.
