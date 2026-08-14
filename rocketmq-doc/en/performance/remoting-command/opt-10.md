# OPT-10: validated frame descriptor stop decision

## Safety and profile audit

Transport admission validates endpoint frame limits before protocol decoding, and protocol decode independently checks announced total, header, body, format, and remaining bytes. A private validated descriptor could share some inspection, but would become a capability authorizing byte ranges and protocol type across crate boundaries.

The formal profile reports complete envelope and round-trip costs, including large bodies, but does not attribute a significant CPU or instruction share to repeated prefix and length inspection. For large frames the measured work is dominated by retained-byte decode and subsequent processing; for small frames no single-scan flamegraph evidence exists. Removing a few checks speculatively cannot justify a new trusted boundary.

## Decision

**STOP.** Keep independent transport admission and protocol validation. Do not introduce `ValidatedFrameDescriptor` until an instruction-level profile identifies duplicate inspection as material and the ownership API is reviewed as a security boundary.

Reopen only with a private, non-forgeable constructor; read-only checked ranges; unchanged public `RemotingCommand::decode`; explicit truncated, negative, header-over-payload, malformed UTF-8, and oversized cases; and fuzz coverage proving no safety check is removed. Report instructions per operation and code size as well as latency.

## Rollback

No code changed. The existing defense-in-depth validation remains the rollback-safe baseline.
