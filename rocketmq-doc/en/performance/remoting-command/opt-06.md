# OPT-06: direct request-header codec expansion stop decision

## Evidence audit

The formal corpus contains 48 Java-compatible cases across 12 representative headers and both wire formats. It is suitable for per-schema correctness and cost comparison, but every schema has equal benchmark weight. It does not identify the production top request codes, the fraction of traffic still using map-based encoding, or the CPU share attributable to materialize, merge, and canonical sort.

Existing direct codecs already cover several send, pull, response, and notification headers. Selecting more headers from benchmark latency alone would favor complex synthetic cases rather than verified production frequency. It would also add generated `.text`, macro output, clean build work, and a wider alias/default/collision compatibility surface.

## Decision

**STOP.** Do not expand the direct-codec registry without a production request-code ranking and CPU attribution showing a non-direct header is hot. No batch can meet the required selection gate, so Java fixtures, code-size measurements, and build-time A/B are intentionally not started.

Reopen with a top-N table containing request share, CPU share, current allocations per operation, wire format, and existing capability. Limit each batch to at most three headers of the same risk level, keep DirectBinary separate, and preserve independent rollback per header.

## Rollback

No production or generated code changed. The checked-in 48-case corpus remains the compatibility control for a future evidence-backed batch.
