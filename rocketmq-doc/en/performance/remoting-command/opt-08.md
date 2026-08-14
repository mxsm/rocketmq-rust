# OPT-08: raw extension-field passthrough stop decision

## Contract audit

Decoded ROCKETMQ extension fields are retained as validated immutable bytes, but normal encoding deliberately emits canonical key order. A passthrough mode would preserve source ordering and duplicates instead. That is not a transparent optimization: it affects signatures, typed and dynamic collision resolution, mutation visibility, JSON conversion, and byte-level equality.

The current API has no typed caller intent distinguishing canonical output from preserved raw output. It also has no end-to-end proof that every add, remove, replace, typed-header materialization, protocol conversion, and ACL re-sign boundary invalidates passthrough eligibility. The formal corpus measures raw decode and canonical encode costs but does not show production traffic dominated by forwarding 8–256 untouched fields.

## Decision

**STOP.** Do not add a raw passthrough flag or implicit fast path. The canonical/preserve contract and production profile triggers are both absent, so the candidate cannot enter implementation or fuzz qualification safely.

Reopen only with an explicit crate-private typed policy defaulting to canonical output, complete dirty-state ownership, automatic rejection for typed headers, mutation, protocol conversion, limits, or signing, and production evidence that untouched large-field forwarding is material. The candidate must improve large-field cases by at least 10% and pass duplicate/order/empty/alias/collision, Java, and fuzz coverage.

## Rollback

No code changed. Canonical re-encoding remains the sole output contract and therefore the rollback-safe baseline.
