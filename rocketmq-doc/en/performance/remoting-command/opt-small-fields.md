# Small extension-field canonicalization

## Change

ROCKETMQ extension-field serialization now iterates maps with zero or one entry directly. Canonical ordering is unambiguous at those sizes, so allocating a temporary vector and sorting it cannot change the wire result. Maps with two or more entries retain the existing filtered vector and unstable key sort.

The direct iterator still filters an empty key. Both the fallible command encoder and exact-capacity map serializer consume the same helper, so length errors, empty-value behavior, and canonical bytes remain aligned.

## Decision

**ACCEPT the 0/1 specialization.** It removes one temporary heap allocation from the one-field path by construction and adds no dependency or stack-resident buffer to every call. The zero-field path remains allocation-free.

**STOP before `SmallVec`.** The formal profile covers 0/1/8/16/32/128/256 fields but does not show the temporary sort allocation as a dominant cost or provide production weights around the 16/17 boundary. Adding a dependency and a stack/heap threshold without that evidence would increase code and stack complexity for an unproven gain.

## Validation and rollback

The 21 serialization tests cover empty, one-field, empty-key/value, length, malformed input, and larger maps. The checked-in ROCKETMQ wire golden remains unchanged. Revert the iterator enum and restore the vector helper if a canonical-byte regression is found; no public API or persisted format changed.
