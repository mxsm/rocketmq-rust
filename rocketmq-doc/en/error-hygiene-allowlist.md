# Error and lint hygiene allowlist

Status: Accepted

This document describes the active typed-error exceptions tracked by
`ARC-ALLOW-001` and the review rules for Rust lint allowances. Error exception
paths remain owned by `scripts/error_architecture_guard.py`. The historical
lint registry and central count-synchronization gate have been retired.

## Error boundary exceptions

The error guard owns four narrowly defined inventories:

- internal-error path prefixes that still need typed variants;
- `anyhow` use at standalone process boundaries;
- protocol processors that still map Java-compatible generic response codes;
- source-stringification paths waiting for source-bearing typed errors.

Every entry is a repository-relative path with a boundary-specific reason.
Adding an entry requires an owning error boundary and must not weaken
redaction, source-chain, or response-code checks.

## Rust lint exceptions

Review lint allowances next to the affected source. Keep them at the narrowest
item and include a reason. Validate the affected package with Clippy when lint
behavior changes; do not maintain a second inventory of source identities or
require a historical allowance count to match.

The workspace `too-many-arguments-threshold` remains 12. Inline reasons do not
permit crate- or module-wide suppression and do not change that threshold.

## Removal rules

- Replace string-only error mapping with a typed variant that preserves its
  source.
- Replace library `anyhow` boundaries with the owning crate's typed error.
- Replace generic protocol response codes with typed response helpers.
- Replace broad or unreasoned lint allowances with a narrower API, request
  object, used capability, or an item-level reason.

## Verification

Run from the repository root:

```powershell
.\scripts\check-error-hygiene.ps1
python -m unittest scripts.tests.test_error_architecture_guard -v
```
