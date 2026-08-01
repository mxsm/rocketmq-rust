# Error and lint hygiene allowlist

Status: Accepted

This document is the review contract for the active error and Rust lint
exceptions counted by `ARC-ALLOW-001`. It does not create an exception by
itself. The executable identities remain in
`scripts/error_architecture_guard.py` and
`scripts/rust-lint-debt-registry.json`.

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

Crate- and module-scope exceptions, plus item exceptions without an inline
reason, remain centralized debt. A narrow item-scope allowance with Rust's
`reason = "..."` metadata is reviewed in source and is not duplicated in the
central registry. Removing an allowance reduces debt automatically; a new
unreasoned allowance fails the guard.

The workspace `too-many-arguments-threshold` remains 12. Inline reasons do not
permit crate- or module-wide suppression and do not change that threshold.

## Removal rules

- Replace string-only error mapping with a typed variant that preserves its
  source.
- Replace library `anyhow` boundaries with the owning crate's typed error.
- Replace generic protocol response codes with typed response helpers.
- Replace broad or unreasoned lint allowances with a narrower API, request
  object, used capability, or an item-level reason.
- Never increase the central maximum merely to absorb a new finding.

## Verification

Run from the repository root:

```powershell
.\scripts\check-error-hygiene.ps1
python scripts/rust_lint_debt_guard.py
python -m unittest scripts.tests.test_rust_lint_debt_guard -v
python scripts/architecture_debt_guard.py --check
```
