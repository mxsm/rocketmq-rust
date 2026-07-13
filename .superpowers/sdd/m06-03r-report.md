# M06-03r report: MappedFile page/progress threshold policy

## Outcome

Moved the fixed-page flush/commit eligibility policy to the canonical
`rocketmq-store-local::mapped_file::kernel` boundary. Local now owns
`OS_PAGE_SIZE: u64 = 1024 * 4` together with public
`MappedFileProgress::is_able_to_flush` and `MappedFileProgress::is_able_to_commit` methods.

The old `rocketmq-store::log_file::mapped_file::default_mapped_file_impl::OS_PAGE_SIZE` path is a direct exact
re-export. Store's private eligibility helpers delegate exactly to Local, while Store continues to own all
flush/commit I/O. The existing `StoreCheckpoint` import remains unchanged. No Cargo manifest, dependency, or
feature changed.

## Compatibility and preserved semantics

The policy remains deliberately fixed to a 4,096-byte page and does not query the host page size. A full mapped
segment short-circuits to eligible. For a positive page threshold, the implementation retains native `i32`
subtraction and division followed by `>=`; for zero or negative thresholds it retains the strict
`source_position > progress_position` fallback.

Flush still uses the mapped file's read position against flushed position. Commit still uses wrote position
against committed position. Tests freeze 4,095/4,096 and 8,191/8,192 boundaries, zero and negative thresholds,
equal and regressed progress, both source positions, and the full-segment short circuit. No checked or saturating
arithmetic and no dynamic page-size lookup were introduced.

## TDD and contract evidence

RED was captured before production code: the Local focused Rust fixture reported 18 missing-constant/method
compile errors, and the focused live source contract reported 11 ownership/adapter violations.

GREEN passes:

- Local mapped-file kernel focused contract: 8/8;
- Store old-path compatibility fixture: 1/1;
- existing Store `DefaultMappedFile` focused tests: 30/30;
- full Local suite: 69 unit plus 104 integration tests, for 173 passed; nine existing doctests remain ignored;
- Store library suite: 568/568;
- full M06 source and mutation contract: 97/97.

The contract freezes the single Local owner, public constant type and exact expression, full short circuit,
positive and non-positive branches, comparators, flush/read and commit/write source selection, exact Store
re-export, exact private delegates, and allowed caller surfaces. Negative mutations reject constant drift,
removed branches, comparator drift, the wrong source, checked/saturating arithmetic, dynamic OS page lookup,
Store wrappers, non-exact re-exports, and extra callers.

Independent review then found that body-only wrapper checks accepted `pub` and every restricted `pub(...)`
visibility, duplicate/cfg-gated decoys, and a raw fixed-page algorithm hidden behind a second definition. The
follow-up contract-first RED recorded 22 visibility/cfg/duplicate escapes. A split-helper RED then proved that
page-delta arithmetic and the least-page comparison could be separated, and a stronger 11-case RED covered
`4096`, `1024 * 4`, `4 * 1024`, `0x1000`, `1 << 12`, `2048 * 2`, local let/const, chained and module aliases,
renamed threshold parameters, plus a nested cfg-decoy module paired with an active `impl ... where` raw policy.

GREEN now parses top-level inherent methods and outer attributes, requires exactly one global and inherent
definition per Local/Store policy method, rejects method/impl `cfg` and `cfg_attr`, and freezes both exact
signatures and bodies. Store wrappers must have no `pub` visibility. A safe integer-expression resolver follows
fixed-page aliases and rejects any DefaultMappedFile production division whose divisor resolves to 4,096,
without rejecting unrelated `/ 2` or dynamic `/ page_size` alignment. The full production reference map still
rejects every additional Local-policy caller or function-item reference.

## Feature, platform, and architecture evidence

Local and Store each pass seven feature checks covering their default/no-default or local-file combinations,
fast, safe, fast+safe, observability, and all features. Both packages pass default and all-feature all-target
Clippy with `-D warnings`; the exact root workspace all-target/all-feature Clippy command also passes. Local
all-feature Rustdoc passes with `RUSTDOCFLAGS=-D warnings`. Store normal Rustdoc succeeds while retaining four
unrelated existing invalid-HTML warnings.

WSL/Linux uses isolated `target/wsl-m06-03r` and passes the Local focused fixture 8/8, Store old-path fixture 1/1,
and Local/Store all-feature checks. The isolated target is then cleaned (9,397 files, 6.1 GiB) and verified absent.

Architecture dependency tests pass 35/35; clean/violation fixtures and baseline mode pass. ArcMut tests pass
63/63, all 24 fixtures pass, and the final guard reports no regression. AGENTS routing reports
`standalone_cargo=4`, `node_projects=3`, and `routes=8`.

## Validation and known baseline

Focused/full tests, the feature matrix, package and root Clippy, strict Local Rustdoc, normal Store Rustdoc, M06
contract, architecture guard, ArcMut guard, routing, WSL checks, `cargo fmt --all -- --check`, and
`git diff --check` pass.

The error-hygiene gate reproduces only pre-existing findings outside this slice: source stringification in
`rocketmq-broker/src/auth/auth_admin_service.rs:326`; anyhow results in MCP `app.rs:45/112`, `main.rs:24/46`,
`transport/stdio.rs:20`, and `transport/streamable_http.rs:42/64/136`; and missing
`docs/07-error-hygiene-allowlist.md` plus `docs/error-codes.md`.

## Scope and handoff

M06-03r completes only the MappedFile fixed-page/progress eligibility policy. It does not move the `MappedFile`
trait, `DefaultMappedFile` owner/factory/builder, configuration, flush/group-commit I/O, CQ/Index, HA, Timer/POP,
runtime ownership, unsafe boundaries, observability behavior, or persisted formats.

PR-M06-03, its Exit Checklist, and M06-04 through M06-12 remain open.
