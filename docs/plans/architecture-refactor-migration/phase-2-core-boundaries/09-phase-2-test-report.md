# M09-06 Phase 2 test report

## Test identity and environment

`[TEST]` tested implementation candidate `490c583e94b31dc7ae1b83c55ed811e2b90d4cce`, tree
`e959367d3b4002653e4e25e5b0c19213de8766b5`.

| Item | Value |
|---|---|
| OS | Windows NT 10.0.26200.0 AMD64 |
| Rust | `rustc 1.99.0-nightly (3659db0d3 2026-07-05)` |
| Cargo | `cargo 1.99.0-nightly (2f0e7011e 2026-07-05)` |
| Python | 3.14.6 |
| Node/npm | v24.13.0 / 11.13.0 |
| Evidence indexes | `target/architecture-refactor/M09/phase2-gate-490c583e/`, `target/architecture-refactor/M09/phase2-gate-8258/` |

## Successful matrix

| Scope | Command or matrix | Result |
|---|---|---|
| Root final profile | `cargo fmt --all -- --check`; root workspace all-target/all-feature strict Clippy; locked metadata | passed, exit 0 |
| Architecture contracts | dependency fixtures/target/baseline, release guard, ArcMut guard | passed; 35/35 ledger, 3/3 dev-only, release 32/10/29/4/2 |
| Phase 2 Python contracts | dependency 43, release 6, Proxy closure 7, M09 target 5, facade 5, client 6, gate 5 | 77/77 passed |
| Public API | `public_api_snapshot.py --check ... --from-existing` | 31/31, differences 0 |
| Compatibility | feature 24, wire/canonical-legacy 6, storage 10 | 40/40 passed |
| Runtime/error/routing | runtime enforcing audit, error hygiene, AGENTS routing, `git diff --check` | all passed; error categories 14/14 |
| MCP all features | `cargo test -p rocketmq-mcp --all-features` | 90 unit + 1 compile fixture + 2 integration passed; 1 external E2E ignored |
| MCP required profile | check, default test, HTTP strict Clippy, Rustdoc | 73 unit + 1 compile fixture + 2 integration passed; all other commands exit 0 |
| Broker focused mapping | `cargo test -p rocketmq-broker --lib map_authz_error_preserves_admin_error_category` | 1/1 passed |
| RocksDB strict profile | Store/Broker RocksDB Clippy and the four required test commands | 82 + 9 + 20 + 4 tests passed |
| Example | local fmt, strict Clippy, locked metadata | passed |
| Tauri | frontend `npm ci`/build; backend fmt/strict Clippy/locked metadata | passed |
| Web dashboard | frontend `npm ci`/build; backend fmt/strict Clippy/build/locked metadata | passed |

The committed compatibility artifacts are intentionally kept under the ignored runtime evidence index and are not Git
artifacts. Their group results are `passed`, and every row records its command, duration, and exit code.

## Skips and non-gating observations

- MCP external-cluster E2E is ignored because `ROCKETMQ_MCP_E2E_NAMESRV_ADDR`, topic, consumer group, and broker were not
  supplied. The local MCP protocol/stdio integration tests passed.
- GPUI is conditionally routed only when `rocketmq-dashboard-common` changes. M09-06 does not change it, so GPUI was not
  triggered; this is an approved scope skip, not a failed command.
- Tauri `npm ci` reports four existing audit findings (one moderate, three high). The clean install and production build
  passed; dependency remediation is outside this architecture-boundary work package.
- MSVC linker informational messages and upstream `proc-macro-error2` future-incompatibility notices are warnings outside
  the repository `-D warnings` lint surface; all required commands returned zero.

## Failed or superseded attempts

These attempts are disclosed and are **not** counted as successful evidence:

1. The initial typed-error gate failed with 11 findings; the candidate fixed all 11 and the complete guard rerun passed.
2. One combined Broker/MCP/Clippy run timed out after 904 seconds; split commands later completed successfully.
3. One Broker test attempt lost a target fingerprint during concurrent target activity; the isolated `--lib` rerun passed.
4. One combined 40-row compatibility run timed out at row 39; three independent group reruns completed 24/24, 6/6,
   and 10/10.
5. The first M09-06 Python contract asserted a narrower symbol spelling; the contract was corrected and passed 4/4.
6. An early format check found the new compile fixture unformatted; it was formatted and the standalone/root checks passed.
7. An invocation used obsolete dependency-guard flags (`--target`/`--baseline`); the valid `--mode` commands passed.

## Conclusion

`[TEST]` **Passed.** All required local, compatibility, specialized, and routed consumer gates for the frozen candidate
completed successfully, with only the documented environment/conditional skips above.
