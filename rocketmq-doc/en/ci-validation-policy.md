# CI validation policy and audit

Reviewed on 2026-09-08 across all 29 workflows. This policy reduces routine PR work while retaining
compilation, focused behavior coverage, feature checks when relevant, and release/security boundaries.

## Routine PR checks

The root workflow starts on every PR to main so required statuses are always reported.
`scripts/ci_scope.py` reads the actual root workspace members and the changed paths. It does not
invoke Cargo metadata, compile code, require a clean checkout, or compare source fingerprints.

- Ordinary root Rust changes run format/Clippy and Linux workspace tests with default features.
- Documentation outside workspace crates and standalone-only changes skip root Rust builds.
  Markdown inside crates can be embedded or consumed as data and keeps its owning build coverage.
- AGENTS, project routing, and CI selector changes run lightweight routing/selector tests.
- Approval automation changes run focused decision tests.
- Protocol/header, error, observability, and RocksDB changes select the corresponding specialist checks.
- Root dependency/toolchain changes and member feature definitions select all-feature Clippy/tests
  as well as the specialist feature matrices, covering optional dependencies before merge.
- Full feature builds and Windows/macOS builds run after relevant changes reach main and during
  scheduled/manual integration. Coverage and full architecture audits run scheduled/manual.
- Workflow concurrency cancels superseded validation runs for the same PR/ref and event type.
  Main pushes do not cancel scheduled or manually requested integration work.

A failed Git diff or selector fails the required check. Renames include both old and new paths;
declared workspace patterns still match a package whose manifest was deleted.
The selector uses Git revisions only to identify the diff, not as a source-identity acceptance gate.

## Required GitHub statuses

The main branch protection inspected during this review requires these five contexts:

- `Check (fmt + clippy)`
- `Build & Test (ubuntu-latest)`
- `Build & Test (windows-latest)`
- `Build & Test (macos-latest)`
- `Code Coverage`

Keep these names stable. Windows/macOS and coverage are fixed-name jobs with job-level conditions;
on routine PRs they report skipped rather than disappearing. The root workflow has no path-level
filter that could leave a required check pending. The example workflow now has a distinct check name.

GitHub documents that a skipped job satisfies required-check status, while a workflow skipped by
path filtering can leave required checks pending:
[job conditions](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-jobs-with-conditions),
[workflow path filters](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onpushpull_requestpull_request_targetpathspaths-ignore).

No branch protection or ruleset settings are changed by this patch. The workflow changes take effect
when published and merged; local validation cannot measure the resulting hosted runner time.

## Expected task reduction

These counts are derived from workflow matrices and selector scenarios, not measured duration.
They count executed root jobs and exclude skipped contexts and independently triggered project workflows.

| Change | Previous root workflow | Updated root workflow |
| --- | ---: | ---: |
| Internal client Rust change | 22 jobs | 2 jobs |
| Protocol header change | 22 jobs | 3 jobs |
| Observability crate change | 22 jobs | 9 jobs; feature Clippy is package-scoped |
| Root README or AGENTS change | 22 jobs | 1 lightweight job |
| Website documentation | Root workflow could be absent | 1 lightweight root job; one website PR build |

The seven observability combinations remain, but redundant workspace `cargo check` is removed and
Clippy runs on `rocketmq-observability` instead of compiling the entire workspace per combination.
Full-workspace all-feature integration still runs on main and scheduled/manual runs.

## Workflow inventory and decisions

| Workflow file | Decision |
| --- | --- |
| `rocketmq-rust-ci.yaml` | Select work by actual changed paths; retain required context names; default-feature Linux PR checks; move full architecture contracts, debt reports, coverage, and redundant interface suites to scheduled/manual integration. |
| `rocketmq-example-ci.yaml` | Keep Linux format/Clippy/tests; move three-platform example builds to main/manual; use a unique check name. |
| `rocketmq-mcp-ci.yaml` | Keep read-only boundary and default/HTTP tests; remove duplicate same-toolchain check/MSRV job; Rustdoc after merge; push only on main to avoid branch push plus PR duplication. |
| `rocketmq-sre-ci.yml` | Keep execution boundaries, tests, PostgreSQL recovery, UI, and Clippy; remove duplicate metadata/check/MSRV work; run only ignored PostgreSQL tests in the second pass; Rustdoc after merge. |
| `dashboard-gpui-ci.yml` | Remove `cargo check` already covered by Clippy/tests. |
| `dashboard-web-ci.yml` | Remove separate build already covered by tests/Clippy; four-engine storage Compose integration runs on main/manual. |
| `dashboard-tauri-ci.yml` | Keep frontend and Rust tests; move four-platform application packaging to main/manual. |
| `fuzz-ci.yml` | Keep harness compilation on PRs; four runtime fuzz jobs execute on nightly/weekly/manual runs. |
| `security-audit.yml` | Keep eight lockfile audits and daily advisory checks; trigger on manifests/lockfiles rather than ordinary broker/protocol/storage source edits. |
| `website-check.yml` | Own the single website PR build, including deployment workflow changes. |
| `deploy.yml` | Build/deploy only on main push or manual dispatch; preserve serialized deployment. |
| `container-foundation-ci.yml` | Keep static contract/tests on PRs; foundation/five-image builds and supply-chain evidence after merge/manual; isolate concurrency per PR/ref. |
| `core-kubernetes-assets-ci.yml` | Keep path-scoped core candidate boundary checks; cancel superseded runs. |
| `kubernetes-assets-ci.yml` | Keep path-scoped Helm/Kustomize contract validation; cancel superseded runs. |
| `architecture-documentation.yml` | Keep weekly/manual documentation checks; remove the cross-registry governance gate. |
| `architecture-nightly-evidence.yml` | Retain scheduled/manual Loom, property, Miri, and standalone coverage; remove registry-governance and registry-SHA jobs. |
| Retired M10 performance workflow | Removed frozen hardware/command inventories, fingerprints, and threshold gates; use maintained Cargo benchmarks when needed. |
| `architecture-slo-evidence.yml` | Retain path-scoped static contracts; six-hour dynamic work was already scheduled/manual only. |
| `kubernetes-fault-matrix.yml` | Retain path-scoped static checks; dynamic fault runs were already scheduled/manual only. |
| `release-candidate.yml` | Retain explicit candidate preparation/build/qualification; not a routine PR gate. |
| `core-service-image-publish.yml` | Retain manual local-candidate checks and remote-publication boundary. |
| `service-image-publish.yml` | Retain release/manual publication, signatures, scanning, and immutable artifact checks. |
| `v1-functional-acceptance.yml` | Retain manual candidate handoff/qualification. |
| `auto_approve_pull_requests.yml` | Remove nine-minute initial sleep and fifteen-minute polling; recheck on root CI completion or PR readiness events; honor required check names/apps and skipped contexts; approve only the checked commit. |
| `auto_merge.yml` | Ignore unrelated label churn and cancel superseded attempts; keep merge label, approval count, and merge policy. |
| `auto_request_review.yml` | Keep reviewer routing and cancel superseded runs. |
| `auto-comment-pr.yml` | Remove unnecessary checkout from metadata-only automation. |
| `remove-label-on-approve.yml` | Remove unnecessary checkout from metadata-only automation. |
| `sync-issue-labels.yml` | Remove unnecessary checkout from metadata-only automation. |

Standalone source workflows exclude only `AGENTS.md`; the root routing job handles those edits.
MCP prompt Markdown, its README compatibility fixture, and the SRE operations runbook remain covered.
Product authorization, audit, wire/storage compatibility, and cryptographic release-artifact integrity
are unchanged. Download checksums and release signatures are distinct from routine development gates.

Automatic approval uses `workflow_run` because GitHub Actions does not trigger `check_suite` workflows
for its own suites. A missing PR list is resolved through the commit association API. Old completion
events cannot approve a newer PR head, and the review is attached to the checked commit. This workflow
uses trusted metadata-only scripts and never checks out PR code or downloads its artifacts.
See GitHub's [event documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows).
The completion trigger becomes active after this workflow is present on the default branch.

## Verification and limitations

- Parse all workflow YAML and run actionlint on changed workflows.
- `python -m unittest discover -s scripts/tests -p test_ci_scope.py -v`
- `node --test scripts/tests/test_auto_approve.cjs`
- Check existing workflow contract tests and the Python test inventory; keep its generated index aligned.
- `scripts/check-agents-routing.ps1` or `bash scripts/check-agents-routing.sh`
- `git diff --check`

The Python and Node tests use standard libraries. PyYAML and actionlint are local review tools, not
new project dependencies or required PR jobs. No Cargo/Node application build is required to validate
these workflow/selector changes locally; hosted builds are not claimed to have passed.
The existing documentation inventory and generated index reflect the removed duplicate commands;
the documentation guard no longer requires Codecov upload failures to fail integration.

Review results: 21 selector tests (including real Git deletion/rename/event scenarios), 13 approval
tests, 6 routing tests, and 9 documentation tests passed. All 29 workflow YAML files passed duplicate-key
and job-dependency checks; all 22 changed workflows passed actionlint. The actual root shell branches
were exercised with a Cargo stub for default/all-feature selection, without compiling applications.
The documentation guard and `git diff --check` passed.

The pre-change full-repository actionlint baseline already reports the unlisted custom runner label
`rocketmq-release-candidate` in `release-candidate.yml` and `v1-functional-acceptance.yml`.
These unchanged workflows are outside this patch's successful changed-workflow lint check.

The existing container contract test reports `mcp_stdio entrypoint must use the shared lifecycle
SIGINT/SIGTERM waiter`. Substituting the HEAD workflow into the same audit returns the identical
finding; these workflow edits introduce no additional container contract findings.

MCP/SRE currently use Rust 1.95 for both normal checks and their MSRV. Restore a distinct MSRV job
only if the development toolchain and supported minimum diverge. Cross-platform regressions and full
coverage now surface during integration instead of blocking every PR. Platform-sensitive root changes
can run the root workflow manually before merge; Tauri packaging, Web storage integration, and example
platform builds have their own manual workflow entry points.

## Maintained script checks (2026-09-08)

The remaining root architecture checks cover actual package boundaries, production
unsafe/runtime contracts, error handling, telemetry semantics, public API intent,
and documentation routes. Historical ArcMut, API snapshot, M10 performance,
release-plan, dependency-count, lint/trait, and resolved-milestone gates are retired.

Dependency checks allow ordinary package/dependency changes without updating a
snapshot. Rust hygiene fails on safety findings; panic and pin observations are
advisory. Dedicated release, security, container, and live qualification tools
remain available for their actual tasks.
