# Request header compatibility assets

This directory owns the reviewed compatibility contract between RocketMQ Rust request headers and a pinned Apache RocketMQ Java checkout.

The Java checkout is an offline oracle. Cargo build and normal Rust tests never read it. Generated mappings, schemas, golden fixtures, hashes, and normalized benchmark summaries are checked in so that regular validation is reproducible.

## Pinned sources

- RocketMQ Rust historical codec: `0c4722568a74987f7be51df12ec87dbfdc05fbba`
- Apache RocketMQ Java: `2daf0e2ca91a1592d18235d43e5d709d1c35d15f`

Release evidence requires a clean Java worktree at the pinned commit. `--allow-dirty` is diagnostic only and cannot produce releasable evidence.

## Contract rules

- Logical map and JSON representations retain present empty strings.
- ROCKETMQ binary encoding writes zero-length values; binary decode normalizes them to absent, matching the Java decoder.
- Required strings reject empty values before sending.
- Optional empty strings therefore round-trip as `None` through ROCKETMQ binary but remain present in logical map and JSON representations.
- Java's historical `proxyFrowardClientId` spelling is the canonical wire key. `proxyForwardClientId` is decode-only compatibility input.
- Canonical and alias values must not depend on hash-map iteration order.
- Unknown fields are ignored only after envelope size and entry-count limits have been enforced.

## Updating the Java baseline

1. Create a clean Java worktree at the candidate commit.
2. Regenerate `header-class-map.json`, `java-schema.json`, and golden fixtures into a temporary output directory.
3. Review the old-to-new normalized schema diff. Every new difference must be aligned or recorded in a reviewed override or extension allowlist.
4. Replay Java-to-Rust and Rust-to-Java golden verification.
5. Update the pinned commit, fixture hashes, corpus version, and normalized benchmark baselines in one reviewed change.

Do not edit generated schema or golden files by hand. Do not commit raw JMH, Criterion, Cargo target, or machine-specific environment output.

## Performance workflow

`perf-corpus-v1.json` defines the production-weighted set of 48 encode and decode operations. Regenerate it after the fixture manifest changes, or use `--check` in verification jobs:

```powershell
python scripts/request-header-codec/generate_perf_corpus.py --check
```

Run the unified harness from a clean Rust worktree. The pinned Java oracle must also be clean. `-Quick` verifies wiring only; its measurements are diagnostic and the comparison tool rejects them as release evidence.

```powershell
$common = @{
  JavaRepo = 'D:\Github\Java\rocketmq-header-codec-oracle'
  Corpus = 'scripts/request-header-codec/perf-corpus-v1.json'
  Gates = 'scripts/request-header-codec/perf-gates.json'
}

# Establish the compatibility-correct post-baseline before codec optimization.
.\scripts\request-header-codec\run-benchmarks.ps1 @common `
  -Mode PostP0 `
  -Output target/request-header-codec-perf/post-p0-<run-id> `
  -PublishBaseline

# Freeze the hardened V2 comparison point.
.\scripts\request-header-codec\run-benchmarks.ps1 @common `
  -Mode Phase1 `
  -Output target/request-header-codec-perf/v2-phase1-<run-id> `
  -PublishBaseline

# Gate V3 against Java and the hardened V2 in the same run.
.\scripts\request-header-codec\run-benchmarks.ps1 @common `
  -Mode Release `
  -V2Worktree D:\path\to\clean\v2-worktree `
  -V2Manifest D:\path\to\v2-phase1.json `
  -Output target/request-header-codec-perf/release-<run-id>
```

Interrupted runs can continue with `-Resume` and the same output directory. Never use `-Resume` after changing the corpus, fixtures, source commit, runner, or benchmark configuration.

Release replay temporarily overlays the bundled current Rust benchmark harness and corpus onto the clean frozen V2 checkout. Only benchmark-driver files are replaced; codec library sources and the frozen commit identity are unchanged. The original bytes are restored in a `finally` path, the checkout must be clean afterward, and the evidence manifest records the shared harness digest used by V3 and V2.

The release comparison is fail-closed. In addition to matching commits, corpus and fixture hashes, runner fingerprint, benchmark profile, and build recipe, it requires all gates in `perf-gates.json`. The primary throughput requirements are:

- V3 aggregate throughput is at least 15% above hardened V2.
- V3 aggregate throughput is at least 10% above pinned Java.
- V3 fast-header throughput is at least 5% above both baselines.
- the 95% confidence-interval lower bound for every aggregate claim is strictly above parity;
- no Tier-1 operation regresses by more than 3% against either baseline.

Allocation, artifact-size, clean-build time, peak process-tree memory, and incremental-build time budgets are independent hard gates. A release is not accepted when any gate lacks evidence or fails.
