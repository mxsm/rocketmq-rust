# Request header compatibility assets

This directory owns the reviewed compatibility contract between RocketMQ Rust request headers and a pinned Apache RocketMQ Java checkout.

The Java checkout is an offline oracle. Cargo build and normal Rust tests never read it. Generated mappings, schemas, golden fixtures, and their provenance are checked in so that regular compatibility validation is reproducible.

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

## Migration governance

`migration.json` is the complete registry for the 152 stable Rust request-header type IDs in the pinned contract. It records the current V2/V3 codec, full Rust path, Java peer, request-code mapping, flatten depth, fast-codec decision, risk, migration wave, and reviewed extension decision. Production field types remain authoritative; the registry does not duplicate `java_type` metadata.

The migration tool is read-only with respect to Rust source:

```powershell
# Rebuild the deterministic inventory after an intentional migration.
python scripts/request-header-codec/migrate.py inventory `
  --output scripts/request-header-codec/migration.json

# Print pending work grouped by migration wave and risk.
python scripts/request-header-codec/migrate.py plan

# Fail on unregistered headers, stale inventory, new V1/V2 derives,
# V3-to-V2 regressions, new legacy required fields, new standalone fast
# implementations, schema/allowlist drift, expired decisions, or stale hashes.
python scripts/request-header-codec/migrate.py check
```

`legacy-alias-window.json` owns the expiry and release window for decode-only compatibility aliases. The checked-in Java schema, mapping, migration registry, extension allowlist, alias window, overrides, and golden fixture manifest are validated as one offline contract. Normal CI never reads a local Java checkout or rewrites a production header.

## Updating the Java baseline

1. Create a clean Java worktree at the candidate commit.
2. Regenerate `header-class-map.json`, `java-schema.json`, and golden fixtures into a temporary output directory.
3. Review the old-to-new normalized schema diff. Every new difference must be aligned or recorded in a reviewed override or extension allowlist.
4. Replay Java-to-Rust and Rust-to-Java golden verification.
5. Update the pinned contract, fixture provenance, and performance corpus together when the fixture set changes.

Do not edit generated schema or golden files by hand. Do not commit raw JMH, Criterion, Cargo target, or machine-specific environment output.

## Performance investigation

The V2/V3 migration performance gate and machine-specific baselines were retired on
2026-09-08. Performance investigation is optional and scoped to the changed behavior;
it does not require a clean checkout, source fingerprint, frozen V2 replay, or fixed
percentage improvement.

`perf-corpus-v1.json` remains the shared input for the Rust and Java benchmarks.
Regenerate it after fixture changes, or check it without rewriting files:

```bash
python scripts/request-header-codec/generate_perf_corpus.py --check
cargo bench -p rocketmq-protocol --bench request_header_codec
cargo bench -p rocketmq-protocol --bench remoting_command_hot_paths
```

The Java benchmark harness and cross-language golden verification remain available
for interoperability investigations. Keep raw benchmark results under `target/`.
For comparisons, use equivalent inputs and record the relevant environment and
measurement settings in the report.
