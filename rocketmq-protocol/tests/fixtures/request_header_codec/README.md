# Request header compatibility fixtures

These files are owned by the protocol crate and consumed directly by Rust tests and
benchmarks. Cargo builds do not require Python, Maven, or a local Java checkout.

| Asset | Purpose |
| --- | --- |
| `java-schema.json` | Pinned Java field semantics used by the typed-schema tests |
| `schema-overrides.json` | Reviewed differences between Rust and Java field semantics |
| `extension-allowlist.json` | Rust-only headers and extension fields |
| `migration.json` | Archived V3 type inventory used to check typed registry coverage |
| `perf-corpus-v1.json` | Shared encode/decode cases for the registry test and benchmark |
| `manifest.json`, `golden/`, `rust-only/` | Wire and semantic compatibility cases |

For a header change, update the affected fixtures and typed registry entries together
when their expected behavior changes. Existing revision and digest metadata records
fixture provenance; it is not a clean-worktree or fingerprint gate for development.
The migration generators and Java extraction harness have been retired.

Select the relevant validation from the repository root:

```bash
cargo test -p rocketmq-protocol --test request_header_codec_v3_registry
cargo test -p rocketmq-protocol --test request_header_java_compatibility
cargo bench -p rocketmq-protocol --bench request_header_codec
```
