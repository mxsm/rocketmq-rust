# RocketMQ Rust correctness fuzzing

This standalone `cargo-fuzz` package exercises four production input boundaries:

- `protocol_decode`: RemotingCommand frame length, serialization type, header, and body decoding.
- `raw_broker_config`: Raw broker JSON deserialization followed by full validated-config conversion.
- `controller_snapshot`: controller snapshot size, schema, version, membership, and checksum validation.
- `store_recovery_record`: CommitLog frame recovery plus ConsumeQueue and Index record decoding.

Run targets locally with the repository-pinned nightly. Before running, set `CARGO_TARGET_DIR`,
`TEMP`, and `TMP` to local directories outside the repository with sufficient free space.

```powershell
$asanRuntime = Get-ChildItem "$env:ProgramFiles\Microsoft Visual Studio" -Recurse `
  -Filter clang_rt.asan_dynamic-x86_64.dll |
  Sort-Object FullName -Descending |
  Select-Object -First 1
$env:PATH = "$($asanRuntime.DirectoryName);$env:PATH"
cargo +nightly-2026-07-05 fuzz run protocol_decode --features protocol_decode -- -max_total_time=60
cargo +nightly-2026-07-05 fuzz run raw_broker_config --features raw_broker_config -- -max_total_time=60
cargo +nightly-2026-07-05 fuzz run controller_snapshot --features controller_snapshot -- -max_total_time=60
cargo +nightly-2026-07-05 fuzz run store_recovery_record --features store_recovery_record -- -max_total_time=60
```

On Windows, the AddressSanitizer runtime must match the LLVM major version reported by the pinned
nightly. If multiple Visual Studio toolsets are installed, select the matching runtime explicitly
instead of relying on the first result above.

Corpus files prefixed with `hex:` are decoded before reaching the target. This keeps binary protocol
and storage seeds reviewable in Git. When a target finds a failure, minimize the input, add the
minimized case to its corpus, and add a deterministic regression test at the owning crate boundary.
