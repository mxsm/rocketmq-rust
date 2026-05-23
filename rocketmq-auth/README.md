# The Rust Implementation of Apache RocketMQ auth

## Overview

This module is mainly the implementation of the [Apache RocketMQ](https://github.com/apache/rocketmq) auth, containing all the functionalities of the Java
version rocketmq-auth.

## Benchmarks

Run focused authentication and authorization hot-path benchmarks with:

```bash
cargo bench -p rocketmq-auth --bench auth_hot_path_bench
```

The benchmark covers remoting signature calculation, white remote address matching, ACL policy matching, and stateful authentication/authorization cache hits.

For CI or local regression gating, first verify the benchmark target compiles:

```bash
cargo test -p rocketmq-auth --benches --no-run
```

When collecting a baseline, run the benchmark on an otherwise idle machine and keep the generated Criterion report under `target/criterion/auth_*`. Compare future changes against the same toolchain and hardware; auth hot-path regressions should be investigated before merging changes that touch signing, whitelist matching, ACL policy matching, or stateful cache keys.
