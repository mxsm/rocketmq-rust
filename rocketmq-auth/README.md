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
