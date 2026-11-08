# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

FROM rust:1.95.0 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    clang cmake libclang-dev libprotobuf-dev pkg-config protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .
ENV CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0
# Build this checkout with each package's defaults. No release or optional feature matrix.
RUN --mount=type=cache,id=gpui-parity-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=gpui-parity-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=gpui-parity-cargo-target,target=/workspace/target,sharing=locked \
    cargo build --locked \
      -p rocketmq-namesrv --bin rocketmq-namesrv-rust \
      -p rocketmq-broker --bin rocketmq-broker-rust \
      -p rocketmq-proxy --bin rocketmq-proxy-rust \
      -p rocketmq-admin-cli --bin rocketmq-admin-cli \
    && mkdir /out \
    && cp target/debug/rocketmq-namesrv-rust target/debug/rocketmq-broker-rust \
      target/debug/rocketmq-proxy-rust target/debug/rocketmq-admin-cli /out/

FROM rust:1.95.0 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/ /usr/local/bin/
WORKDIR /var/lib/rocketmq
ENV RUST_LOG=info
CMD ["rocketmq-admin-cli", "--help"]
