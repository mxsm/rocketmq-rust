# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

FROM rust:1.95.0-bookworm

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        clang \
        cmake \
        libclang-dev \
        libssl-dev \
        llvm \
        make \
        ninja-build \
        pkg-config \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .

ENV CARGO_TERM_COLOR=always \
    RUST_BACKTRACE=1

ENTRYPOINT ["cargo", "test", "--manifest-path", "rocketmq-dashboard/rocketmq-dashboard-web/backend/Cargo.toml", "--all-targets", "--all-features", "--", "--include-ignored", "--nocapture", "--test-threads=1"]
