# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

ARG BUILDER_IMAGE=rust:1.95.0-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${BUILDER_IMAGE} AS builder

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

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/workspace/rocketmq-dashboard/rocketmq-dashboard-web/backend/target,sharing=locked \
    cargo +1.95.0 build \
    --manifest-path rocketmq-dashboard/rocketmq-dashboard-web/backend/Cargo.toml \
    --locked \
    --release \
    --bin rocketmq-dashboard-web-backend \
    --bin rocketmq-dashboard-storage \
    && install -D -m 0555 \
    rocketmq-dashboard/rocketmq-dashboard-web/backend/target/release/rocketmq-dashboard-web-backend \
    /opt/rocketmq-dashboard/rocketmq-dashboard-web-backend \
    && install -D -m 0555 \
    rocketmq-dashboard/rocketmq-dashboard-web/backend/target/release/rocketmq-dashboard-storage \
    /opt/rocketmq-dashboard/rocketmq-dashboard-storage

FROM ${RUNTIME_IMAGE}

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 rocketmq-dashboard \
    && useradd --system --uid 10001 --gid 10001 --create-home rocketmq-dashboard \
    && install -d -o 10001 -g 10001 -m 0700 /var/lib/rocketmq-dashboard

COPY --from=builder /opt/rocketmq-dashboard/rocketmq-dashboard-web-backend /usr/local/bin/
COPY --from=builder /opt/rocketmq-dashboard/rocketmq-dashboard-storage /usr/local/bin/

USER 10001:10001
WORKDIR /var/lib/rocketmq-dashboard

ENV DASHBOARD_WEB_HOST=0.0.0.0 \
    DASHBOARD_WEB_PORT=8082 \
    DASHBOARD_WEB_STORAGE_BACKEND=sqlite \
    DASHBOARD_WEB_STORAGE_PATH=/var/lib/rocketmq-dashboard/dashboard.db \
    RUST_LOG=info

EXPOSE 8082
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD wget --quiet --output-document=- http://127.0.0.1:8082/api/health/live > /dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/rocketmq-dashboard-web-backend"]
