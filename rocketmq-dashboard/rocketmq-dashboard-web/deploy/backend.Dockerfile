# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

ARG BUILDER_IMAGE=rust:1.95.0-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${BUILDER_IMAGE} AS builder

WORKDIR /workspace
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/workspace/rocketmq-dashboard/rocketmq-dashboard-web/backend/target,sharing=locked \
    cargo +1.95.0 build \
    --manifest-path rocketmq-dashboard/rocketmq-dashboard-web/backend/Cargo.toml \
    --locked \
    --release \
    && install -D -m 0555 \
    rocketmq-dashboard/rocketmq-dashboard-web/backend/target/release/rocketmq-dashboard-web-backend \
    /opt/rocketmq-dashboard/rocketmq-dashboard-web-backend

FROM ${RUNTIME_IMAGE}

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 rocketmq-dashboard \
    && useradd --system --uid 10001 --gid 10001 --create-home rocketmq-dashboard \
    && install -d -o 10001 -g 10001 -m 0700 /var/lib/rocketmq-dashboard

COPY --from=builder /opt/rocketmq-dashboard/rocketmq-dashboard-web-backend /usr/local/bin/

USER 10001:10001
WORKDIR /var/lib/rocketmq-dashboard

ENV DASHBOARD_WEB_HOST=0.0.0.0 \
    DASHBOARD_WEB_PORT=8082 \
    DASHBOARD_WEB_STORAGE_BACKEND=sqlite \
    DASHBOARD_WEB_STORAGE_PATH=/var/lib/rocketmq-dashboard/dashboard.db \
    DASHBOARD_WEB_MONITOR_STORE_PATH=/var/lib/rocketmq-dashboard/consumer-monitor-config.json \
    RUST_LOG=info

EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/rocketmq-dashboard-web-backend"]
