# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

ARG BUILDER_IMAGE=rust:1.95.0-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${BUILDER_IMAGE} AS builder

WORKDIR /workspace
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/workspace/rocketmq-sre/target,sharing=locked \
    cargo +1.95.0 build \
    --manifest-path rocketmq-sre/Cargo.toml \
    --locked \
    --release \
    --package rocketmq-sre-eval \
    --bin phase01-shadow-eval

FROM ${RUNTIME_IMAGE}

RUN useradd --system --uid 10001 --no-create-home rocketmq-sre

COPY --from=builder \
    /workspace/rocketmq-sre/target/release/phase01-shadow-eval \
    /usr/local/bin/phase01-shadow-eval
COPY rocketmq-sre/tests/fixtures /opt/rocketmq-sre/tests/fixtures

USER 10001:10001
WORKDIR /opt/rocketmq-sre

ENV ROCKETMQ_SRE_SHADOW_PROVIDER_MODE=mock
ENTRYPOINT ["/usr/local/bin/phase01-shadow-eval"]
CMD ["--manifest", "tests/fixtures/e2e/wave-a-manifest.v1.yaml", "--fixtures-root", "tests/fixtures", "--compact"]
