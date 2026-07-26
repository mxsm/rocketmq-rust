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
    --package rocketmq-sre-control-plane \
    --package rocketmq-sre-connector \
    --package rocketmq-sre-probe \
    --package rocketmq-sre-eval \
    --bins \
    && install -d /opt/rocketmq-sre-binaries \
    && install -m 0555 rocketmq-sre/target/release/rocketmq-sre-control-plane /opt/rocketmq-sre-binaries/ \
    && install -m 0555 rocketmq-sre/target/release/rocketmq-sre-connector /opt/rocketmq-sre-binaries/ \
    && install -m 0555 rocketmq-sre/target/release/rocketmq-sre-probe /opt/rocketmq-sre-binaries/ \
    && install -m 0555 rocketmq-sre/target/release/phase00-dev-issuer /opt/rocketmq-sre-binaries/

FROM ${RUNTIME_IMAGE} AS runtime-base

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --create-home rocketmq-sre
USER 10001:10001
WORKDIR /opt/rocketmq-sre

ENV RUST_LOG=info

FROM runtime-base AS control-plane
COPY --from=builder /opt/rocketmq-sre-binaries/rocketmq-sre-control-plane /usr/local/bin/rocketmq-sre-control-plane
COPY rocketmq-sre/migrations /opt/rocketmq-sre/migrations
COPY rocketmq-sre/config /opt/rocketmq-sre/config
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/rocketmq-sre-control-plane"]

FROM runtime-base AS connector
COPY --from=builder /opt/rocketmq-sre-binaries/rocketmq-sre-connector /usr/local/bin/rocketmq-sre-connector
COPY rocketmq-sre/config /opt/rocketmq-sre/config
EXPOSE 8091
ENTRYPOINT ["/usr/local/bin/rocketmq-sre-connector"]

FROM runtime-base AS probe
COPY --from=builder /opt/rocketmq-sre-binaries/rocketmq-sre-probe /usr/local/bin/rocketmq-sre-probe
ENTRYPOINT ["/usr/local/bin/rocketmq-sre-probe"]

FROM runtime-base AS dev-issuer
COPY --from=builder /opt/rocketmq-sre-binaries/phase00-dev-issuer /usr/local/bin/phase00-dev-issuer
EXPOSE 8092
ENTRYPOINT ["/usr/local/bin/phase00-dev-issuer"]
