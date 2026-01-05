FROM lukemathwalker/cargo-chef:latest-rust-slim AS chef

WORKDIR /app

# Install nightly toolchain and build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        g++ \
        make \
        pkg-config \
        libssl-dev \
        ca-certificates \
        && rm -rf /var/lib/apt/lists/* && \
    rustup toolchain install nightly && \
    rustup default nightly

# Copy entire workspace structure
COPY . .

# Prepare recipe for caching
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

# Copy the recipe file for caching dependencies
COPY --from=chef /app/recipe.json recipe.json

# Build dependencies only (cached layer)
RUN cargo chef cook --release --recipe-path recipe.json

# Copy actual source code
COPY . .

# Build all binaries in release mode
RUN cargo build --release \
    --bin rocketmq-broker-rust \
    --bin rocketmq-namesrv-rust \
    --bin rocketmq-cli-rust \
    --bin rocketmq-admin-cli-rust

# Extract only binaries using objcopy for minimal size
RUN for bin in broker-rust namesrv-rust cli-rust admin-cli-rust; do \
    objcopy --strip-all /app/target/release/rocketmq-$bin /tmp/rocketmq-$bin 2>/dev/null || \
    cp /app/target/release/rocketmq-$bin /tmp/rocketmq-$bin; \
    done

# Runtime stage - Debian unstable-slim for latest glibc support
FROM debian:trixie-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy stripped binaries from builder
COPY --from=builder /tmp/rocketmq-broker-rust /usr/local/bin/rocketmq-broker-rust
COPY --from=builder /tmp/rocketmq-namesrv-rust /usr/local/bin/rocketmq-namesrv-rust
COPY --from=builder /tmp/rocketmq-cli-rust /usr/local/bin/rocketmq-cli-rust
COPY --from=builder /tmp/rocketmq-admin-cli-rust /usr/local/bin/rocketmq-admin-cli-rust

# Create directory structure and default configs
RUN mkdir -p /app/config /store/config /store/commitlog /store/consumequeue /store/index && \
    chmod -R 755 /store /app/config && \
    chown -R 1000:1000 /store /app/config

COPY --from=builder --chown=1000:1000 /app/distribution/config/ /app/config/

# Create startup script that detects which component to run
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
COMPONENT=${ROCKETMQ_COMPONENT:-all}\n\
\n\
case "$COMPONENT" in\n\
    namesrv)\n\
        exec /usr/local/bin/rocketmq-namesrv-rust\n\
        ;;\n\
    broker)\n\
        exec /usr/local/bin/rocketmq-broker-rust\n\
        ;;\n\
    all)\n\
        /usr/local/bin/rocketmq-namesrv-rust &\n\
        sleep 3\n\
        exec /usr/local/bin/rocketmq-broker-rust\n\
        ;;\n\
    cli)\n\
        exec /usr/local/bin/rocketmq-cli-rust "$@"\n\
        ;;\n\
    *)\n\
        echo "Unknown component: $COMPONENT"\n\
        echo "Usage: ROCKETMQ_COMPONENT=<all|broker|namesrv|cli> docker run ..."\n\
        exit 1\n\
        ;;\n\
esac\n' > /usr/local/bin/rocketmq-start.sh && chmod +x /usr/local/bin/rocketmq-start.sh

# Health check - verify services are actually running
RUN echo '#!/bin/bash\n\
COMPONENT=${ROCKETMQ_COMPONENT:-all}\n\
check_port() { timeout ${2:-5} bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$1" 2>/dev/null; }\n\
case "$COMPONENT" in\n\
    namesrv) check_port 9876;;\n\
    broker) check_port 10911 10;;\n\
    all) check_port 9876 && check_port 10911 10;;\n\
    cli) exit 0;;\n\
    *) exit 1;;\n\
esac' > /usr/local/bin/rocketmq-healthcheck.sh && chmod +x /usr/local/bin/rocketmq-healthcheck.sh

# Create a non-root user (numeric ID for scratch image)
USER 1000:1000

# Expose ports
EXPOSE 9876 10911 10931

HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD ["/usr/local/bin/rocketmq-healthcheck.sh"]

# Set working directory
WORKDIR /app

# Set default environment variable (can be overridden at runtime)
ENV ROCKETMQ_COMPONENT=all

# Default entrypoint - uses startup script
ENTRYPOINT ["/usr/local/bin/rocketmq-start.sh"]

