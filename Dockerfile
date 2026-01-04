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

# Copy entire workspace structure first
COPY . .

# Prepare recipe for caching
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

# Copy the recipe file for caching dependencies
COPY --from=chef /app/recipe.json recipe.json

# Build dependencies only (cached layer)
RUN cargo chef cook --release --recipe-path recipe.json

# Copy actual source code (only changed files)
COPY . .

# Build all binaries in release mode
RUN cargo build --release \
    --bin rocketmq-broker-rust \
    --bin rocketmq-namesrv-rust \
    --bin rocketmq-cli-rust \
    --bin rocketmq-admin-cli-rust

# Extract only binaries using objcopy for minimal size
RUN for bin in broker-rust namesrv-rust cli-rust admin-cli-rust; do \
    objcopy --strip-all /app/target/release/rocketmq-$bin /tmp/rocketmq-$bin 2>/dev/null || true; \
    done

# Runtime stage - Debian unstable-slim for latest glibc support
FROM debian:unstable-slim

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
COPY --chown=1000:1000 distribution/config/ /app/config/

# Create startup script that detects which component to run
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Get component type from environment variable (default: all)\n\
COMPONENT=${ROCKETMQ_COMPONENT:-all}\n\
\n\
# Function to run in background and monitor\n\
run_in_background() {\n\
    local name="$1"\n\
    shift\n\
    echo "Starting $name..."\n\
    "$@" &\n\
    local pid=$!\n\
    echo "$name started with PID: $pid"\n\
    echo "$pid" > /tmp/${name}.pid\n\
}\n\
\n\
case "$COMPONENT" in\n\
    namesrv)\n\
        echo "Starting NameServer only..."\n\
        exec /usr/local/bin/rocketmq-namesrv-rust\n\
        ;;\n\
    broker)\n\
        # Get namesrv address (default to localhost if not specified via network)\n\
        NAMESRV_ADDR=${NAMESRV_ADDR:-127.0.0.1:9876}\n\
        echo "Starting Broker with NameServer: $NAMESRV_ADDR"\n\
        exec /usr/local/bin/rocketmq-broker-rust --namesrv-addr "$NAMESRV_ADDR"\n\
        ;;\n\
    all)\n\
        echo "Starting NameServer and Broker together..."\n\
        \n\
        # Start NameServer in background\n\
        run_in_background "namesrv" /usr/local/bin/rocketmq-namesrv-rust\n\
        \n\
        # Wait for NameServer to be ready\n\
        echo "Waiting for NameServer to be ready..."\n\
        sleep 3\n\
        \n\
        # Start Broker in foreground\n\
        NAMESRV_ADDR=${NAMESRV_ADDR:-127.0.0.1:9876}\n\
        echo "Starting Broker with NameServer: $NAMESRV_ADDR"\n\
        exec /usr/local/bin/rocketmq-broker-rust --namesrv-addr "$NAMESRV_ADDR"\n\
        ;;\n\
    cli)\n\
        echo "Running CLI..."\n\
        exec /usr/local/bin/rocketmq-cli-rust "$@"\n\
        ;;\n\
    *)\n\
        echo "Unknown component: $COMPONENT"\n\
        echo "Usage: ROCKETMQ_COMPONENT=<all|broker|namesrv|cli> docker run ..."\n\
        echo "  all     - Start both NameServer and Broker (default)"\n\
        echo "  broker  - Start only Broker"\n\
        echo "  namesrv - Start only NameServer"\n\
        echo "  cli     - Run CLI tools"\n\
        exit 1\n\
        ;;\n\
esac\n' > /usr/local/bin/rocketmq-start.sh && chmod +x /usr/local/bin/rocketmq-start.sh

# Create a non-root user (numeric ID for scratch image)
USER 1000:1000

# Expose ports
EXPOSE 9876 10911 10931

# Health check using the CLI
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["/usr/local/bin/rocketmq-cli-rust", "--version"]

# Set working directory
WORKDIR /app

# Set default environment variable (can be overridden at runtime)
ENV ROCKETMQ_COMPONENT=all

# Default entrypoint - uses startup script
ENTRYPOINT ["/usr/local/bin/rocketmq-start.sh"]

