# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

FROM gcr.io/distroless/cc-debian13:nonroot

ARG SERVICE
ARG BINARY
ARG CONFIG

LABEL org.opencontainers.image.title="RocketMQ Rust Community Distribution" \
      io.rocketmq.distribution="unofficial-community"

COPY --chmod=0555 ${BINARY} /usr/local/bin/rocketmq-service
COPY --chmod=0444 ${CONFIG} /etc/rocketmq/service.toml
COPY --chmod=0444 LICENSE-APACHE NOTICE /

USER 10001:10001
WORKDIR /var/lib/rocketmq
VOLUME ["/var/lib/rocketmq", "/var/log/rocketmq"]
ENTRYPOINT ["/usr/local/bin/rocketmq-service"]
CMD ["-c", "/etc/rocketmq/service.toml"]
