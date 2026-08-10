# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

FROM node:24-alpine AS builder

WORKDIR /workspace
COPY rocketmq-dashboard/rocketmq-dashboard-web/frontend/package.json \
    rocketmq-dashboard/rocketmq-dashboard-web/frontend/package-lock.json ./
RUN npm ci
COPY rocketmq-dashboard/rocketmq-dashboard-web/frontend/ ./
RUN npm run build

FROM nginx:1.29-alpine

COPY rocketmq-dashboard/rocketmq-dashboard-web/deploy/nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /workspace/dist /usr/share/nginx/html

EXPOSE 3003
