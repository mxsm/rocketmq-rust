FROM node:24-alpine AS builder

WORKDIR /workspace/rocketmq-sre/ui
COPY rocketmq-sre/ui/package.json rocketmq-sre/ui/package-lock.json ./
RUN npm ci
COPY rocketmq-sre/ui/ ./
RUN npm run build

FROM nginx:1.29-alpine
COPY rocketmq-sre/deploy/docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /workspace/rocketmq-sre/ui/dist /usr/share/nginx/html
EXPOSE 3004
