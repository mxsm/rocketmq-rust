---
title: "Build and run local service containers"
---

This walkthrough packages locally built Linux binaries into the repository's community runtime image. It runs one NameServer and one Broker using Linux host networking and loopback-only listeners. It is a development topology, with no replica failover or public listener.

Use a Linux host or Linux VM with Rust, the repository build prerequisites, and Docker. Run clients on that same Linux host. Windows-native binaries cannot run in the Linux image, and Docker Desktop host-network behavior is not assumed here. For a Windows-native trial, use [local source deployment](../getting-started/local-source.md).

## Choose the image path

| Asset | Behavior |
| --- | --- |
| `distribution/container/core-service.Dockerfile` | Copies the supplied binary and configuration into a distroless runtime; it does not compile Rust |
| `docker/Dockerfile.base` | Multi-stage builder/runtime targets for services and separate test drivers |
| Core Helm chart image values | Image references to supply for your own available artifacts; default tags do not prove registry availability |

The commands below use the first path. Match the binary's CPU architecture and dynamic-library requirements to the selected distroless runtime. A successful host compilation alone does not prove container startup.

## Build binaries and a small context

From the repository root on Linux:

```bash
cargo build --release -p rocketmq-namesrv --bin rocketmq-namesrv-rust
cargo build --release -p rocketmq-broker --bin rocketmq-broker-rust
mkdir -p .rocketmq-container-demo/context
cp distribution/container/core-service.Dockerfile .rocketmq-container-demo/context/Dockerfile
cp LICENSE-APACHE NOTICE .rocketmq-container-demo/context/
cp target/release/rocketmq-namesrv-rust target/release/rocketmq-broker-rust .rocketmq-container-demo/context/
cp rocketmq-website/examples/first-message/namesrv.toml .rocketmq-container-demo/context/
cp rocketmq-website/examples/first-message/broker.toml .rocketmq-container-demo/context/
docker build -f .rocketmq-container-demo/context/Dockerfile \
  --build-arg SERVICE=namesrv --build-arg BINARY=rocketmq-namesrv-rust \
  --build-arg CONFIG=namesrv.toml -t rocketmq-docs/namesrv:local .rocketmq-container-demo/context
docker build -f .rocketmq-container-demo/context/Dockerfile \
  --build-arg SERVICE=broker --build-arg BINARY=rocketmq-broker-rust \
  --build-arg CONFIG=broker.toml -t rocketmq-docs/broker:local .rocketmq-container-demo/context
```

The separate context is intentional: the repository's root `.dockerignore` excludes `target/`. Both images run the supplied program as UID/GID `10001:10001`, with default configuration at `/etc/rocketmq/service.toml` and working directory `/var/lib/rocketmq`. The image contains no shell for interactive repair commands.

## Prepare persistent directories

The copied first-message configs use relative paths under `.rocketmq-doc-demo`. They resolve inside the container's working directory, not the host checkout. Give each service a separate host mount:

```bash
mkdir -p .rocketmq-container-demo/data/namesrv/.rocketmq-doc-demo/namesrv
mkdir -p .rocketmq-container-demo/data/broker/.rocketmq-doc-demo/broker
mkdir -p .rocketmq-container-demo/data/broker/.rocketmq-doc-demo/store
sudo chown -R 10001:10001 .rocketmq-container-demo/data
```

Change ownership only on this newly created tutorial directory. On rootless Docker or a host with mandatory access controls, arrange equivalent mapped-UID access and mount labeling. Do not solve a permission failure by making an existing production store world-writable.

## Start and observe

Keep host ports `9876`, `10911`, `10909`, and `10912` free. Start NameServer first:

```bash
docker run -d --name rocketmq-docs-namesrv --network host \
  --read-only --tmpfs /tmp:rw,nosuid,noexec,size=64m \
  -e ROCKETMQ_HOME=/var/lib/rocketmq \
  -e ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback \
  --mount type=bind,src="$(pwd)/.rocketmq-container-demo/data/namesrv",dst=/var/lib/rocketmq \
  rocketmq-docs/namesrv:local
docker logs rocketmq-docs-namesrv
```

After NameServer reports successful startup:

```bash
docker run -d --name rocketmq-docs-broker --network host \
  --read-only --tmpfs /tmp:rw,nosuid,noexec,size=64m \
  -e ROCKETMQ_HOME=/var/lib/rocketmq \
  -e ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback \
  --mount type=bind,src="$(pwd)/.rocketmq-container-demo/data/broker",dst=/var/lib/rocketmq \
  rocketmq-docs/broker:local
docker logs rocketmq-docs-broker
```

Both configs bind and advertise loopback. Linux host networking makes that address shared with the host clients; no `-p` mapping is used. Do not switch these commands to a bridge network unchanged: loopback would then refer to each container separately.

For a configuration override, mount your own file read-only at `/etc/rocketmq/service.toml` when creating the container. Keep mutable data in the data mount. The image's `/var/log/rocketmq` volume is a separate path if you explicitly configure a file-log sink there; these commands rely on the selected config's logging behavior and `docker logs`.

## Verify the message path

Run the Admin CLI and the [first-message walkthrough](../getting-started/quick-start.md) on this Linux host, using `127.0.0.1:9876`, cluster `DocsCluster`, Broker `docs-broker`, and Topic `DocsFirstMessage`. Provision the Topic and consumer group, send the five messages, consume them, and compare consumer progress.

`docker ps` establishes container status only. Logs establish startup observations. Route lookup and actual send/consume establish additional, separate evidence. This page's container commands have not been exercised as a Docker deployment in the Windows documentation environment.

## Stop without deleting data

```bash
docker stop --time 60 rocketmq-docs-broker
docker stop --time 60 rocketmq-docs-namesrv
docker rm rocketmq-docs-broker rocketmq-docs-namesrv
```

Inspect shutdown output before removing containers. If the timeout forces termination, record that as incomplete shutdown. The bind-mounted data remains under `.rocketmq-container-demo/data` for a later restart. Removing data is a separate deliberate operation, not part of this walkthrough.

For other hosts or shared networks, plan advertised endpoints and security using [multi-node deployment](multi-node.md) and [deployment security](security.md).

## Source assets

[Runtime Dockerfile](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/container/core-service.Dockerfile), [multi-stage Dockerfile](https://github.com/mxsm/rocketmq-rust/blob/main/docker/Dockerfile.base), [first-message configs](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/first-message).
