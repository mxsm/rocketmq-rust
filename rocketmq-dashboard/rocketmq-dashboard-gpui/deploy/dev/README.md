# Local source cluster for Dashboard parity

This Docker Compose project runs one NameServer, one ordinary master Broker, and
one cluster-mode Proxy, built from the current local checkout. The same endpoints
can be used by native GPUI and Web backend processes. There is no replica or automatic failover.
The Broker uses its default local file store and asynchronous disk flushing.

The source builder uses `rust:1.95.0` and `cargo build --locked` for the four selected
service/Admin CLI binaries, with their default Cargo features. It uses the normal
development profile with debug information disabled and four build jobs. Native
prerequisites and Cargo/target caches live in Docker. It does not use a released
RocketMQ image or the production image publication pipeline.

## Build and start

Use Docker Desktop with Linux containers. Run these commands from this directory:

```powershell
docker compose config --quiet
docker compose --progress plain build namesrv
if ($LASTEXITCODE -ne 0) { throw 'Source build failed' }

# Parse each service configuration before starting the cluster.
docker compose run --rm --no-deps namesrv rocketmq-namesrv-rust -c /etc/rocketmq/namesrv.toml -p
docker compose up -d --wait --wait-timeout 90 namesrv
docker compose run --rm --no-deps broker rocketmq-broker-rust -c /etc/rocketmq/broker.toml -p
docker compose run --rm --no-deps proxy rocketmq-proxy-rust -c /etc/rocketmq/proxy.toml --printConfig

docker compose up -d --wait --wait-timeout 180
docker compose ps
```

The project name is `rocketmq-gpui-parity`. Its NameServer owns the shared Docker
network namespace, and all service data has separate named volumes. Broker routes
advertise `127.0.0.1:30911`; matching host port publication makes that address work
both for tools in the shared namespace and for native Windows Dashboard clients.
Changing the NameServer network requires recreating the complete project together.
To use different ports, update the matching service configuration and publications;
changing only a host port would leave advertised routes pointing at the old port.
If the Web backend also runs in Docker, give it this shared network namespace;
an independent container's loopback address refers to that container instead.

The configuration checks for Broker and Proxy share the NameServer namespace,
which is why the commands start the parsed NameServer first.

An [optional local governance profile](README.governance.md) reuses the existing
Web Backend with File, SQLite, MySQL, or PostgreSQL storage. It provides server
Sessions, Audit, and sanitized storage status; native GPUI remains independent
and retains its own local data.

## Endpoints

| Purpose | Host address |
| --- | --- |
| NameServer | `127.0.0.1:39876` |
| Broker | `127.0.0.1:30911` |
| Broker VIP channel | `127.0.0.1:30909` |
| Proxy Remoting | `127.0.0.1:38080` |
| Proxy gRPC | `127.0.0.1:38081` |
| NameServer / Broker / Proxy readiness | `http://127.0.0.1:38082/readyz`, `38083/readyz`, `38084/readyz` |

Use cluster `GpuiParityCluster` and Broker `gpui-broker`. In GPUI, create a dedicated
environment profile, add the NameServer address, and keep TLS and VIP disabled for
the initial check. The Proxy Remoting address supports the Dashboard's applicable
consumer queries; it is distinct from the gRPC endpoint and from a NameServer.

## Local security posture

This is an explicitly unauthenticated local container fixture. All published
ports bind to host loopback. Services bind Docker interfaces, with the existing
security bootstrap intentionally unset and NameServer's supported
`allowInsecurePublicListener = true` setting explicit in its configuration. The
entrypoints log this posture. No runtime security validation is bypassed or changed.

`development-insecure-loopback` cannot be used unchanged here: container-published
traffic reaches the container interface, and that profile rejects non-loopback
listeners. This fixture supplies no ACL/TLS credentials and establishes no ACL/TLS
coverage. Do not publish its ports on non-loopback host interfaces.

## Verify administration and a bounded message round trip

With the cluster running, use its locally built Admin CLI. Each command is bounded
to 45 seconds and runs in the shared namespace; it cannot select an unrelated
NameServer from the host environment.

```powershell
function Invoke-ParityAdmin {
    docker compose run --rm --no-deps --entrypoint timeout admin 45s rocketmq-admin-cli @args
    if ($LASTEXITCODE -ne 0) { throw 'Parity Admin command failed' }
}

Invoke-ParityAdmin cluster clusterList
$parityTopic = 'GPUI_PARITY_' + [guid]::NewGuid().ToString('N').Substring(0, 12)
$parityToken = 'gpui-parity-' + [guid]::NewGuid().ToString('N')
Invoke-ParityAdmin topic updateTopic -n 127.0.0.1:39876 -c GpuiParityCluster -t $parityTopic -r 1 -w 1 -y
Invoke-ParityAdmin topic topicRoute -n 127.0.0.1:39876 -t $parityTopic
Invoke-ParityAdmin message sendMessage -t $parityTopic -p $parityToken -k $parityToken -b gpui-broker -i 0
Invoke-ParityAdmin message consumeMessage -t $parityTopic -b gpui-broker -i 0 -o 0 -c 1
```

Expect the registered Broker and host-reachable route, `SEND_OK`, and the exact
unique token in the consumed message body. Offset zero is valid for the fresh
Topic in this example. Reuse the returned offset or another fresh Topic on retries.
Keep test resources until the corresponding Dashboard checks finish.

The fixture explicitly enables the Broker's `traceTopicEnable` setting. On fresh
state, startup creates the default `RMQ_SYS_TRACE_TOPIC`; general automatic Topic
creation remains disabled. This is local fixture configuration and does not change
the Broker's production defaults. Do not try to create that reserved system Topic
through the ordinary Topic administration form.

A retained volume can restore a Topic catalog without the default Trace Topic
even when the startup setting is enabled. Check the actual Broker Topic catalog.
If the Topic is missing, explicitly activate it through the supported Broker
configuration update: disable and then re-enable `traceTopicEnable`, and recheck
the authoritative catalog. Preserve the volumes and avoid ordinary reserved-Topic
creation; the read-only Trace workspace does not perform this activation.

To verify Trace, use a producer with SDK tracing enabled as well as its per-message
Trace switch. GPUI Topic Send configures both from **Enable producer Trace** and flushes
the SDK-owned dispatcher on producer shutdown. A producer with tracing disabled
does not emit producer Trace records, while Consumer tracing remains independent.
Disabling producer tracing therefore does not promise complete Trace absence.
The Trace workspace accepts a custom Trace Topic;
an operator must configure the producer and Broker for that Topic. The read-only
workspace reports an absent Topic, empty records or an RPC failure honestly.

Readiness is separate from message delivery. The commands above verify direct
Broker administration and storage; they do not establish gRPC send/receive/ack
coverage. A gRPC client must use the current schemas in `rocketmq-proxy-core/proto`
and the session/metadata exchange demonstrated in `rocketmq-proxy/tests/grpc_ingress.rs`.

## Persistence, logs, and lifecycle

| Volume | Container data |
| --- | --- |
| `rocketmq-gpui-parity_namesrv-data` | `/var/lib/rocketmq/namesrv` |
| `rocketmq-gpui-parity_broker-data` | `/var/lib/rocketmq/broker` |
| `rocketmq-gpui-parity_proxy-data` | `/var/lib/rocketmq/proxy` |

Docker manages these volumes in its Linux data disk. Inspect the actual mount
locations with `docker volume inspect <volume>`. Logs use Docker's rotated JSON
driver, limited to three 10 MB files per service. No test output or build artifacts
are written into the source checkout.

```powershell
docker compose logs --tail 50
docker compose stop
docker compose start --wait
docker compose down
```

`down` preserves data volumes. Do not use `--volumes`, prune Docker, or delete an
existing fixture's data as part of routine startup or rebuild. Rebuilding the local
image and running `docker compose up -d --wait` replaces the services while keeping
their data. Registration and a new message round trip should be rechecked afterward.
