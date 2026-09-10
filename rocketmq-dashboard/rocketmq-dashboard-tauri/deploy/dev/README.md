# Local RocketMQ Rust debugging cluster

This Compose project runs **RocketMQ Rust binaries**, using the repository's
`rocketmq-rust/namesrv:local`, `rocketmq-rust/broker:local`, and
`rocketmq-rust/proxy:local` images. It contains one NameServer, two independent
master Brokers, and a Proxy with remoting and gRPC enabled. It does not provide
replication or failover. Use it for desktop integration and partial-result tests.

## Start

Docker Desktop must use Linux containers. Build images from the latest local
source at the repository root using the existing Dockerfile (the initial build
compiles the Rust services and can take substantial time):

```powershell
$debugSourceRevision = git rev-parse HEAD
foreach ($debugTarget in @('broker', 'namesrv', 'proxy', 'fault-driver')) {
    docker build -f docker/Dockerfile.base --build-arg "SOURCE_REVISION=$debugSourceRevision" `
        --target $debugTarget -t "rocketmq-rust/${debugTarget}:local" .
    if ($LASTEXITCODE -ne 0) { throw "Build failed: $debugTarget" }
}
```

From this directory:

```powershell
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120
docker compose ps
```

Readiness checks use each service's `/readyz` endpoint. Logs are rotated and
service data uses separate named volumes. All published ports bind to host
loopback; this is an unauthenticated local debugging fixture. ACL and TLS require
a separately configured secured fixture before claiming authentication coverage.
The [ACL fixture](acl/README.md) provides isolated credential verification with
the same local Rust images.

| Desktop setting | Address |
| --- | --- |
| NameServer | `127.0.0.1:9876` |
| Broker A | `127.0.0.1:10911` |
| Broker B | `127.0.0.1:11911` |
| Proxy remoting | `127.0.0.1:8080` |
| Proxy gRPC | `127.0.0.1:8081` |
| Readiness (NameServer / A / B / Proxy) | `18088` / `18090` / `18092` / `18091` |

Disable VIP and TLS in the desktop connection settings for this fixture.
Services share the NameServer network namespace, so the loopback Broker routes
work both inside Docker and from the desktop. Recreate the full project with
`docker compose up -d --force-recreate --wait` when changing the NameServer's
network settings. Existing unrelated containers are not part of this project.

## Exercise real administration and messages

The Rust admin CLI image can run in the same namespace. In PowerShell:

```powershell
function Invoke-DebugAdmin {
    docker run --rm --network container:rocketmq-tauri-debug-namesrv-1 `
        -e NAMESRV_ADDR=127.0.0.1:9876 rocketmq-rust/fault-driver:local @args
    if ($LASTEXITCODE -ne 0) { throw 'Rust admin command failed' }
}
Invoke-DebugAdmin cluster clusterList
Invoke-DebugAdmin topic updateTopic -n 127.0.0.1:9876 -c TauriDebugCluster -t TauriDebugSmoke -r 4 -w 4 -y
Invoke-DebugAdmin topic topicRoute -n 127.0.0.1:8080 -t TauriDebugSmoke
Invoke-DebugAdmin message sendMessage -t TauriDebugSmoke -p rocketmq-rust-tauri-smoke
```

Expect both Brokers in the cluster and Proxy route response, and `SEND_OK` from
the send. To read the message, run `message queryMsgByOffset` with the Broker
name (`-b`), queue ID (`-i`), and queue offset (`-o`) from that send receipt,
along with `-t TauriDebugSmoke`. Sending appends to existing data; do not assume
queue zero or offset zero identifies the newly sent message.
This checks remoting administration and basic message storage. Use the bounded
client below to exercise consumer processing and Trace. gRPC clients, ACL,
scheduled messages, and failover require their own fixtures.

## Online client, DLQ, and Trace fixture

Use fresh Topic and group names for each run. The example sends one keyed message,
rejects its first delivery with zero retries, then accepts later deliveries. It
keeps a Producer and Consumer online until Ctrl-C or the specified lifetime
(1–3600 seconds), and awaits both clients and their runtime during shutdown.
It prints delivery counts and send receipts without printing message bodies.

Prepare the normal Topic and a dedicated Trace Topic using `Invoke-DebugAdmin`
above. Both must exist because this fixture disables automatic Topic creation:

```powershell
$debugTopic = 'DesktopAcceptanceFresh'
$debugGroup = 'DesktopAcceptanceFreshGroup'
Invoke-DebugAdmin topic updateTopic -n 127.0.0.1:9876 -c TauriDebugCluster -t $debugTopic -r 1 -w 1 -y
Invoke-DebugAdmin topic updateTopic -n 127.0.0.1:9876 -c TauriDebugCluster -t "${debugTopic}_trace" -r 1 -w 1 -y

# Run from the repository root; leave this process running during desktop checks.
cargo run -p rocketmq-client-rust --example dashboard-debug-client -- 127.0.0.1:9876 $debugTopic $debugGroup 600
```

In the desktop, discover the group in Consumers and open its connection details,
RunningInfo, and JStack. Discover `${debugGroup}_producer` in Producers and open
its connection details. Query the group's DLQ by Key `dashboard-debug-key`, inspect
its unique ID, export CSV, and resend to the displayed online client. Mixed valid
and invalid selections should retain individual success/failure receipts.

For Trace, use `${debugTopic}_trace` and the producer message ID in the send receipt.
Wait for the asynchronous trace flush before querying. A dedicated normal Topic
avoids changing the cluster's system Trace Topic policy.

The ignored SDK regression can also verify live Broker forwarding while the
example remains online (run from the repository root in another terminal):

```powershell
$env:DASHBOARD_DEBUG_NAMESRV = '127.0.0.1:9876'
$env:DASHBOARD_DEBUG_GROUP = $debugGroup
cargo test -p rocketmq-client-rust --lib live_consumer_diagnostics_are_forwarded_by_the_broker -- --ignored
```

After the client exits, delete only these test resources through the desktop:
the Consumer group (including retry/DLQ cleanup), the normal Topic, and its Trace
Topic. Existing physical message IDs generated with an unspecified Broker host
remain a separate Broker limitation; do not count those lookups as passing.

## Stop and inspect

```powershell
docker compose logs --tail 50
docker compose stop
docker compose start --wait
docker compose down
```

`down` keeps the named volumes. Only when intentionally discarding this fixture's
messages and configuration, run `docker compose down --volumes`.
