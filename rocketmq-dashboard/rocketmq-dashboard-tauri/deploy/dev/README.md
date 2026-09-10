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
Invoke-DebugAdmin message sendMessage -t TauriDebugSmoke -p rocketmq-rust-tauri-smoke -b tauri-broker-a -i 0
Invoke-DebugAdmin message sendMessage -t TauriDebugSmoke -p rocketmq-rust-tauri-smoke-b -b tauri-broker-b -i 0
Invoke-DebugAdmin message queryMsgByOffset -t TauriDebugSmoke -b tauri-broker-a -i 0 -o 0
Invoke-DebugAdmin message queryMsgByOffset -t TauriDebugSmoke -b tauri-broker-b -i 0 -o 0
```

Expect both Brokers in the cluster and Proxy route response, `SEND_OK` from
each send, and the sent bodies in the read responses. On subsequent runs, offset
zero reads the first retained message; sending appends additional messages.
This checks remoting administration and basic message storage, not gRPC clients,
ACL, consumer processing, scheduled messages, or failover.

## Stop and inspect

```powershell
docker compose logs --tail 50
docker compose stop
docker compose start --wait
docker compose down
```

`down` keeps the named volumes. Only when intentionally discarding this fixture's
messages and configuration, run `docker compose down --volumes`.
