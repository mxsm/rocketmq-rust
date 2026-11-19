#!/usr/bin/env bash
#
# Copyright 2023 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Trains the profile-guided optimization (PGO) profile of the Proxy for release images.
#
# Runs a NameServer, a Broker and an instrumented cluster-mode Proxy on loopback and drives gRPC
# sends through the Proxy with the gRPC benchmark: synchronous sends, one connection with many
# sends in flight, and several and many concurrent clients. The client benchmark creates the
# topics through the Broker first. The Proxy writes its raw profile when it exits after SIGTERM;
# merge the files in <profile-dir> with llvm-profdata.
#
# usage: pgo-train-proxy.sh <instrumented-proxy> <namesrv> <broker> <client-benchmark> \
#            <grpc-benchmark> <work-dir> <profile-dir>

set -euo pipefail

if [[ $# -ne 7 ]]; then
  echo "usage: $0 <instrumented-proxy> <namesrv> <broker> <client-benchmark> <grpc-benchmark> <work-dir> <profile-dir>" >&2
  exit 2
fi

PROXY_BIN="$(realpath "$1")"
NAMESRV_BIN="$(realpath "$2")"
BROKER_BIN="$(realpath "$3")"
CLIENT_BIN="$(realpath "$4")"
GRPC_BIN="$(realpath "$5")"
WORK_DIR="$6"
PROFILE_DIR="$7"
HOME_DIR="${WORK_DIR}/home"

rm -rf "${HOME_DIR}" "${PROFILE_DIR}"
mkdir -p "${HOME_DIR}/data" "${PROFILE_DIR}"
HOME_DIR="$(realpath "${HOME_DIR}")"
PROFILE_DIR="$(realpath "${PROFILE_DIR}")"

cat > "${HOME_DIR}/namesrv.toml" <<EOF
rocketmqHome = "${HOME_DIR}"
listenPort = 9876
bindAddress = "127.0.0.1"
allowInsecurePublicListener = false
kvConfigPath = "${HOME_DIR}/data/kvConfig.json"
configStorePath = "${HOME_DIR}/data/namesrv.properties"
authenticationEnabled = false
authorizationEnabled = false
enableControllerInNamesrv = false
EOF

cat > "${HOME_DIR}/broker.toml" <<EOF
[broker]
listenPort = 10911
brokerIp1 = "127.0.0.1"
brokerIp2 = "127.0.0.1"
storePathRootDir = "${HOME_DIR}/store"
namesrvAddr = "127.0.0.1:9876"
autoCreateTopicEnable = true
enableControllerMode = false

[broker.brokerIdentity]
brokerName = "pgo-broker"
brokerClusterName = "PgoCluster"
brokerId = 0

[broker.brokerServerConfig]
bindAddress = "127.0.0.1"

[store]
storePathRootDir = "${HOME_DIR}/store"
storePathBrokerIdentity = "${HOME_DIR}/store/brokerIdentity"
brokerRole = "ASYNC_MASTER"
flushDiskType = "ASYNC_FLUSH"
haListenAddress = "127.0.0.1"
haListenPort = 10912
totalReplicas = 1
inSyncReplicas = 1
minInSyncReplicas = 1
mappedFileSizeCommitLog = 268435456
EOF

cat > "${HOME_DIR}/proxy.toml" <<EOF
mode = "cluster"

[grpc]
listenAddr = "127.0.0.1:8081"

[grpc.tls]
enabled = false

[remoting]
enabled = false

[cluster]
namesrvAddr = "127.0.0.1:9876"
brokerClusterName = "PgoCluster"
EOF

export ROCKETMQ_HOME="${HOME_DIR}"
export ROCKETMQ_SECURITY_PROFILE="development-insecure-loopback"
export ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS=30
export RUST_LOG=warn

NAMESRV_PID=""
BROKER_PID=""
PROXY_PID=""

# Prints the HTTP status of a request to a loopback health endpoint, or nothing.
http_status() {
  local port="$1" method="$2" path="$3" line=""
  exec 3<>"/dev/tcp/127.0.0.1/${port}" || return 0
  printf '%s %s HTTP/1.0\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n' "${method}" "${path}" >&3
  IFS= read -r line <&3 || true
  exec 3<&- 3>&-
  printf '%s' "${line}" | cut -d' ' -f2
}

wait_ready() {
  local name="$1" port="$2" pid="$3"
  for _ in $(seq 1 240); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "${name} exited before it became ready; log:" >&2
      tail -n 50 "${HOME_DIR}/${name}.log" >&2 || true
      return 1
    fi
    if [[ "$(http_status "${port}" GET /readyz 2>/dev/null)" == "200" ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "${name} did not become ready" >&2
  return 1
}

# Stops a process with SIGTERM and waits for it; the Proxy writes its profile on this exit.
stop_process() {
  local pid="$1"
  [[ -n "${pid}" ]] || return 0
  kill -TERM "${pid}" 2>/dev/null || return 0
  for _ in $(seq 1 120); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      wait "${pid}" 2>/dev/null || true
      return 0
    fi
    sleep 0.5
  done
  echo "process ${pid} did not stop after SIGTERM; killing it" >&2
  kill -KILL "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
  return 1
}

cleanup() {
  stop_process "${PROXY_PID}" || true
  stop_process "${BROKER_PID}" || true
  stop_process "${NAMESRV_PID}" || true
}
trap cleanup EXIT

# Creates a topic by sending one message through the Broker, which creates topics on demand.
create_topic() {
  local topic="$1" log="${HOME_DIR}/create-${1}.log"
  if ! RUST_LOG=error "${CLIENT_BIN}" --namesrv 127.0.0.1:9876 --topic "${topic}" --scenario sync \
    --message-count 1 --message-size 1024 --batch-size 32 --inflight 1 --timeout-ms 10000 > "${log}" 2>&1; then
    echo "creating topic ${topic} failed; log:" >&2
    tail -n 50 "${log}" >&2 || true
    return 1
  fi
}

grpc() {
  local topic="$1" count="$2" connections="$3" inflight="$4"
  local log="${HOME_DIR}/grpc-${topic}-${connections}-${inflight}.log"
  if ! RUST_LOG=error "${GRPC_BIN}" --endpoint http://127.0.0.1:8081 --topic "${topic}" --message-count "${count}" \
    --connections "${connections}" --inflight "${inflight}" --message-size 1024 > "${log}" 2>&1; then
    echo "training sends on ${topic} failed; log:" >&2
    tail -n 50 "${log}" >&2 || true
    return 1
  fi
}

ROCKETMQ_HEALTH_BIND_ADDR=127.0.0.1:18000 "${NAMESRV_BIN}" -c "${HOME_DIR}/namesrv.toml" \
  > "${HOME_DIR}/namesrv.log" 2>&1 &
NAMESRV_PID=$!
wait_ready namesrv 18000 "${NAMESRV_PID}"

ROCKETMQ_HEALTH_BIND_ADDR=127.0.0.1:18001 "${BROKER_BIN}" -c "${HOME_DIR}/broker.toml" \
  > "${HOME_DIR}/broker.log" 2>&1 &
BROKER_PID=$!
wait_ready broker 18001 "${BROKER_PID}"

for topic in PgoProxySync PgoProxyAsync PgoProxyMulti; do
  create_topic "${topic}"
done
sleep 3

LLVM_PROFILE_FILE="${PROFILE_DIR}/proxy-%p-%m.profraw" ROCKETMQ_HEALTH_BIND_ADDR=127.0.0.1:18002 \
  "${PROXY_BIN}" -c "${HOME_DIR}/proxy.toml" > "${HOME_DIR}/proxy.log" 2>&1 &
PROXY_PID=$!
wait_ready proxy 18002 "${PROXY_PID}"

started=${SECONDS}
grpc PgoProxySync 20000 1 1
grpc PgoProxyAsync 200000 1 256
grpc PgoProxyMulti 200000 4 64
grpc PgoProxyMulti 200000 32 4
echo "trained the Proxy in $((SECONDS - started)) s"

stop_process "${PROXY_PID}"
PROXY_PID=""
stop_process "${BROKER_PID}" || true
BROKER_PID=""
stop_process "${NAMESRV_PID}" || true
NAMESRV_PID=""

shopt -s nullglob
profiles=("${PROFILE_DIR}"/*.profraw)
if [[ ${#profiles[@]} -eq 0 ]]; then
  echo "the instrumented Proxy wrote no profile to ${PROFILE_DIR}" >&2
  exit 1
fi
echo "raw profiles: ${#profiles[@]} in ${PROFILE_DIR}"
