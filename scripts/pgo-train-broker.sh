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

# Trains the profile-guided optimization (PGO) profile of the Broker for release images.
#
# Runs an instrumented Broker with a local NameServer on loopback and drives the send and pull
# workloads that dominate production with the client benchmark: synchronous and asynchronous
# sends, LitePull consumption and several concurrent sessions. The Broker writes its raw profile
# when it exits after SIGTERM; merge the files in <profile-dir> with llvm-profdata.
#
# Usage: pgo-train-broker.sh <instrumented-broker> <namesrv> <client-benchmark> <work-dir> <profile-dir>
# docker/Dockerfile.base runs it; it needs bash, coreutils and free loopback ports 9876, 10911,
# 10912, 18000 and 18001.

set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 <instrumented-broker> <namesrv> <client-benchmark> <work-dir> <profile-dir>" >&2
  exit 2
fi

BROKER_BIN="$(realpath "$1")"
NAMESRV_BIN="$(realpath "$2")"
CLIENT_BIN="$(realpath "$3")"
WORK_DIR="$4"
PROFILE_DIR="$5"
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

# Smaller CommitLog files than the 1 GiB default keep the training store small and still
# exercise file rolls.
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

export ROCKETMQ_HOME="${HOME_DIR}"
export ROCKETMQ_SECURITY_PROFILE="development-insecure-loopback"
export ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS=30
export RUST_LOG=warn

NAMESRV_PID=""
BROKER_PID=""

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

# Stops a process with SIGTERM and waits for it; the Broker writes its profile on this exit.
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
  stop_process "${BROKER_PID}" || true
  stop_process "${NAMESRV_PID}" || true
}
trap cleanup EXIT

client() {
  local topic="$1" scenario="$2" count="$3" inflight="$4"
  local log="${HOME_DIR}/client-${topic}-${scenario}-${inflight}-${BASHPID}.log"
  if ! RUST_LOG=error "${CLIENT_BIN}" --namesrv 127.0.0.1:9876 --topic "${topic}" --scenario "${scenario}" \
    --message-count "${count}" --message-size 1024 --batch-size 32 --inflight "${inflight}" \
    --timeout-ms 10000 > "${log}" 2>&1; then
    echo "training client ${scenario} on ${topic} failed; log:" >&2
    tail -n 50 "${log}" >&2 || true
    return 1
  fi
}

ROCKETMQ_HEALTH_BIND_ADDR=127.0.0.1:18000 "${NAMESRV_BIN}" -c "${HOME_DIR}/namesrv.toml" \
  > "${HOME_DIR}/namesrv.log" 2>&1 &
NAMESRV_PID=$!
wait_ready namesrv 18000 "${NAMESRV_PID}"

LLVM_PROFILE_FILE="${PROFILE_DIR}/broker-%p-%m.profraw" ROCKETMQ_HEALTH_BIND_ADDR=127.0.0.1:18001 \
  "${BROKER_BIN}" -c "${HOME_DIR}/broker.toml" > "${HOME_DIR}/broker.log" 2>&1 &
BROKER_PID=$!
wait_ready broker 18001 "${BROKER_PID}"

started=${SECONDS}
for topic in PgoSync PgoAsync PgoLite PgoMulti; do
  client "${topic}" sync 1 1
done
sleep 3

client PgoSync sync 20000 1
client PgoAsync async 200000 256
client PgoLite lite-pull 200000 256
client PgoAsync async 100000 16
session_pids=()
for _ in 1 2 3 4; do
  client PgoMulti async 50000 64 &
  session_pids+=("$!")
done
for pid in "${session_pids[@]}"; do
  wait "${pid}"
done
echo "trained the Broker in $((SECONDS - started)) s"

stop_process "${BROKER_PID}"
BROKER_PID=""
stop_process "${NAMESRV_PID}" || true
NAMESRV_PID=""

shopt -s nullglob
profiles=("${PROFILE_DIR}"/*.profraw)
if [[ ${#profiles[@]} -eq 0 ]]; then
  echo "the instrumented Broker wrote no profile to ${PROFILE_DIR}" >&2
  exit 1
fi
echo "raw profiles: ${#profiles[@]} in ${PROFILE_DIR}"
