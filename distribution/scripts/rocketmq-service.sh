#!/usr/bin/env bash

set -euo pipefail

action="${1:-}"
service="${2:-}"
workdir="${ROCKETMQ_WORKDIR:-$(pwd)/work}"
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

case "$service" in
  namesrv) binary="rocketmq-namesrv-rust" ;;
  broker) binary="rocketmq-broker-rust" ;;
  controller) binary="rocketmq-controller-rust" ;;
  proxy) binary="rocketmq-proxy-rust" ;;
  *) echo "usage: $0 start|stop|status namesrv|broker|controller|proxy" >&2; exit 2 ;;
esac

mkdir -p "$workdir/run" "$workdir/logs" "$workdir/data/$service"
pid_file="$workdir/run/$service.pid"
log_file="$workdir/logs/$service.log"

is_running() {
  [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

case "$action" in
  start)
    if is_running; then echo "$service already running"; exit 0; fi
    ROCKETMQ_HOME="$workdir" "$root/bin/$binary" -c "$root/conf/$service.toml" \
      >>"$log_file" 2>&1 &
    echo "$!" >"$pid_file"
    ;;
  stop)
    if ! is_running; then rm -f "$pid_file"; echo "$service not running"; exit 0; fi
    kill "$(cat "$pid_file")"
    rm -f "$pid_file"
    ;;
  status)
    if is_running; then echo "$service running"; else echo "$service stopped"; exit 3; fi
    ;;
  *) echo "usage: $0 start|stop|status namesrv|broker|controller|proxy" >&2; exit 2 ;;
esac
