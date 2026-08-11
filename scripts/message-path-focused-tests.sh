#!/usr/bin/env bash

set -euo pipefail

scope="all"
list_only=false

usage() {
    cat <<'EOF'
Usage: message-path-focused-tests.sh [--scope send|store|ha|consume|proxy|all] [--list-only]
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --scope)
            if [[ $# -lt 2 ]]; then
                echo "--scope requires a value" >&2
                usage >&2
                exit 2
            fi
            scope="$2"
            shift 2
            ;;
        --list-only)
            list_only=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

case "$scope" in
    send|store|ha|consume|proxy|all) ;;
    *)
        echo "Unknown scope: $scope" >&2
        usage >&2
        exit 2
        ;;
esac

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
workspace_root="$(cd -- "$script_dir/.." && pwd)"

run_command() {
    local domain="$1"
    shift

    printf '[%s] cargo' "$domain"
    printf ' %q' "$@"
    printf '\n'

    if [[ "$list_only" == false ]]; then
        cargo "$@"
    fi
}

run_send() {
    run_command send test -p rocketmq-protocol --test request_header_java_compatibility
}

run_store() {
    run_command store test -p rocketmq-store --test timer_java_compat_tests
}

run_ha() {
    run_command ha test -p rocketmq-store --test ha_transfer_engine
}

run_consume() {
    run_command consume test -p rocketmq-client-rust --test pull_message_service_test
    run_command consume test -p rocketmq-client-rust --test lite_pull_capability_tests
    run_command consume test -p rocketmq-client-rust --test lite_pull_assignment_registry_tests
}

run_proxy() {
    run_command proxy test -p rocketmq-proxy --test grpc_ingress
    run_command proxy test -p rocketmq-proxy-cluster
    run_command proxy test -p rocketmq-proxy-local
}

cd "$workspace_root"

if [[ "$scope" == "all" ]]; then
    run_send
    run_store
    run_ha
    run_consume
    run_proxy
else
    "run_${scope}"
fi

if [[ "$list_only" == true ]]; then
    echo "Focused test commands listed."
else
    echo "Focused tests completed."
fi
