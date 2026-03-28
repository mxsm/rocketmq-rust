#!/bin/bash

# Note: We don't use 'set -e' here because we want to continue processing
# all projects even if some fail, and report a summary at the end.
set -u
set -o pipefail

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default options
DRY_RUN=false
SKIP_PACKAGE=false
ALLOW_DIRTY=false
SPECIFIC_PROJECT=""
VERBOSE=false
ALL_FEATURES=false
NO_DEFAULT_FEATURES=false
FEATURES=""

# Statistics
SUCCESS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
MATCH_COUNT=0

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
CURRENT_DIR="$(pwd)"

# Publishable workspace crates in dependency order.
# Standalone projects are excluded:
# - rocketmq-example
# - rocketmq-dashboard/rocketmq-dashboard-gpui
# - rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri
# Temporarily excluded from default release:
# - rocketmq-controller
# - rocketmq-broker
# - rocketmq-proxy
PROJECT_SPECS=(
    "rocketmq-error|rocketmq-error|rocketmq-error"
    "rocketmq-macros|rocketmq-macros|rocketmq-macros"
    "rocketmq-runtime|rocketmq-runtime|rocketmq-runtime"
    "rocketmq-dashboard-common|rocketmq-dashboard/rocketmq-dashboard-common|rocketmq-dashboard-common"
    "rocketmq-admin-cli|rocketmq-tools/rocketmq-admin/rocketmq-admin-cli|rocketmq-admin-cli"
    "rocketmq-rust|rocketmq|rocketmq,rocketmq-rust"
    "rocketmq-admin-tui|rocketmq-tools/rocketmq-admin/rocketmq-admin-tui|rocketmq-admin-tui"
    "rocketmq-common|rocketmq-common|rocketmq-common"
    "rocketmq-filter|rocketmq-filter|rocketmq-filter"
    "rocketmq-remoting|rocketmq-remoting|rocketmq-remoting"
    "rocketmq-auth|rocketmq-auth|rocketmq-auth"
    "rocketmq-client-rust|rocketmq-client|rocketmq-client,rocketmq-client-rust"
    "rocketmq-namesrv|rocketmq-namesrv|rocketmq-namesrv"
    "rocketmq-store|rocketmq-store|rocketmq-store"
    "rocketmq-admin-core|rocketmq-tools/rocketmq-admin/rocketmq-admin-core|rocketmq-admin-core"
    "rocketmq-store-inspect|rocketmq-tools/rocketmq-store-inspect|rocketmq-store-inspect"
)

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

cleanup() {
    cd "$CURRENT_DIR" >/dev/null 2>&1 || true
}

matches_project_filter() {
    local filter="$1"
    local project_name="$2"
    local project_path="$3"
    local project_aliases="$4"
    local alias=""

    if [ -z "$filter" ]; then
        return 0
    fi

    if [ "$filter" = "$project_name" ] || [ "$filter" = "$project_path" ]; then
        return 0
    fi

    IFS=',' read -r -a alias_list <<< "$project_aliases"
    for alias in "${alias_list[@]}"; do
        if [ "$filter" = "$alias" ]; then
            return 0
        fi
    done

    return 1
}

show_usage() {
    cat << EOF
========================================
RocketMQ Rust Workspace Publisher
========================================

USAGE:
    $(basename "$0") [OPTIONS]

DESCRIPTION:
    Package and publish the current RocketMQ Rust workspace crates to crates.io
    in dependency order. Standalone projects are excluded from this workflow.

OPTIONS:
    --dry-run              Run cargo package without publishing
    --skip-package         Skip cargo package step, only publish
    --allow-dirty          Allow publishing with uncommitted changes
    --project NAME         Only package/publish a specific package or alias
    --verbose              Enable verbose output
    --all-features         Activate all available features
    --no-default-features  Do not activate default features
    --features FEATURES    Space or comma separated list of features
    --help, -h             Show this help message

EXAMPLES:
    1. Test packaging without publishing:
       ./$(basename "$0") --dry-run

    2. Publish all workspace crates with verbose output:
       ./$(basename "$0") --verbose

    3. Publish a specific package:
       ./$(basename "$0") --project rocketmq-client-rust

    4. Publish using a compatible alias:
       ./$(basename "$0") --project rocketmq-client

    5. Publish with all features enabled:
       ./$(basename "$0") --all-features

    6. Publish with specific features:
       ./$(basename "$0") --features "local_file_store,async_fs"

    7. Allow dirty working directory (for testing):
       ./$(basename "$0") --allow-dirty --dry-run

    8. Skip packaging, only publish:
       ./$(basename "$0") --skip-package

PUBLISH ORDER:
    rocketmq-error -> rocketmq-macros -> rocketmq-runtime ->
    rocketmq-dashboard-common -> rocketmq-admin-cli -> rocketmq-rust ->
    rocketmq-admin-tui -> rocketmq-common -> rocketmq-filter ->
    rocketmq-remoting -> rocketmq-auth -> rocketmq-client-rust ->
    rocketmq-namesrv -> rocketmq-store -> rocketmq-admin-core ->
    rocketmq-store-inspect

NOTES:
    - Requires cargo login credentials configured
    - Each package must have a valid Cargo.toml
    - Working directory should be clean (unless --allow-dirty)
    - Standalone projects are not published by this script:
      rocketmq-example,
      rocketmq-dashboard/rocketmq-dashboard-gpui,
      rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri
    - Default release temporarily excludes:
      rocketmq-controller,
      rocketmq-broker,
      rocketmq-proxy

EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-package)
            SKIP_PACKAGE=true
            shift
            ;;
        --allow-dirty)
            ALLOW_DIRTY=true
            shift
            ;;
        --project)
            SPECIFIC_PROJECT="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --all-features)
            ALL_FEATURES=true
            shift
            ;;
        --no-default-features)
            NO_DEFAULT_FEATURES=true
            shift
            ;;
        --features)
            FEATURES="$2"
            shift 2
            ;;
        --help|-h)
            show_usage
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

trap cleanup EXIT

if [ ! -f "$WORKSPACE_ROOT/Cargo.toml" ]; then
    print_error "Workspace root Cargo.toml not found: $WORKSPACE_ROOT"
    exit 1
fi

cd "$WORKSPACE_ROOT"

print_header "RocketMQ Rust Workspace Publisher"
print_info "Workspace Root: $WORKSPACE_ROOT"
print_info "Start Time: $(date '+%Y-%m-%d %H:%M:%S')"
print_info "Standalone projects are excluded from publishing"
if [ "$DRY_RUN" = true ]; then
    print_warning "Mode: DRY RUN (no publishing)"
fi
if [ "$SKIP_PACKAGE" = true ]; then
    print_warning "Mode: SKIP PACKAGE"
fi
if [ "$ALLOW_DIRTY" = true ]; then
    print_warning "Mode: ALLOW DIRTY"
fi
if [ "$ALL_FEATURES" = true ]; then
    print_info "Features: ALL FEATURES"
fi
if [ "$NO_DEFAULT_FEATURES" = true ]; then
    print_info "Features: NO DEFAULT FEATURES"
fi
if [ -n "$FEATURES" ]; then
    print_info "Features: $FEATURES"
fi
if [ -n "$SPECIFIC_PROJECT" ]; then
    print_info "Target: $SPECIFIC_PROJECT"
fi
echo ""

TOTAL_PROJECTS=${#PROJECT_SPECS[@]}
CURRENT_INDEX=0

for SPEC in "${PROJECT_SPECS[@]}"; do
    CURRENT_INDEX=$((CURRENT_INDEX + 1))
    IFS='|' read -r PROJECT_NAME PROJECT_PATH PROJECT_ALIASES <<< "$SPEC"

    if ! matches_project_filter "$SPECIFIC_PROJECT" "$PROJECT_NAME" "$PROJECT_PATH" "$PROJECT_ALIASES"; then
        SKIP_COUNT=$((SKIP_COUNT + 1))
        continue
    fi

    MATCH_COUNT=$((MATCH_COUNT + 1))

    echo ""
    print_info "[$CURRENT_INDEX/$TOTAL_PROJECTS] Processing: $PROJECT_NAME ($PROJECT_PATH)"

    PROJECT_DIR="$WORKSPACE_ROOT/$PROJECT_PATH"
    if [ ! -d "$PROJECT_DIR" ]; then
        print_error "[$PROJECT_NAME] Directory not found: $PROJECT_PATH"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi

    cd "$PROJECT_DIR"

    if [ "$SKIP_PACKAGE" = false ]; then
        print_info "[$PROJECT_NAME] Running cargo package..."
        PACKAGE_ARGS=()
        if [ "$ALLOW_DIRTY" = true ]; then
            PACKAGE_ARGS+=("--allow-dirty")
        fi
        if [ "$VERBOSE" = true ]; then
            PACKAGE_ARGS+=("--verbose")
        fi
        if [ "$ALL_FEATURES" = true ]; then
            PACKAGE_ARGS+=("--all-features")
        fi
        if [ "$NO_DEFAULT_FEATURES" = true ]; then
            PACKAGE_ARGS+=("--no-default-features")
        fi
        if [ -n "$FEATURES" ]; then
            PACKAGE_ARGS+=("--features" "$FEATURES")
        fi

        if cargo package "${PACKAGE_ARGS[@]}"; then
            print_success "[$PROJECT_NAME] Package created successfully"
        else
            print_error "[$PROJECT_NAME] cargo package failed"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            cd "$WORKSPACE_ROOT"
            continue
        fi
    fi

    if [ "$DRY_RUN" = false ]; then
        print_info "[$PROJECT_NAME] Publishing to crates.io..."
        PUBLISH_ARGS=()
        if [ "$ALLOW_DIRTY" = true ]; then
            PUBLISH_ARGS+=("--allow-dirty")
        fi
        if [ "$VERBOSE" = true ]; then
            PUBLISH_ARGS+=("--verbose")
        fi
        if [ "$ALL_FEATURES" = true ]; then
            PUBLISH_ARGS+=("--all-features")
        fi
        if [ "$NO_DEFAULT_FEATURES" = true ]; then
            PUBLISH_ARGS+=("--no-default-features")
        fi
        if [ -n "$FEATURES" ]; then
            PUBLISH_ARGS+=("--features" "$FEATURES")
        fi

        if cargo publish "${PUBLISH_ARGS[@]}"; then
            print_success "[$PROJECT_NAME] Published successfully"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            print_error "[$PROJECT_NAME] cargo publish failed"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            cd "$WORKSPACE_ROOT"
            continue
        fi
    else
        print_warning "[$PROJECT_NAME] SKIPPED: dry-run mode enabled"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    fi

    cd "$WORKSPACE_ROOT"
done

echo ""
print_header "Summary"
print_success "Success: $SUCCESS_COUNT"
if [ $FAIL_COUNT -gt 0 ]; then
    print_error "Failed:  $FAIL_COUNT"
fi
if [ $SKIP_COUNT -gt 0 ]; then
    print_info "Skipped: $SKIP_COUNT"
fi
print_info "End Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

if [ -n "$SPECIFIC_PROJECT" ] && [ $MATCH_COUNT -eq 0 ]; then
    print_error "No package matched --project '$SPECIFIC_PROJECT'"
    exit 1
fi

if [ $FAIL_COUNT -gt 0 ]; then
    print_error "Some packages failed to publish"
    exit 1
fi

print_success "All packages processed successfully!"
exit 0
