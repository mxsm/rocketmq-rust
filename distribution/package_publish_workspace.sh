#!/bin/bash

# Note: We don't use 'set -e' here because we want to continue processing
# all projects even if some fail, and report a summary at the end.
set -u  # Exit on undefined variable
set -o pipefail  # Exit on pipe failure

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Function to print colored messages
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

# Function to show usage
show_usage() {
    cat << EOF
========================================
RocketMQ Rust Workspace Publisher
========================================

USAGE:
    $(basename $0) [OPTIONS]

DESCRIPTION:
    Automate packaging and publishing of all RocketMQ Rust workspace
    crates to crates.io in correct dependency order.

OPTIONS:
    --dry-run              Run cargo package without publishing
    --skip-package         Skip cargo package step, only publish
    --allow-dirty          Allow publishing with uncommitted changes
    --project NAME         Only package/publish specific project
    --verbose              Enable verbose output
    --all-features         Activate all available features
    --no-default-features  Do not activate default features
    --features FEATURES    Space or comma separated list of features
    --help, -h             Show this help message

EXAMPLES:
    1. Test packaging without publishing:
       ./$(basename $0) --dry-run

    2. Publish all projects with verbose output:
       ./$(basename $0) --verbose

    3. Publish specific project only:
       ./$(basename $0) --project rocketmq-common

    4. Publish with all features enabled:
       ./$(basename $0) --all-features

    5. Publish with specific features:
       ./$(basename $0) --features "local_file_store,async_fs"

    6. Allow dirty working directory (for testing):
       ./$(basename $0) --allow-dirty --dry-run

    7. Skip packaging, only publish:
       ./$(basename $0) --skip-package

    8. Combine multiple options:
       ./$(basename $0) --project rocketmq-broker --all-features --verbose --dry-run

PUBLISH ORDER:
    Projects are published in dependency order:
    rocketmq-error → rocketmq-common → rocketmq-runtime →
    rocketmq-macros → rocketmq → rocketmq-filter → rocketmq-store →
    rocketmq-remoting → rocketmq-cli → rocketmq-client →
    rocketmq-namesrv → rocketmq-broker → rocketmq-tools →
    rocketmq-tui → rocketmq-example → rocketmq-controller

NOTES:
    - Requires cargo login credentials configured
    - Each project must have a valid Cargo.toml
    - Working directory should be clean (unless --allow-dirty)
    - Script will continue on failure and report summary at end

EOF
    exit 0
}

# Parse command line arguments
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

# Save the current directory
CURRENT_DIR=$(pwd)

# Cleanup function
cleanup() {
    cd "$CURRENT_DIR"
}

# Set trap to ensure we return to original directory on exit
trap cleanup EXIT

# Navigate to the workspace root directory
cd ..

# Display configuration
print_header "RocketMQ Rust Workspace Publisher"
print_info "Start Time: $(date '+%Y-%m-%d %H:%M:%S')"
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
    print_info "Target: $SPECIFIC_PROJECT only"
fi
echo ""

# Define projects in dependency order
PROJECTS=(
    "rocketmq-error"
    "rocketmq-common"
    "rocketmq-runtime"
    "rocketmq-macros"
    "rocketmq"
    "rocketmq-filter"
    "rocketmq-store"
    "rocketmq-remoting"
    "rocketmq-cli"
    "rocketmq-client"
    "rocketmq-namesrv"
    "rocketmq-broker"
    "rocketmq-tools"
    "rocketmq-tui"
    "rocketmq-example"
    "rocketmq-controller"
)

TOTAL_PROJECTS=${#PROJECTS[@]}
CURRENT_INDEX=0

# Process each project
for PROJECT in "${PROJECTS[@]}"; do
    CURRENT_INDEX=$((CURRENT_INDEX + 1))
    
    # Skip if specific project is set and this is not it
    if [ -n "$SPECIFIC_PROJECT" ] && [ "$PROJECT" != "$SPECIFIC_PROJECT" ]; then
        SKIP_COUNT=$((SKIP_COUNT + 1))
        continue
    fi
    
    echo ""
    print_info "[$CURRENT_INDEX/$TOTAL_PROJECTS] Processing: $PROJECT"
    
    # Check if directory exists
    if [ ! -d "$PROJECT" ]; then
        print_error "[$PROJECT] Directory not found"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi
    
    cd "$PROJECT"
    
    # Run cargo package unless skipped
    if [ "$SKIP_PACKAGE" = false ]; then
        print_info "[$PROJECT] Running cargo package..."
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
            print_success "[$PROJECT] Package created successfully"
        else
            print_error "[$PROJECT] cargo package failed"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            cd ..
            continue
        fi
    fi
    
    # Run cargo publish unless dry-run
    if [ "$DRY_RUN" = false ]; then
        print_info "[$PROJECT] Publishing to crates.io..."
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
            print_success "[$PROJECT] Published successfully"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            print_error "[$PROJECT] cargo publish failed"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            cd ..
            continue
        fi
    else
        print_warning "[$PROJECT] SKIPPED: dry-run mode enabled"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    fi
    
    cd ..
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

if [ $FAIL_COUNT -gt 0 ]; then
    print_error "Some projects failed to publish"
    exit 1
fi

print_success "All projects processed successfully!"
exit 0