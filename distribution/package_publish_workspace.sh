#!/bin/bash

# Save the current directory
CURRENT_DIR=$(pwd)

# Navigate to the workspace root directory
cd ..

echo "Starting to package Rust workspace projects..."

PROJECTS=(
    "rocketmq-common"
    "rocketmq-runtime"
    "rocketmq-macros"
    "rocketmq-error"
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
)

for PROJECT in "${PROJECTS[@]}"
do
    echo "Packaging $PROJECT..."
    cd $PROJECT
    cargo package
    cargo publish
    if [ $? -ne 0 ]; then
        echo "$PROJECT packaging failed."
        cd $CURRENT_DIR
        exit 1
    fi
    cd ..
done

echo "Finished packaging projects."
# Return to the original directory
cd $CURRENT_DIR