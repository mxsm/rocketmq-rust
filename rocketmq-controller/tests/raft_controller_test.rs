//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use std::sync::Arc;

use rocketmq_controller::Controller;
use rocketmq_controller::RaftController;
use rocketmq_runtime::RocketMQRuntime;

#[tokio::test]
async fn test_open_raft_controller_lifecycle() {
    // Use spawn_blocking to create runtime outside async context
    let runtime = tokio::task::spawn_blocking(|| Arc::new(RocketMQRuntime::new_multi(4, "test-runtime")))
        .await
        .unwrap();

    let controller = RaftController::new_open_raft(runtime.clone());

    assert!(controller.startup().await.is_ok());
    assert!(!controller.is_leader()); // Default is false
    assert!(controller.shutdown().await.is_ok());

    // Drop controller first, then runtime in blocking context
    drop(controller);
    tokio::task::spawn_blocking(move || {
        drop(runtime);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_raft_rs_controller_lifecycle() {
    let runtime = tokio::task::spawn_blocking(|| Arc::new(RocketMQRuntime::new_multi(4, "test-runtime")))
        .await
        .unwrap();

    let controller = RaftController::new_raft_rs(runtime.clone());

    assert!(controller.startup().await.is_ok());
    assert!(!controller.is_leader()); // Default is false
    assert!(controller.shutdown().await.is_ok());

    drop(controller);
    tokio::task::spawn_blocking(move || {
        drop(runtime);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_raft_controller_wrapper() {
    let runtime = tokio::task::spawn_blocking(|| Arc::new(RocketMQRuntime::new_multi(4, "test-runtime")))
        .await
        .unwrap();

    // Test OpenRaft variant
    let open_raft_controller = RaftController::new_open_raft(runtime.clone());
    assert!(open_raft_controller.startup().await.is_ok());
    assert!(!open_raft_controller.is_leader());
    assert!(open_raft_controller.shutdown().await.is_ok());
    drop(open_raft_controller);

    // Test RaftRs variant
    let raft_rs_controller = RaftController::new_raft_rs(runtime.clone());
    assert!(raft_rs_controller.startup().await.is_ok());
    assert!(!raft_rs_controller.is_leader());
    assert!(raft_rs_controller.shutdown().await.is_ok());
    drop(raft_rs_controller);

    tokio::task::spawn_blocking(move || {
        drop(runtime);
    })
    .await
    .unwrap();
}
