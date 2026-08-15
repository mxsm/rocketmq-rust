// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(not(feature = "extended_timeline"))]

use std::sync::Arc;

use dashmap::DashMap;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::LocalFileMessageStore;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StoreRuntimeConfig;
use rocketmq_store_api::TimerStoreMode;

fn service_context() -> rocketmq_runtime::ChildServiceContext {
    let owner = Box::leak(Box::new(
        RuntimeOwner::new(RuntimeConfig::server_default("timer-mode-feature-gate"))
            .expect("timer feature-gate runtime"),
    ));
    owner.root_context().component("store")
}

#[tokio::test]
async fn extended_modes_fail_before_startup_without_the_feature() {
    for (mode, shadow) in [
        (TimerStoreMode::ExtendedTimeline, false),
        (TimerStoreMode::JavaCompat, true),
    ] {
        let root = tempfile::tempdir().expect("store root");
        let config = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            timer_wheel_enable: false,
            timer_store_mode: mode,
            timer_extended_shadow_enable: shadow,
            timer_extended_admission_enable: mode == TimerStoreMode::ExtendedTimeline,
            timer_extended_activation_epoch: u64::from(mode == TimerStoreMode::ExtendedTimeline),
            ..MessageStoreConfig::default()
        };
        let mut store = LocalFileMessageStore::new(
            Arc::new(config),
            Arc::new(StoreRuntimeConfig::default()),
            Arc::new(DashMap::new()),
            None,
            false,
            service_context(),
        );
        let error = store.init().await.expect_err("unsupported mode must fail closed");
        assert!(error.to_string().contains("extended_timeline feature"), "{error}");
    }
}
