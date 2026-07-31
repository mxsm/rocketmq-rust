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

#[test]
fn admin_dispatch_is_shared_without_a_request_wide_mutex() {
    let processor = include_str!("../src/processor.rs");
    let request_pipeline = include_str!("../src/broker_runtime/request_pipeline.rs");
    let admin = include_str!("../src/processor/admin_broker_processor.rs");

    assert!(processor.contains("AdminBroker(Arc<AdminBrokerProcessor<MS>>)"));
    assert!(processor.contains("processor.process_request_shared(channel, ctx, request).await"));
    assert!(!processor.contains("Mutex<AdminBrokerProcessor"));
    assert!(!processor.contains("processor.lock().await.process_request"));
    assert!(!request_pipeline.contains("Mutex::new(AdminBrokerProcessor::new"));
    assert!(admin.contains("pub(crate) async fn process_request_shared(\n        &self,"));
    assert!(admin.contains("async fn process_request_inner(\n        &self,"));
}

#[test]
fn admin_mutations_use_domain_owned_synchronization() {
    let runtime = include_str!("../src/broker/broker_admin_runtime.rs").replace("\r\n", "\n");
    let subscription_groups =
        include_str!("../src/subscription/manager/subscription_group_manager.rs").replace("\r\n", "\n");

    assert!(runtime.contains("config_update_lock: Arc<parking_lot::Mutex<()>>"));
    assert!(runtime.contains("pub(crate) fn commit_broker_config_patch(\n        &self,"));
    assert!(!runtime.contains("subscription_group_manager_mut("));
    assert!(subscription_groups.contains("metadata_transition: Arc<parking_lot::Mutex<()>>"));
    assert!(subscription_groups.contains("pub(crate) fn update_subscription_group_config(&self,"));
    assert!(subscription_groups.contains("pub fn delete_subscription_group_config(&self,"));
}
