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

use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_admin_core::admin::LegacyMQAdminExt;
use rocketmq_admin_core::client_adapter::legacy::compat_types::TopicConfig;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::client_adapter::ClientAdminBuilder;
use rocketmq_admin_core::core::broker::BrokerAdmin;
use rocketmq_admin_core::core::consumer::ConsumerAdmin;
use rocketmq_admin_core::core::lite::LiteAdmin;
use rocketmq_admin_core::core::security::SecurityAdmin;
use rocketmq_admin_core::core::topic::TopicAdmin;

fn assert_mq_admin_ext<T: LegacyMQAdminExt>() {}

fn assert_modern_capabilities<T: TopicAdmin + BrokerAdmin + ConsumerAdmin + SecurityAdmin + LiteAdmin>() {}

#[test]
fn legacy_default_admin_surface_still_compiles() {
    assert_mq_admin_ext::<DefaultMQAdminExt>();

    let mut admin = DefaultMQAdminExt::new();
    let _ = admin.client_config();
    let _ = admin.client_config_mut();

    let topic_config = TopicConfig::default();
    let future = admin.create_and_update_topic_config("127.0.0.1:10911".into(), topic_config);
    drop(future);
}

#[test]
fn modern_adapter_remains_available_when_legacy_is_enabled() {
    let _ = ClientAdminBuilder::new().instance_name("feature-union");
    assert_modern_capabilities::<AdminSession>();
}
