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

use crate::consumer::types::ConsumerError;
use crate::consumer::types::ConsumerResult;
use crate::nameserver::NameServerRuntimeState;
use rocketmq_admin_core::admin::default_mq_admin_ext::DefaultMQAdminExt;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub(crate) struct ManagedConsumerAdmin {
    pub(crate) admin: DefaultMQAdminExt,
    pub(crate) snapshot: NameServerConfigSnapshot,
    pub(crate) generation: u64,
}

impl ManagedConsumerAdmin {
    pub(crate) async fn connect(runtime: &Arc<NameServerRuntimeState>) -> ConsumerResult<Self> {
        let (snapshot, generation) = runtime.snapshot_and_generation();
        let current_namesrv = snapshot.current_namesrv.clone().ok_or_else(|| {
            ConsumerError::Configuration(
                "No active NameServer is configured. Add and select a NameServer first.".into(),
            )
        })?;

        let mut admin = DefaultMQAdminExt::with_admin_ext_group_and_timeout(
            format!("dashboard-consumer-admin-{}", Uuid::new_v4()),
            Duration::from_millis(5_000),
        );
        admin
            .client_config_mut()
            .set_namesrv_addr(current_namesrv.clone().into());
        admin
            .client_config_mut()
            .set_vip_channel_enabled(snapshot.use_vip_channel);
        admin.client_config_mut().set_use_tls(snapshot.use_tls);
        admin
            .start()
            .await
            .map_err(|error| ConsumerError::RocketMQ(error.to_string()))?;

        Ok(Self {
            admin,
            snapshot,
            generation,
        })
    }

    pub(crate) fn matches_generation(&self, generation: u64) -> bool {
        self.generation == generation
    }

    pub(crate) async fn shutdown(&mut self) {
        self.admin.shutdown().await;
    }
}
