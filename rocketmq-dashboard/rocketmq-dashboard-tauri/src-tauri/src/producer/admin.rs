// Copyright 2023 The RocketMQ Rust Authors
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

use crate::nameserver::NameServerRuntimeState;
use crate::producer::types::ProducerError;
use crate::producer::types::ProducerResult;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::client_adapter::ClientAdminBuilder;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct ManagedProducerAdmin {
    pub(crate) admin: AdminSession,
    pub(crate) snapshot: NameServerConfigSnapshot,
    pub(crate) generation: u64,
}

impl ManagedProducerAdmin {
    pub(crate) async fn connect(runtime: &Arc<NameServerRuntimeState>) -> ProducerResult<Self> {
        let (snapshot, generation) = runtime.snapshot_and_generation();
        let current_namesrv = snapshot.current_namesrv.clone().ok_or_else(|| {
            ProducerError::Configuration(
                "No active NameServer is configured. Add and select a NameServer first.".into(),
            )
        })?;

        let admin = ClientAdminBuilder::new()
            .admin_group(format!("dashboard-producer-admin-{}", Uuid::new_v4()))
            .namesrv_addr(current_namesrv)
            .timeout_millis(5_000)
            .vip_channel_enabled(snapshot.use_vip_channel)
            .use_tls(snapshot.use_tls)
            .build_and_start()
            .await
            .map_err(ProducerError::from)?;

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
