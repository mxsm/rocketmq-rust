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

use crate::connection::AdminPurpose;
use crate::error::DashboardError as AclError;
use crate::error::DashboardResult as AclResult;
use crate::nameserver::NameServerRuntimeState;
use rocketmq_admin_core::client_adapter::AdminSession;
use std::sync::Arc;

pub(crate) struct ManagedAclAdmin {
    pub(crate) admin: AdminSession,
    pub(crate) generation: u64,
}

impl ManagedAclAdmin {
    pub(crate) async fn connect(runtime: &Arc<NameServerRuntimeState>) -> AclResult<Self> {
        let (snapshot, generation) = runtime.snapshot_and_generation();
        let admin = runtime
            .admin_builder(&snapshot, AdminPurpose::Acl)?
            .build_and_start()
            .await
            .map_err(AclError::from)?;

        Ok(Self { admin, generation })
    }

    pub(crate) fn matches_generation(&self, generation: u64) -> bool {
        self.generation == generation
    }

    pub(crate) async fn shutdown(&mut self) {
        self.admin.shutdown().await;
    }
}
