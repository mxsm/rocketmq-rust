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

//! The ACL users the masters of a cluster hold. This module exists only with the
//! `security-admin` feature.

use rocketmq_admin_core::core::security::AclUserQueryAdmin;
use rocketmq_admin_core::core::security::AclUserQueryRequest;

use super::map_admin_error;
use super::AdminCoreSession;
use crate::model::contract::observed_at;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::executor::ToolRejection;
use crate::tools::security_tools::AclUserRow;
use crate::tools::security_tools::ListAclUsersOutput;

impl AdminCoreSession {
    pub(super) async fn query_acl_user_observation(&mut self) -> Result<ListAclUsersOutput, ToolFailure> {
        let request = AclUserQueryRequest::try_new(self.cluster.rocketmq_cluster_name.clone())
            .map_err(|_| ToolFailure::Rejected(ToolRejection::InvalidArguments { _source: None }))?;
        let listed = self
            .admin_mut()?
            .query_acl_users(&request)
            .await
            .map_err(map_admin_error(NotFoundEntity::Cluster))?;
        Ok(ListAclUsersOutput {
            cluster: self.cluster.name.clone(),
            user_count: listed.user_count,
            users: listed
                .users
                .into_iter()
                .map(|user| AclUserRow {
                    username: user.username,
                    broker_name: user.broker_name,
                    user_type: user.user_type,
                    user_status: user.user_status,
                })
                .collect(),
            truncated: listed.truncated,
            queried_broker_count: listed.queried_broker_count,
            unanswered_brokers: listed.unanswered_brokers,
            generated_at: observed_at(),
        })
    }
}
