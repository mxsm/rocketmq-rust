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

use cheetah_string::CheetahString;
use rocketmq_client_rust::MQAdminExt;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::security::ListUsersRequest;
use crate::core::security::ListUsersResult;
use crate::core::security::SecurityAdmin;
use crate::core::security::UserSummary;
use crate::core::AdminError;
use crate::core::AdminFuture;

impl SecurityAdmin for AdminSession {
    fn list_users<'a>(&'a mut self, request: &'a ListUsersRequest) -> AdminFuture<'a, ListUsersResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let users = self
                .inner
                .list_users(
                    CheetahString::from(request.broker_addr.as_str()),
                    CheetahString::from(request.filter.as_str()),
                )
                .await
                .map_err(|error| AdminError::backend("list_users", error.to_string()))?;
            Ok(ListUsersResult {
                users: users
                    .into_iter()
                    .map(|user| UserSummary {
                        username: user.username.map(|value| value.to_string()),
                        user_type: user.user_type.map(|value| value.to_string()),
                        user_status: user.user_status.map(|value| value.to_string()),
                    })
                    .collect(),
            })
        })
    }
}
