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
use rocketmq_client_rust::common::acl::SigningAlgorithm;
use rocketmq_client_rust::AclClientRPCHook;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_client_rust::SessionCredentials;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::security::ListUsersRequest;
use crate::core::security::ListUsersResult;
use crate::core::security::SecurityAdmin;
use crate::core::security::UserSummary;
use crate::core::AdminError;
use crate::core::AdminFuture;

/// Build the legacy client adapter used by admin tools for a secure profile.
///
/// Credential values are retained only by the redacting client RPC hook. The
/// caller is responsible for obtaining them from a non-command-line secret
/// source such as the process environment or a mounted secret provider.
pub fn admin_acl_rpc_hook(
    access_key: impl Into<CheetahString>,
    secret_key: impl Into<CheetahString>,
    security_token: Option<impl Into<CheetahString>>,
) -> AclClientRPCHook {
    let access_key = access_key.into();
    let secret_key = secret_key.into();
    let credentials = match security_token {
        Some(security_token) => SessionCredentials::with_token(access_key, secret_key, security_token),
        None => SessionCredentials::with_keys(access_key, secret_key),
    };
    AclClientRPCHook::with_signature_algorithm(credentials, SigningAlgorithm::HmacSha256)
}

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

#[cfg(test)]
mod tests {
    use super::admin_acl_rpc_hook;

    #[test]
    fn admin_acl_hook_selects_sha256_and_redacts_credentials() {
        let hook = admin_acl_rpc_hook("access", "secret", Some("token"));

        let debug = format!("{hook:?}");
        assert!(debug.contains("HmacSha256"));
        assert!(!debug.contains("access"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("token"));
    }
}
