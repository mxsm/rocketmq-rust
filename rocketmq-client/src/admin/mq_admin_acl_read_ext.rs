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

//! Exact-Broker, read-only, secret-free ACL user capability.

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::body::user_info::UserInfo;

use super::default_mq_admin_ext::DefaultMQAdminExt;

/// One ACL user as a Broker lists it, without its password.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AclUserRead {
    pub username: String,
    pub user_type: Option<String>,
    pub user_status: Option<String>,
}

/// Additive capability for listing the ACL users one already resolved Broker
/// holds. Address resolution remains the responsibility of a trusted adapter.
///
/// A Broker answers with the password of every user. It is dropped here and
/// never crosses this trait's result boundary.
#[allow(async_fn_in_trait)]
pub trait MQAdminAclReadExt: Send {
    /// Lists the ACL users of one exact Broker, without their passwords.
    async fn acl_users_at(&self, broker_addr: CheetahString) -> crate::ClientResult<Vec<AclUserRead>>;
}

impl MQAdminAclReadExt for DefaultMQAdminExt {
    async fn acl_users_at(&self, broker_addr: CheetahString) -> crate::ClientResult<Vec<AclUserRead>> {
        let users = self
            .inner()
            .mq_client_api()?
            .list_users(
                broker_addr,
                CheetahString::new(),
                self.inner().remoting_timeout_millis()?,
            )
            .await?;
        Ok(without_passwords(users))
    }
}

/// Keeps the name, the type and the status of every user that has a name.
fn without_passwords(users: Vec<UserInfo>) -> Vec<AclUserRead> {
    users
        .into_iter()
        .filter_map(|user| {
            Some(AclUserRead {
                username: user.username.filter(|username| !username.is_empty())?.to_string(),
                user_type: user.user_type.map(|value| value.to_string()),
                user_status: user.user_status.map(|value| value.to_string()),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listed_user_keeps_its_name_type_and_status_and_never_its_password() {
        let listed = vec![
            UserInfo {
                username: Some("ops".into()),
                password: Some("ops-secret".into()),
                user_type: Some("Super".into()),
                user_status: Some("enable".into()),
            },
            UserInfo {
                username: Some("reader".into()),
                password: None,
                user_type: None,
                user_status: None,
            },
            // A row without a name is no user.
            UserInfo {
                username: None,
                password: Some("orphan-secret".into()),
                user_type: Some("Normal".into()),
                user_status: Some("enable".into()),
            },
        ];

        let users = without_passwords(listed);

        assert_eq!(
            users,
            [
                AclUserRead {
                    username: "ops".to_owned(),
                    user_type: Some("Super".to_owned()),
                    user_status: Some("enable".to_owned()),
                },
                AclUserRead {
                    username: "reader".to_owned(),
                    user_type: None,
                    user_status: None,
                },
            ]
        );
        let rendered = format!("{users:?}");
        assert!(!rendered.contains("secret"));
    }
}
