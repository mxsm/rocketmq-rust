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

use super::*;
pub(super) fn validate_acl_file_path_for_global_white_addr_config(
    acl_file_full_path: Option<&CheetahString>,
) -> rocketmq_error::RocketMQResult<()> {
    if acl_file_full_path.is_some_and(|acl_file_full_path| !acl_file_full_path.is_empty()) {
        return Err(RocketMQError::illegal_argument(
            "acl_file_full_path is not supported by RocketMQ ACL 2.0 global white address updates",
        ));
    }
    Ok(())
}

impl DefaultMQAdminExtImpl {
    pub async fn create_acl_with_acl_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        if acl_info.subject.as_ref().is_none_or(|subject| subject.is_empty()) {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(
                "ACL subject is required".into(),
            ));
        }

        if let Some(ref client_instance) = self.client_instance {
            client_instance
                .get_mq_client_api_impl()?
                .create_acl(broker_addr, &acl_info, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    pub async fn update_acl_with_acl_info(
        &self,
        broker_addr: CheetahString,
        acl_info: AclInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        if acl_info.subject.as_ref().is_none_or(|subject| subject.is_empty()) {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(
                "ACL subject is required".into(),
            ));
        }

        if let Some(ref client_instance) = self.client_instance {
            client_instance
                .get_mq_client_api_impl()?
                .update_acl(broker_addr, &acl_info, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    pub async fn create_user_with_user_info(
        &self,
        broker_addr: CheetahString,
        user_info: UserInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        let username = user_info
            .username
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("User username is required".into()))?;

        let password = user_info.password.clone().unwrap_or_default();
        let user_type = user_info.user_type.clone().unwrap_or_default();

        self.create_user(broker_addr, username, password, user_type).await
    }

    pub async fn update_user_with_user_info(
        &self,
        broker_addr: CheetahString,
        user_info: UserInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        let username = user_info
            .username
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("User username is required".into()))?;

        let password = user_info.password.clone().unwrap_or_default();
        let user_type = user_info.user_type.clone().unwrap_or_default();
        let user_status = user_info.user_status.clone().unwrap_or_default();

        self.update_user(broker_addr, username, password, user_type, user_status)
            .await
    }
}

pub(super) fn build_acl_info(
    subject: CheetahString,
    resources: Vec<CheetahString>,
    actions: Vec<CheetahString>,
    source_ips: Vec<CheetahString>,
    decision: CheetahString,
) -> AclInfo {
    let entries = if resources.is_empty() {
        vec![PolicyEntryInfo {
            resource: None,
            actions: Some(actions),
            source_ips: Some(source_ips),
            decision: if decision.is_empty() { None } else { Some(decision) },
        }]
    } else {
        resources
            .into_iter()
            .map(|resource| PolicyEntryInfo {
                resource: Some(resource),
                actions: Some(actions.clone()),
                source_ips: Some(source_ips.clone()),
                decision: if decision.is_empty() {
                    None
                } else {
                    Some(decision.clone())
                },
            })
            .collect()
    };

    AclInfo {
        subject: Some(subject),
        policies: Some(vec![PolicyInfo {
            policy_type: Some(CheetahString::from_static_str("Custom")),
            entries: Some(entries),
        }]),
    }
}
