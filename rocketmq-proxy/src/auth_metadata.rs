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

use cheetah_string::CheetahString;
use rocketmq_auth::authentication::enums::subject_type::SubjectType;
use rocketmq_auth::authentication::enums::user_status::UserStatus;
use rocketmq_auth::authentication::enums::user_type::UserType;
use rocketmq_auth::authentication::model::user::User;
use rocketmq_auth::authorization::enums::decision::Decision;
use rocketmq_auth::authorization::enums::policy_type::PolicyType;
use rocketmq_auth::authorization::model::acl::Acl;
use rocketmq_auth::authorization::model::environment::Environment;
use rocketmq_auth::authorization::model::policy::Policy;
use rocketmq_auth::authorization::model::policy_entry::PolicyEntry;
use rocketmq_auth::authorization::model::resource::Resource;
use rocketmq_common::common::action::Action;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyEntryInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyInfo;
use rocketmq_remoting::protocol::body::user_info::UserInfo;

pub(crate) fn user_from_info(user_info: &UserInfo) -> Option<User> {
    let username = user_info.username.as_ref()?.trim();
    if username.is_empty() {
        return None;
    }
    let mut user = User::of(CheetahString::from(username));

    if let Some(password) = user_info.password.as_ref().filter(|password| !password.is_empty()) {
        user.set_password(password.clone());
    }
    if let Some(user_type_name) = user_info.user_type.as_ref().filter(|user_type| !user_type.is_empty()) {
        if let Some(user_type) = UserType::get_by_name(user_type_name) {
            user.set_user_type(user_type);
        }
    }
    if let Some(user_status_name) = user_info
        .user_status
        .as_ref()
        .filter(|user_status| !user_status.is_empty())
    {
        if let Some(user_status) = UserStatus::get_by_name(user_status_name) {
            user.set_user_status(user_status);
        }
    }

    Some(user)
}

pub(crate) fn acl_from_info(acl_info: &AclInfo, fallback_subject: &str) -> RocketMQResult<Option<Acl>> {
    let subject = acl_info
        .subject
        .as_ref()
        .filter(|subject| !subject.trim().is_empty())
        .map(|subject| subject.as_str())
        .unwrap_or(fallback_subject);
    let (subject_key, subject_type) = parse_subject(subject)?;
    let policies = acl_info
        .policies
        .as_ref()
        .map(|policies| {
            policies
                .iter()
                .map(convert_policy_info)
                .collect::<RocketMQResult<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    if policies.is_empty() {
        return Ok(None);
    }

    Ok(Some(Acl::of_with_policies(subject_key, subject_type, policies)))
}

fn convert_policy_info(policy: &PolicyInfo) -> RocketMQResult<Policy> {
    let policy_type = policy
        .policy_type
        .as_ref()
        .map(|policy_type| {
            PolicyType::get_by_name(policy_type.as_str())
                .ok_or_else(|| RocketMQError::illegal_argument(format!("Invalid policy type '{}'", policy_type)))
        })
        .transpose()?
        .unwrap_or(PolicyType::Custom);
    let entries = policy
        .entries
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(convert_policy_entry_info)
                .collect::<RocketMQResult<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    if entries.is_empty() {
        return Err(RocketMQError::illegal_argument("The policy entries is empty."));
    }
    Ok(Policy::of_entries(policy_type, entries))
}

fn convert_policy_entry_info(entry: &PolicyEntryInfo) -> RocketMQResult<PolicyEntry> {
    let resource_key = entry
        .resource
        .as_ref()
        .ok_or_else(|| RocketMQError::illegal_argument("The resource is null."))?;
    let resource = Resource::of_str(resource_key.as_str())
        .ok_or_else(|| RocketMQError::illegal_argument(format!("Invalid resource '{}'", resource_key)))?;

    let actions = entry
        .actions
        .as_ref()
        .ok_or_else(|| RocketMQError::illegal_argument("The actions is empty."))?
        .iter()
        .flat_map(|action| action.as_str().split(','))
        .map(str::trim)
        .filter(|action| !action.is_empty())
        .map(|action| {
            Action::get_by_name(action)
                .ok_or_else(|| RocketMQError::illegal_argument(format!("Invalid action '{action}'")))
        })
        .collect::<RocketMQResult<Vec<_>>>()?;
    if actions.is_empty() {
        return Err(RocketMQError::illegal_argument("The actions is empty."));
    }

    let environment = entry.source_ips.as_ref().and_then(|source_ips| {
        let filtered = source_ips
            .iter()
            .map(|source_ip| source_ip.as_str().trim().to_string())
            .filter(|source_ip| !source_ip.is_empty())
            .collect::<Vec<_>>();
        if filtered.is_empty() {
            None
        } else {
            Environment::of_list(filtered)
        }
    });

    let decision_name = entry
        .decision
        .as_ref()
        .ok_or_else(|| RocketMQError::illegal_argument("The decision is null."))?;
    let decision = Decision::get_by_name(decision_name.as_str())
        .ok_or_else(|| RocketMQError::illegal_argument(format!("Invalid decision '{}'", decision_name)))?;

    Ok(PolicyEntry::of(resource, actions, environment, decision))
}

fn parse_subject(subject: &str) -> RocketMQResult<(String, SubjectType)> {
    let trimmed = subject.trim();
    if trimmed.is_empty() {
        return Err(RocketMQError::illegal_argument("The subject is blank"));
    }

    let (subject_type, subject_name) = match trimmed.split_once(':') {
        Some((subject_type, subject_name)) => (
            SubjectType::get_by_name(subject_type)
                .ok_or_else(|| RocketMQError::illegal_argument(format!("Unsupported subject type '{subject_type}'")))?,
            subject_name.trim(),
        ),
        None => (SubjectType::User, trimmed),
    };
    if subject_name.is_empty() {
        return Err(RocketMQError::illegal_argument("The subject name is blank"));
    }

    Ok((format!("{}:{}", subject_type.name(), subject_name), subject_type))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_from_info_converts_java_wire_user() {
        let user = user_from_info(&UserInfo {
            username: Some(CheetahString::from_static_str("alice")),
            password: Some(CheetahString::from_static_str("secret")),
            user_type: Some(CheetahString::from_static_str("Normal")),
            user_status: Some(CheetahString::from_static_str("Enable")),
        })
        .unwrap();

        assert_eq!(user.username().as_str(), "alice");
        assert_eq!(user.password().map(CheetahString::as_str), Some("secret"));
    }

    #[test]
    fn acl_from_info_converts_java_wire_acl() {
        let acl = acl_from_info(
            &AclInfo {
                subject: Some(CheetahString::from_static_str("User:alice")),
                policies: Some(vec![PolicyInfo {
                    policy_type: Some(CheetahString::from_static_str("Custom")),
                    entries: Some(vec![PolicyEntryInfo {
                        resource: Some(CheetahString::from_static_str("Topic:TopicA")),
                        actions: Some(vec![CheetahString::from_static_str("Pub")]),
                        source_ips: None,
                        decision: Some(CheetahString::from_static_str("Allow")),
                    }]),
                }]),
            },
            "User:alice",
        )
        .unwrap()
        .unwrap();

        assert_eq!(acl.subject_key(), "User:alice");
        assert_eq!(acl.policies().len(), 1);
    }
}
