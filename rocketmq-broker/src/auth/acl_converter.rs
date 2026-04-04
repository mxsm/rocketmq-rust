use cheetah_string::CheetahString;
use rocketmq_auth::authentication::enums::subject_type::SubjectType;
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

pub struct AclConverter;

impl AclConverter {
    pub fn convert_acl_info(acl_info: &AclInfo, fallback_subject: &str) -> RocketMQResult<Acl> {
        let subject = acl_info
            .subject
            .as_ref()
            .filter(|subject| !subject.is_empty())
            .map(|subject| subject.as_str())
            .unwrap_or(fallback_subject);
        let (subject_key, subject_type) = parse_subject(subject)?;
        let policies = acl_info
            .policies
            .as_ref()
            .map(|policies| {
                policies
                    .iter()
                    .map(Self::convert_policy_info)
                    .collect::<RocketMQResult<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        if policies.is_empty() {
            return Err(RocketMQError::illegal_argument("The policies is empty."));
        }

        Ok(Acl::of_with_policies(subject_key, subject_type, policies))
    }

    pub fn convert_acl(acl: &Acl) -> AclInfo {
        AclInfo {
            subject: Some(CheetahString::from_string(acl.subject_key().to_string())),
            policies: Some(acl.policies().iter().map(Self::convert_policy).collect()),
        }
    }

    fn convert_policy(policy: &Policy) -> PolicyInfo {
        PolicyInfo {
            policy_type: Some(CheetahString::from_static_str(policy.policy_type().name())),
            entries: Some(policy.entries().iter().map(Self::convert_policy_entry).collect()),
        }
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
                    .map(Self::convert_policy_entry_info)
                    .collect::<RocketMQResult<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        if entries.is_empty() {
            return Err(RocketMQError::illegal_argument("The policy entries is empty."));
        }
        Ok(Policy::of_entries(policy_type, entries))
    }

    fn convert_policy_entry(entry: &PolicyEntry) -> PolicyEntryInfo {
        PolicyEntryInfo {
            resource: entry.resource().resource_key().map(CheetahString::from_string),
            actions: entry
                .to_actions_str()
                .map(|actions| CheetahString::from_string(actions.join(","))),
            source_ips: entry.environment().map(|environment| {
                environment
                    .source_ips()
                    .iter()
                    .cloned()
                    .map(CheetahString::from_string)
                    .collect()
            }),
            decision: Some(CheetahString::from_static_str(entry.decision().name())),
        }
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
            .split(',')
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
    use rocketmq_auth::authentication::enums::subject_type::SubjectType;
    use rocketmq_auth::authorization::enums::decision::Decision;
    use rocketmq_auth::authorization::enums::policy_type::PolicyType;
    use rocketmq_auth::authorization::model::environment::Environment;
    use rocketmq_auth::authorization::model::policy::Policy;
    use rocketmq_auth::authorization::model::policy_entry::PolicyEntry;
    use rocketmq_auth::authorization::model::resource::Resource;
    use rocketmq_common::common::action::Action;

    use super::*;

    #[test]
    fn convert_acl_preserves_subject_and_policy_fields() {
        let acl = Acl::of(
            "alice",
            SubjectType::User,
            Policy::of_entries(
                rocketmq_auth::authorization::enums::policy_type::PolicyType::Custom,
                vec![PolicyEntry::of(
                    Resource::of_topic("topic-a"),
                    vec![Action::Pub, Action::Sub],
                    Environment::of("127.0.0.1"),
                    Decision::Allow,
                )],
            ),
        );

        let info = AclConverter::convert_acl(&acl);
        assert_eq!(info.subject, Some(CheetahString::from_static_str("User:alice")));

        let policy = info.policies.unwrap().pop().unwrap();
        assert_eq!(policy.policy_type, Some(CheetahString::from_static_str("Custom")));

        let entry = policy.entries.unwrap().pop().unwrap();
        assert_eq!(entry.resource, Some(CheetahString::from_static_str("Topic:topic-a")));
        assert_eq!(entry.actions, Some(CheetahString::from_static_str("Pub,Sub")));
        assert_eq!(
            entry.source_ips.unwrap(),
            vec![CheetahString::from_static_str("127.0.0.1")]
        );
        assert_eq!(entry.decision, Some(CheetahString::from_static_str("Allow")));
    }

    #[test]
    fn convert_acl_info_uses_fallback_subject_and_preserves_entries() {
        let info = AclInfo {
            subject: None,
            policies: Some(vec![PolicyInfo {
                policy_type: Some(CheetahString::from_static_str("Custom")),
                entries: Some(vec![PolicyEntryInfo {
                    resource: Some(CheetahString::from_static_str("Topic:topic-a")),
                    actions: Some(CheetahString::from_static_str("Pub,Sub")),
                    source_ips: Some(vec![CheetahString::from_static_str("127.0.0.1")]),
                    decision: Some(CheetahString::from_static_str("Allow")),
                }]),
            }]),
        };

        let acl = AclConverter::convert_acl_info(&info, "User:alice").expect("convert acl info");
        assert_eq!(acl.subject_key(), "User:alice");
        assert_eq!(acl.policies().len(), 1);
        assert_eq!(acl.policies()[0].policy_type(), PolicyType::Custom);
        assert_eq!(
            acl.policies()[0].entries()[0].resource().resource_key().as_deref(),
            Some("Topic:topic-a")
        );
        assert_eq!(
            acl.policies()[0].entries()[0].actions(),
            &vec![Action::Pub, Action::Sub]
        );
    }
}
