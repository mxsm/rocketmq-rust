use cheetah_string::CheetahString;
use rocketmq_auth::authorization::model::acl::Acl;
use rocketmq_auth::authorization::model::policy::Policy;
use rocketmq_auth::authorization::model::policy_entry::PolicyEntry;
use rocketmq_remoting::protocol::body::acl_info::AclInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyEntryInfo;
use rocketmq_remoting::protocol::body::acl_info::PolicyInfo;

pub struct AclConverter;

impl AclConverter {
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
}

#[cfg(test)]
mod tests {
    use rocketmq_auth::authentication::enums::subject_type::SubjectType;
    use rocketmq_auth::authorization::enums::decision::Decision;
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
}
