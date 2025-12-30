// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fmt;

use super::policy::Policy;
use super::resource::Resource;
use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::model::subject::Subject;
use crate::authorization::enums::policy_type::PolicyType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Acl {
    subject_key: String,
    subject_type: SubjectType,
    policies: Vec<Policy>,
}

impl Acl {
    /// Create from a subject (trait impl) and a single policy
    pub fn of_subject_and_policy<S: Subject>(subject: &S, policy: Policy) -> Self {
        Self::of_subject_and_policies(subject, vec![policy])
    }

    /// Create from a subject and a list of policies
    pub fn of_subject_and_policies<S: Subject>(subject: &S, policies: Vec<Policy>) -> Self {
        Acl {
            subject_key: subject.subject_key().to_string(),
            subject_type: subject.subject_type(),
            policies,
        }
    }

    /// Create from subject key/type and single policy
    pub fn of(subject_key: impl Into<String>, subject_type: SubjectType, policy: Policy) -> Self {
        Self::of_with_policies(subject_key, subject_type, vec![policy])
    }

    /// Create from subject key/type and policies
    pub fn of_with_policies(subject_key: impl Into<String>, subject_type: SubjectType, policies: Vec<Policy>) -> Self {
        Acl {
            subject_key: subject_key.into(),
            subject_type,
            policies,
        }
    }

    /// Update policies: if policy with same type exists -> update entries, else add it
    pub fn update_policy(&mut self, policy: Policy) {
        self.update_policies(vec![policy])
    }

    pub fn update_policies(&mut self, policies: Vec<Policy>) {
        if self.policies.is_empty() {
            self.policies = Vec::new();
        }
        for new_policy in policies {
            match self.get_policy_mut(new_policy.policy_type()) {
                None => self.policies.push(new_policy),
                Some(old_policy) => old_policy.update_entry(new_policy.entries().clone()),
            }
        }
    }

    /// Delete a resource entry from the policy of the given type; remove the policy if empty
    pub fn delete_policy(&mut self, policy_type: PolicyType, resource: &Resource) {
        if let Some(policy) = self.get_policy_mut(policy_type) {
            policy.delete_entry(resource);
            if policy.entries().is_empty() {
                // remove policy entirely
                if let Some(pos) = self.policies.iter().position(|p| p.policy_type() == policy_type) {
                    self.policies.remove(pos);
                }
            }
        }
    }

    pub fn get_policy(&self, policy_type: PolicyType) -> Option<&Policy> {
        self.policies.iter().find(|p| p.policy_type() == policy_type)
    }

    fn get_policy_mut(&mut self, policy_type: PolicyType) -> Option<&mut Policy> {
        self.policies.iter_mut().find(|p| p.policy_type() == policy_type)
    }

    // getters / setters
    pub fn subject_key(&self) -> &str {
        &self.subject_key
    }

    pub fn subject_type(&self) -> SubjectType {
        self.subject_type
    }

    pub fn policies(&self) -> &Vec<Policy> {
        &self.policies
    }

    pub fn set_policies(&mut self, p: Vec<Policy>) {
        self.policies = p;
    }

    pub fn set_subject_key(&mut self, k: impl Into<String>) {
        self.subject_key = k.into();
    }

    pub fn set_subject_type(&mut self, t: SubjectType) {
        self.subject_type = t;
    }
}

impl fmt::Display for Acl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Acl{{subject_key={}, subject_type={}, policies_len={}}}",
            self.subject_key,
            self.subject_type.name(),
            self.policies.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::action::Action;
    use rocketmq_common::common::resource::resource_pattern::ResourcePattern;
    use rocketmq_common::common::resource::resource_type::ResourceType;

    use super::*;
    use crate::authentication::model::user::User;
    use crate::authorization::enums::decision::Decision;
    use crate::authorization::model::policy::Policy;

    #[test]
    fn test_of_and_update_delete() {
        let user = User::of("alice");
        let r1 = Resource::of(ResourceType::Topic, Some("t1".to_string()), ResourcePattern::Literal);
        let r2 = Resource::of(ResourceType::Topic, Some("t2".to_string()), ResourcePattern::Literal);
        let mut acl = Acl::of_subject_and_policies(
            &user,
            vec![Policy::of(vec![r1.clone()], vec![Action::Pub], None, Decision::Allow)],
        );
        assert_eq!(acl.policies().len(), 1);

        // update: add resource r2 to the existing CUSTOM policy (entries should increase)
        acl.update_policy(Policy::of(vec![r2.clone()], vec![Action::Pub], None, Decision::Allow));
        assert_eq!(acl.policies().len(), 1);
        assert_eq!(acl.policies().first().unwrap().entries().len(), 2);

        // delete r1 from custom policy (entries should decrease)
        acl.delete_policy(PolicyType::Custom, &r1);
        assert_eq!(acl.policies().len(), 1);
        assert_eq!(acl.policies().first().unwrap().entries().len(), 1);
    }
}
