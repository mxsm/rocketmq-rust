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

use rocketmq_common::common::action::Action;

use super::environment::Environment;
use super::policy_entry::PolicyEntry;
use super::resource::Resource;
use crate::authorization::enums::decision::Decision;
use crate::authorization::enums::policy_type::PolicyType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    policy_type: PolicyType,
    entries: Vec<PolicyEntry>,
}

impl Policy {
    pub fn of(
        resources: Vec<Resource>,
        actions: Vec<Action>,
        environment: Option<Environment>,
        decision: Decision,
    ) -> Self {
        Self::of_type(PolicyType::Custom, resources, actions, environment, decision)
    }

    pub fn of_type(
        policy_type: PolicyType,
        resources: Vec<Resource>,
        actions: Vec<Action>,
        environment: Option<Environment>,
        decision: Decision,
    ) -> Self {
        let entries = resources
            .into_iter()
            .map(|r| PolicyEntry::of(r, actions.clone(), environment.clone(), decision))
            .collect();
        Policy { policy_type, entries }
    }

    pub fn of_entries(policy_type: PolicyType, entries: Vec<PolicyEntry>) -> Self {
        Policy { policy_type, entries }
    }

    pub fn update_entry(&mut self, new_entries: Vec<PolicyEntry>) {
        if self.entries.is_empty() {
            self.entries = Vec::new();
        }
        for new_entry in new_entries {
            match self.get_entry_mut(new_entry.resource()) {
                None => self.entries.push(new_entry),
                Some(entry) => entry.update_entry(
                    new_entry.actions().clone(),
                    new_entry.environment().cloned(),
                    new_entry.decision(),
                ),
            }
        }
    }

    pub fn delete_entry(&mut self, resource: &Resource) {
        if let Some(pos) = self.entries.iter().position(|e| e.resource() == resource) {
            self.entries.remove(pos);
        }
    }

    fn get_entry(&self, resource: &Resource) -> Option<&PolicyEntry> {
        self.entries.iter().find(|e| e.resource() == resource)
    }

    fn get_entry_mut(&mut self, resource: &Resource) -> Option<&mut PolicyEntry> {
        self.entries.iter_mut().find(|e| e.resource() == resource)
    }

    // getters/setters
    pub fn policy_type(&self) -> PolicyType {
        self.policy_type
    }

    pub fn set_policy_type(&mut self, t: PolicyType) {
        self.policy_type = t;
    }

    pub fn entries(&self) -> &Vec<PolicyEntry> {
        &self.entries
    }

    pub fn set_entries(&mut self, e: Vec<PolicyEntry>) {
        self.entries = e;
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::action::Action;
    use rocketmq_common::common::resource::resource_pattern::ResourcePattern;
    use rocketmq_common::common::resource::resource_type::ResourceType;

    use super::*;
    use crate::authorization::enums::policy_type::PolicyType;

    #[test]
    fn test_of_and_update_delete() {
        let r1 = Resource::of(ResourceType::Topic, Some("t1".to_string()), ResourcePattern::Literal);
        let r2 = Resource::of(ResourceType::Topic, Some("t2".to_string()), ResourcePattern::Literal);
        let mut p = Policy::of(vec![r1.clone()], vec![Action::Pub], None, Decision::Allow);
        assert_eq!(p.entries().len(), 1);

        // update: add r2
        p.update_entry(vec![PolicyEntry::of(
            r2.clone(),
            vec![Action::Pub],
            None,
            Decision::Allow,
        )]);
        assert_eq!(p.entries().len(), 2);

        // delete r1
        p.delete_entry(&r1);
        assert_eq!(p.entries().len(), 1);
        assert!(p.entries().iter().all(|e| e.resource() != &r1));

        // set entries directly
        p.set_entries(vec![PolicyEntry::of(
            r1.clone(),
            vec![Action::Sub],
            None,
            Decision::Deny,
        )]);
        assert_eq!(p.entries().len(), 1);
        assert_eq!(p.policy_type(), PolicyType::Custom);
    }
}
