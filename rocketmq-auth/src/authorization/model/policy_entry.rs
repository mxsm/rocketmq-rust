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
use super::resource::Resource;
use crate::authorization::enums::decision::Decision;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyEntry {
    resource: Resource,
    actions: Vec<Action>,
    environment: Option<Environment>,
    decision: Decision,
}

impl PolicyEntry {
    pub fn of(resource: Resource, actions: Vec<Action>, environment: Option<Environment>, decision: Decision) -> Self {
        Self {
            resource,
            actions,
            environment,
            decision,
        }
    }

    pub fn update_entry(&mut self, actions: Vec<Action>, environment: Option<Environment>, decision: Decision) {
        self.actions = actions;
        self.environment = environment;
        self.decision = decision;
    }

    pub fn is_match_resource(&self, resource: &Resource) -> bool {
        self.resource.is_match(resource)
    }

    /// Check whether the provided actions match this policy's actions.
    /// Returns false if this policy has no actions configured.
    pub fn is_match_action(&self, actions: &[Action]) -> bool {
        if self.actions.is_empty() {
            return false;
        }
        if actions.contains(&Action::Any) {
            return true;
        }
        // any action in `actions` present in self.actions OR self.actions contains ALL
        let contains_all = self.actions.contains(&Action::All);
        actions
            .iter()
            .any(|a| self.actions.iter().any(|sa| sa == a) || contains_all)
    }

    pub fn is_match_environment(&self, environment: &Environment) -> bool {
        match &self.environment {
            None => true,
            Some(env) => env.is_match(environment),
        }
    }

    pub fn to_resource_str(&self) -> Option<String> {
        self.resource.resource_key()
    }

    pub fn to_actions_str(&self) -> Option<Vec<String>> {
        if self.actions.is_empty() {
            return None;
        }
        Some(
            self.actions
                .iter()
                .map(|a| a.name().to_string())
                .collect::<Vec<String>>(),
        )
    }

    // getters/setters
    pub fn resource(&self) -> &Resource {
        &self.resource
    }

    pub fn set_resource(&mut self, r: Resource) {
        self.resource = r;
    }

    pub fn actions(&self) -> &Vec<Action> {
        &self.actions
    }

    pub fn set_actions(&mut self, a: Vec<Action>) {
        self.actions = a;
    }

    pub fn environment(&self) -> Option<&Environment> {
        self.environment.as_ref()
    }

    pub fn set_environment(&mut self, e: Option<Environment>) {
        self.environment = e;
    }

    pub fn decision(&self) -> Decision {
        self.decision
    }

    pub fn set_decision(&mut self, d: Decision) {
        self.decision = d;
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::action::Action;
    use rocketmq_common::common::resource::resource_type::ResourceType;

    use super::*;
    use crate::authorization::enums::decision::Decision;
    use crate::authorization::model::resource::Resource;

    #[test]
    fn test_of_and_to_strings() {
        let r = Resource::of(
            ResourceType::Topic,
            Some("foo".to_string()),
            rocketmq_common::common::resource::resource_pattern::ResourcePattern::Literal,
        );
        let p = PolicyEntry::of(r.clone(), vec![Action::Pub, Action::Sub], None, Decision::Allow);
        assert_eq!(p.to_resource_str(), Some("Topic:foo".to_string()));
        assert_eq!(p.to_actions_str().unwrap(), vec!["Pub".to_string(), "Sub".to_string()]);
        assert_eq!(p.decision(), Decision::Allow);
    }

    #[test]
    fn test_is_match_action_any_and_all() {
        let r = Resource::of(
            ResourceType::Topic,
            Some("x".to_string()),
            rocketmq_common::common::resource::resource_pattern::ResourcePattern::Literal,
        );
        let p = PolicyEntry::of(r, vec![Action::Pub, Action::Sub], None, Decision::Allow);
        assert!(p.is_match_action(&[Action::Pub]));
        assert!(!p.is_match_action(&[Action::Create]));

        // ANY in incoming actions -> true
        assert!(p.is_match_action(&[Action::Any]));

        // ALL in policy -> matches any
        let r2 = Resource::of(
            ResourceType::Topic,
            Some("x".to_string()),
            rocketmq_common::common::resource::resource_pattern::ResourcePattern::Literal,
        );
        let p2 = PolicyEntry::of(r2, vec![Action::All], None, Decision::Allow);
        assert!(p2.is_match_action(&[Action::Create]));
    }

    #[test]
    fn test_is_match_resource_and_environment() {
        let rule = PolicyEntry::of(
            Resource::of(
                ResourceType::Topic,
                Some("pre".to_string()),
                rocketmq_common::common::resource::resource_pattern::ResourcePattern::Prefixed,
            ),
            vec![Action::Pub],
            Some(crate::authorization::model::environment::Environment::of("1.2.3.4").unwrap()),
            Decision::Allow,
        );

        let res = Resource::of(
            ResourceType::Topic,
            Some("prefix_val".to_string()),
            rocketmq_common::common::resource::resource_pattern::ResourcePattern::Literal,
        );
        assert!(rule.is_match_resource(&res));
        assert!(
            rule.is_match_environment(&crate::authorization::model::environment::Environment::of("1.2.3.4").unwrap())
        );
        assert!(
            !rule.is_match_environment(&crate::authorization::model::environment::Environment::of("8.8.8.8").unwrap())
        );
    }
}
