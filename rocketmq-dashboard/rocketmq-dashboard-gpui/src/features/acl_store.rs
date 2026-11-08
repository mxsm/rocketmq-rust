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

use super::dashboard_store::ResourceSlot;
use rocketmq_dashboard_common::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AclTab {
    #[default]
    Users,
    Policies,
}
#[derive(Clone)]
pub enum AclRow {
    User(AclUser),
    Policy(AclPolicy, usize),
}
#[derive(Default)]
pub struct AclStore {
    pub inventory: ResourceSlot<Vec<BrokerInventoryItem>>,
    pub confirmed: Option<AclScope>,
    pub draft_cluster: String,
    pub draft_broker: Option<String>,
    pub users: ResourceSlot<AclRead<AclUser>>,
    pub policies: ResourceSlot<AclRead<AclPolicy>>,
    pub tab: AclTab,
    pub users_search: String,
    pub policies_search: String,
    pub users_page: usize,
    pub policies_page: usize,
    pub visible: Vec<AclRow>,
    pub filtered_count: usize,
}
impl AclStore {
    pub fn clear_secrets(&mut self) {
        use crate::state::Loadable;
        let result = match &mut self.users.state {
            Loadable::Ready(result)
            | Loadable::Refreshing(result)
            | Loadable::Failed {
                previous: Some(result), ..
            } => Some(result),
            _ => None,
        };
        if let Some(result) = result {
            for user in &mut result.items {
                user.clear_secret();
            }
        }
        for row in &mut self.visible {
            if let AclRow::User(user) = row {
                user.clear_secret();
            }
        }
    }
    pub fn identities(&self) -> Vec<BrokerIdentity> {
        self.inventory
            .state
            .value()
            .into_iter()
            .flatten()
            .map(|item| item.identity.clone())
            .collect()
    }
    pub fn invalidate_scope(&mut self) {
        self.confirmed = None;
        self.users.clear();
        self.policies.clear();
        self.visible.clear();
        self.users_page = 0;
        self.policies_page = 0;
        self.filtered_count = 0;
    }
    pub fn confirm(&mut self) -> Result<(), AclValidationError> {
        let scope = AclScope::confirm(&self.identities(), &self.draft_cluster, self.draft_broker.as_deref())?;
        self.invalidate_scope();
        self.confirmed = Some(scope);
        Ok(())
    }
    pub fn change_tab(&mut self, tab: AclTab) {
        // Backend-returned user passwords never survive a tab change, including an in-flight read.
        self.users.clear();
        self.policies.invalidate();
        self.visible.clear();
        self.tab = tab;
        self.rebuild();
    }
    pub fn rebuild(&mut self) {
        let mut rows = match self.tab {
            AclTab::Users => self
                .users
                .state
                .value()
                .map(|result| {
                    result
                        .items
                        .iter()
                        .filter(|user| {
                            [
                                user.username.as_str(),
                                user.user_type.as_deref().unwrap_or(""),
                                user.status.as_deref().unwrap_or(""),
                                &user.target.broker_name,
                            ]
                            .iter()
                            .any(|value| value.to_lowercase().contains(&self.users_search))
                        })
                        .cloned()
                        .map(AclRow::User)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            AclTab::Policies => self
                .policies
                .state
                .value()
                .map(|result| {
                    result
                        .items
                        .iter()
                        .flat_map(|policy| (0..policy.entries.len().max(1)).map(move |index| (policy, index)))
                        .filter(|(policy, index)| {
                            let entry = policy.entries.get(*index);
                            [
                                policy.subject.clone(),
                                policy.policy_type.clone(),
                                entry.map_or(String::new(), |entry| entry.resource.clone()),
                                entry.map_or(String::new(), |entry| entry.actions.join(",")),
                                entry.map_or(String::new(), |entry| entry.source_ips.join(",")),
                                entry.map_or(String::new(), |entry| entry.decision.clone()),
                            ]
                            .iter()
                            .any(|value| value.to_lowercase().contains(&self.policies_search))
                        })
                        .map(|(policy, index)| AclRow::Policy(policy.clone(), index))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        };
        rows.sort_by_key(row_key);
        self.filtered_count = rows.len();
        let page = match self.tab {
            AclTab::Users => &mut self.users_page,
            AclTab::Policies => &mut self.policies_page,
        };
        *page = (*page).min(rows.len().saturating_sub(1) / ACL_PAGE_SIZE);
        self.visible = rows
            .into_iter()
            .skip(*page * ACL_PAGE_SIZE)
            .take(ACL_PAGE_SIZE)
            .collect();
    }
    pub fn search(&mut self, value: &str) {
        match self.tab {
            AclTab::Users => {
                self.users_search = value.trim().to_lowercase();
                self.users_page = 0;
            }
            AclTab::Policies => {
                self.policies_search = value.trim().to_lowercase();
                self.policies_page = 0;
            }
        }
        self.rebuild();
    }
    pub fn page(&self) -> usize {
        match self.tab {
            AclTab::Users => self.users_page,
            AclTab::Policies => self.policies_page,
        }
    }
    pub fn move_page(&mut self, next: bool) {
        let page = match self.tab {
            AclTab::Users => &mut self.users_page,
            AclTab::Policies => &mut self.policies_page,
        };
        *page = if next {
            page.saturating_add(1)
        } else {
            page.saturating_sub(1)
        };
        self.rebuild();
    }
}
fn row_key(row: &AclRow) -> String {
    match row {
        AclRow::User(user) => format!("{}|{}", user.username, user.target.address),
        AclRow::Policy(policy, index) => format!(
            "{}|{}|{}|{}",
            policy.subject, policy.policy_type, policy.target.address, index
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acl_scope_edit_clears_both_tabs_secrets_and_pending_epochs() {
        let mut store = AclStore::default();
        let old = store.users.begin(4).unwrap();
        store.policies_search = "topic".into();
        store.invalidate_scope();
        assert!(!store.users.finish(old, 4, Ok(Some(AclRead::default()))));
        assert!(store.users.state.value().is_none());
        assert!(store.confirmed.is_none());
        assert_eq!(store.policies_search, "topic");
        assert!(store.confirm().is_err());
    }
}
