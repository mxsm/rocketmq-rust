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

//! Explicit ACL scope and structured, transient commands. Secrets are never serializable.

use std::fmt;

use crate::BrokerIdentity;

pub const ACL_TARGET_LIMIT: usize = 64;
pub const ACL_ROW_LIMIT: usize = 2_000;
pub const ACL_PAGE_SIZE: usize = 10;
pub const ACL_ACTIONS: &[&str] = &[
    "All",
    "Pub",
    "Sub",
    "Create",
    "Update",
    "Delete",
    "Get",
    "List",
    "Publish",
    "Subscribe",
    "Describe",
    "Manage",
];
pub const ACL_POLICY_TYPES: &[&str] = &["Custom", "Default"];
pub const ACL_DECISIONS: &[&str] = &["Allow", "Deny"];
pub const ACL_USER_TYPES: &[&str] = &["Normal", "Super"];
pub const ACL_USER_STATUSES: &[&str] = &["enable", "disable"];

/// A frozen selection obtained by confirming real inventory. Empty Broker means the whole Cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclScope {
    cluster: String,
    broker: Option<String>,
    targets: Vec<BrokerIdentity>,
}
impl AclScope {
    /// # Errors
    /// Rejects an empty, missing, ambiguous, or excessively broad selection.
    pub fn confirm(
        inventory: &[BrokerIdentity],
        cluster: &str,
        broker: Option<&str>,
    ) -> Result<Self, AclValidationError> {
        let cluster = cluster.trim();
        let broker = broker.map(str::trim).filter(|value| !value.is_empty());
        let mut targets = inventory
            .iter()
            .filter(|target| target.cluster == cluster && broker.is_none_or(|name| target.broker_name == name))
            .cloned()
            .collect::<Vec<_>>();
        targets.sort();
        targets.dedup();
        if cluster.is_empty()
            || targets.is_empty()
            || targets.len() > ACL_TARGET_LIMIT
            || targets.iter().any(|target| target.address.is_empty())
            || targets.iter().enumerate().any(|(index, target)| {
                targets[..index]
                    .iter()
                    .any(|previous| previous.address == target.address)
            })
        {
            return Err(AclValidationError::Scope);
        }
        Ok(Self {
            cluster: cluster.into(),
            broker: broker.map(str::to_owned),
            targets,
        })
    }
    pub fn cluster(&self) -> &str {
        &self.cluster
    }
    pub fn broker(&self) -> Option<&str> {
        self.broker.as_deref()
    }
    pub fn targets(&self) -> &[BrokerIdentity] {
        &self.targets
    }
    /// # Errors
    /// Rejects inventory drift rather than expanding an already confirmed selection.
    pub fn verify(&self, inventory: &[BrokerIdentity]) -> Result<(), AclValidationError> {
        if Self::confirm(inventory, &self.cluster, self.broker.as_deref())? == *self {
            Ok(())
        } else {
            Err(AclValidationError::Scope)
        }
    }
}

/// Transient secret with redacted formatting and no Serde implementation.
#[derive(Clone, PartialEq, Eq)]
pub struct AclSecret(String);
impl AclSecret {
    pub fn new(value: String) -> Self {
        Self(value)
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
    pub fn clear(&mut self) {
        self.0.clear();
    }
}
impl fmt::Debug for AclSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}
impl Drop for AclSecret {
    fn drop(&mut self) {
        self.clear();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub target: BrokerIdentity,
    pub username: String,
    pub user_type: Option<String>,
    pub status: Option<String>,
    pub password: Option<AclSecret>,
    pub read_only: bool,
}
impl AclUser {
    pub fn clear_secret(&mut self) {
        self.password = None;
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclPolicyEntry {
    pub resource: String,
    pub actions: Vec<String>,
    pub source_ips: Vec<String>,
    pub decision: String,
}
impl AclPolicyEntry {
    /// # Errors
    /// Rejects noncanonical resources, unsupported enum values, and invalid IP patterns.
    pub fn validate(&self) -> Result<(), AclValidationError> {
        if !canonical_resource(&self.resource)
            || self.actions.is_empty()
            || self.actions.iter().any(|value| !ACL_ACTIONS.contains(&value.as_str()))
            || !ACL_DECISIONS.contains(&self.decision.as_str())
            || self.source_ips.len() > 64
        {
            return Err(AclValidationError::Policy);
        }
        for value in &self.source_ips {
            if value == "*" {
                continue;
            }
            if value.contains('*')
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() || b".:*".contains(&byte))
            {
                continue;
            }
            if let Some((start, end)) = value.split_once('-') {
                let start: std::net::IpAddr = start.trim().parse().map_err(|_| AclValidationError::Policy)?;
                let end: std::net::IpAddr = end.trim().parse().map_err(|_| AclValidationError::Policy)?;
                if start.is_ipv4() != end.is_ipv4() || start > end {
                    return Err(AclValidationError::Policy);
                }
                continue;
            }
            let (ip, prefix) = value
                .split_once('/')
                .map_or((value.as_str(), None), |(ip, prefix)| (ip, Some(prefix)));
            let address: std::net::IpAddr = ip.parse().map_err(|_| AclValidationError::Policy)?;
            if let Some(prefix) = prefix {
                let prefix: u8 = prefix.parse().map_err(|_| AclValidationError::Policy)?;
                if prefix > if address.is_ipv4() { 32 } else { 128 } {
                    return Err(AclValidationError::Policy);
                }
            }
        }
        Ok(())
    }
}

// The Broker accepts aliases and trims identity components before upserting. Require its
// canonical spelling so preflight, audit, and the exact wire operation identify the same entry.
fn canonical_subject(subject: &str) -> bool {
    subject
        .strip_prefix("User:")
        .is_some_and(|name| !name.is_empty() && name.trim() == name && !name.chars().any(char::is_control))
}
fn canonical_resource(resource: &str) -> bool {
    resource.len() <= 512
        && (resource == "*"
            || resource.split_once(':').is_some_and(|(kind, name)| {
                ["Topic", "Group", "Cluster"].contains(&kind)
                    && !name.is_empty()
                    && name.trim() == name
                    && !name.ends_with("**")
                    && !name.chars().any(char::is_control)
            }))
}

/// Unknown response fields/enums make the whole returned policy read-only, preserving Broker data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclPolicy {
    pub target: BrokerIdentity,
    pub subject: String,
    pub policy_type: String,
    pub entries: Vec<AclPolicyEntry>,
    pub read_only: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclFailure {
    AccessDenied,
    Unavailable,
    Invalid,
    Unknown,
}
impl AclFailure {
    pub const fn summary(self) -> &'static str {
        match self {
            Self::AccessDenied => "Access denied by the Broker.",
            Self::Unavailable => "ACL capability or Broker is unavailable.",
            Self::Invalid => "The selected record is absent, changed, or unsupported; reload before editing.",
            Self::Unknown => "The call was not confirmed; verify Broker state before another attempt.",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclTargetFailure {
    pub target: BrokerIdentity,
    pub failure: AclFailure,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclRead<T> {
    pub items: Vec<T>,
    pub failures: Vec<AclTargetFailure>,
    pub truncated: bool,
}
impl<T> Default for AclRead<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            failures: Vec::new(),
            truncated: false,
        }
    }
}

/// Password absence on edit explicitly means unchanged; creation requires an explicit nonempty input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AclChange {
    SaveUser {
        create: bool,
        username: String,
        password: Option<AclSecret>,
        user_type: String,
        status: String,
    },
    DeleteUser {
        username: String,
    },
    SavePolicy {
        create: bool,
        subject: String,
        policy_type: String,
        entry: AclPolicyEntry,
    },
    DeletePolicyEntry {
        subject: String,
        policy_type: String,
        resource: String,
    },
}
impl AclChange {
    pub fn subject(&self) -> &str {
        match self {
            Self::SaveUser { username, .. } | Self::DeleteUser { username } => username,
            Self::SavePolicy { subject, .. } | Self::DeletePolicyEntry { subject, .. } => subject,
        }
    }
    /// # Errors
    /// Rejects unsupported edits before audit admission or network work.
    /// Policy subjects and resources must use canonical Broker spelling so aliases cannot bypass preflight.
    pub fn validate(&self) -> Result<(), AclValidationError> {
        let subject = self.subject();
        if subject.trim() != subject
            || subject.is_empty()
            || subject.len() > 255
            || subject.chars().any(char::is_control)
        {
            return Err(AclValidationError::Subject);
        }
        match self {
            Self::SaveUser {
                create,
                password,
                user_type,
                status,
                ..
            } => {
                if !ACL_USER_TYPES.contains(&user_type.as_str())
                    || !ACL_USER_STATUSES.contains(&status.as_str())
                    || (*create && password.is_none())
                    || password
                        .as_ref()
                        .is_some_and(|value| value.expose().is_empty() || value.expose().len() > 1024)
                {
                    return Err(AclValidationError::User);
                }
            }
            Self::SavePolicy { policy_type, entry, .. } => {
                if !canonical_subject(subject) || !ACL_POLICY_TYPES.contains(&policy_type.as_str()) {
                    return Err(AclValidationError::Policy);
                }
                entry.validate()?;
            }
            Self::DeletePolicyEntry {
                policy_type, resource, ..
            } => {
                if !canonical_subject(subject)
                    || !ACL_POLICY_TYPES.contains(&policy_type.as_str())
                    || !canonical_resource(resource)
                {
                    return Err(AclValidationError::Policy);
                }
            }
            Self::DeleteUser { .. } => {}
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclCommand {
    pub scope: AclScope,
    pub targets: Vec<BrokerIdentity>,
    pub change: AclChange,
}
impl AclCommand {
    /// # Errors
    /// Rejects commands outside the frozen confirmed scope or with duplicate targets.
    pub fn validate(&self) -> Result<(), AclValidationError> {
        self.change.validate()?;
        if self.targets.is_empty()
            || self.targets.len() > ACL_TARGET_LIMIT
            || self
                .targets
                .iter()
                .enumerate()
                .any(|(index, target)| !self.scope.targets.contains(target) || self.targets[..index].contains(target))
        {
            return Err(AclValidationError::Scope);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AclWriteState {
    Applied,
    Rejected(AclFailure),
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclWriteTarget {
    pub target: BrokerIdentity,
    pub state: AclWriteState,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclWriteResult {
    pub targets: Vec<AclWriteTarget>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AclValidationError {
    #[error("Confirm a current Cluster/Broker scope before reading or writing ACL data.")]
    Scope,
    #[error("Enter a nonempty subject or username of at most 255 bytes.")]
    Subject,
    #[error("Select a known user type/status; creation requires an explicit password.")]
    User,
    #[error(
        "Use User:name, Topic:name/Group:name/Cluster:name or *, supported actions, and valid source IP patterns."
    )]
    Policy,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acl_scope_is_exact_and_secret_debug_and_password_intent_are_safe() {
        let first = BrokerIdentity {
            cluster: "a".into(),
            broker_name: "broker".into(),
            broker_id: 0,
            address: "broker:10911".into(),
        };
        let scope = AclScope::confirm(std::slice::from_ref(&first), "a", None).unwrap();
        let mut moved = first.clone();
        moved.address = "other:10911".into();
        assert!(scope.verify(&[moved]).is_err());
        let mut duplicate_address = first.clone();
        duplicate_address.broker_id = 1;
        assert!(AclScope::confirm(&[first, duplicate_address], "a", None).is_err());
        let mut change = AclChange::SaveUser {
            create: false,
            username: "alice".into(),
            password: None,
            user_type: "Normal".into(),
            status: "enable".into(),
        };
        assert!(change.validate().is_ok());
        if let AclChange::SaveUser { create, password, .. } = &mut change {
            *create = true;
            assert!(password.is_none());
        }
        assert!(change.validate().is_err());
        if let AclChange::SaveUser { password, .. } = &mut change {
            *password = Some(AclSecret::new("private-password".into()));
        }
        assert!(change.validate().is_ok());
        assert!(!format!("{change:?}").contains("private-password"));
    }

    #[test]
    fn acl_supported_source_patterns_and_actions_do_not_make_unknown_resources_editable() {
        let mut entry = AclPolicyEntry {
            resource: "Topic:orders".into(),
            actions: vec!["Publish".into(), "Manage".into()],
            source_ips: vec!["10.0.*".into(), "127.0.0.1-127.0.0.9".into(), "2001:db8::/32".into()],
            decision: "Allow".into(),
        };
        assert!(entry.validate().is_ok());
        entry.resource = "FutureResource:orders".into();
        assert!(entry.validate().is_err());
        entry.resource = "Topic:orders".into();
        entry.source_ips = vec!["127.0.0.9-127.0.0.1".into()];
        assert!(entry.validate().is_err());
    }

    #[test]
    fn acl_policy_aliases_and_response_only_any_are_rejected_before_upsert() {
        let mut change = AclChange::SavePolicy {
            create: true,
            subject: "User:alice".into(),
            policy_type: "Custom".into(),
            entry: AclPolicyEntry {
                resource: "Topic:orders".into(),
                actions: vec!["Pub".into()],
                source_ips: vec![],
                decision: "Allow".into(),
            },
        };
        assert!(change.validate().is_ok());
        for alias in ["alice", "user:alice", "User: alice", "User:alice "] {
            if let AclChange::SavePolicy { subject, .. } = &mut change {
                *subject = alias.into();
            }
            assert!(
                change.validate().is_err(),
                "Alias {alias} must not bypass canonical preflight"
            );
        }
        for alias in ["topic:orders", "Topic:orders**", "Topic:**"] {
            if let AclChange::SavePolicy { subject, entry, .. } = &mut change {
                *subject = "User:alice".into();
                entry.resource = alias.into();
            }
            assert!(change.validate().is_err());
        }
        if let AclChange::SavePolicy { entry, .. } = &mut change {
            entry.resource = "Topic:orders".into();
            entry.actions = vec!["Any".into()];
        }
        assert!(change.validate().is_err());
    }
}
