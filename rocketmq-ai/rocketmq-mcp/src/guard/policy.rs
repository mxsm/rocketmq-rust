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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;

use serde::Deserialize;

use crate::guard::context::Principal;
use crate::guard::GuardRejection;
use crate::guard::RiskLevel;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionConfig {
    #[serde(default)]
    pub roles: BTreeMap<String, PermissionRole>,
}

const WILDCARD: &str = "*";
const RESOURCE_READ: &str = "resource:read";
/// Rules naming the overview Tool also grant, and deny, the generic `resource:read` operation.
const RESOURCE_READ_BACKING_TOOL: &str = "rocketmq_get_cluster_overview";

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PermissionRole {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub allowed_clusters: Vec<String>,
    /// Tool names this role grants; `*` grants every Tool.
    #[serde(default)]
    pub allow_tools: Vec<String>,
    /// Tool names this role denies; `*` denies every Tool that no role grants by name.
    ///
    /// Rules from all of a principal's roles, including included roles, are combined. The more
    /// specific rule wins and a denial wins between equally specific rules: a denial by name
    /// beats a grant by name, which beats `*` in `deny_tools`, which beats `*` in `allow_tools`.
    #[serde(default)]
    pub deny_tools: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PolicyEngine {
    roles: BTreeMap<String, PermissionRole>,
}

impl PolicyEngine {
    pub fn load(path: &Path) -> crate::McpResult<Self> {
        let config = config::Config::builder().add_source(config::File::from(path)).build()?;
        let permissions = config.try_deserialize::<PermissionConfig>()?;
        if permissions.roles.is_empty() {
            return Err(crate::McpError::invalid_config(
                "permissions configuration must define at least one role".to_string(),
            ));
        }
        Ok(Self {
            roles: permissions.roles,
        })
    }

    pub fn authorize_tool(
        &self,
        principal: &Principal,
        tool_name: &str,
        cluster: Option<&str>,
        risk_level: RiskLevel,
    ) -> Result<(), GuardRejection> {
        self.require_scope(principal, risk_level)?;
        self.authorize(principal, tool_name, cluster)
    }

    pub fn authorize_resource(&self, principal: &Principal, cluster: &str) -> Result<(), GuardRejection> {
        self.require_scope(principal, RiskLevel::ReadOnly)?;
        self.authorize(principal, RESOURCE_READ, Some(cluster))
    }

    pub fn authorize_system_resource(&self, principal: &Principal) -> Result<(), GuardRejection> {
        self.require_scope(principal, RiskLevel::Diagnose)?;
        if principal
            .roles
            .iter()
            .any(|role| self.collect_role(role, &mut BTreeSet::new()).is_ok())
        {
            Ok(())
        } else {
            Err(GuardRejection::PermissionDenied)
        }
    }

    pub fn allows_tool(&self, principal: &Principal, tool_name: &str, risk_level: RiskLevel) -> bool {
        self.authorize_tool(principal, tool_name, None, risk_level).is_ok()
    }

    pub fn allows_resources(&self, principal: &Principal) -> bool {
        self.require_scope(principal, RiskLevel::ReadOnly).is_ok()
            && principal
                .roles
                .iter()
                .any(|role| self.collect_role(role, &mut BTreeSet::new()).is_ok())
    }

    pub fn allows_system_resources(&self, principal: &Principal) -> bool {
        self.authorize_system_resource(principal).is_ok()
    }

    fn authorize(&self, principal: &Principal, operation: &str, cluster: Option<&str>) -> Result<(), GuardRejection> {
        let mut rules = ToolRuleMatches::default();
        let mut allowed_clusters = BTreeSet::new();
        for role in &principal.roles {
            let mut visited = BTreeSet::new();
            for role_name in self.collect_role(role, &mut visited)? {
                let Some(role) = self.roles.get(&role_name) else {
                    continue;
                };
                rules.record(role, operation);
                allowed_clusters.extend(role.allowed_clusters.iter().cloned());
            }
        }

        if !rules.allows() {
            return Err(GuardRejection::PermissionDenied);
        }
        if let Some(cluster) = cluster {
            if !matches_cluster(&allowed_clusters, cluster)
                || principal
                    .allowed_clusters
                    .as_ref()
                    .is_some_and(|clusters| !matches_cluster(clusters, cluster))
            {
                return Err(GuardRejection::ClusterNotAllowed);
            }
        }
        Ok(())
    }

    fn require_scope(&self, principal: &Principal, risk_level: RiskLevel) -> Result<(), GuardRejection> {
        let required_scope = match risk_level {
            RiskLevel::ReadOnly => "rocketmq:read",
            RiskLevel::Diagnose => "rocketmq:diagnose",
            RiskLevel::Plan => "rocketmq:plan",
            RiskLevel::Destructive => "rocketmq:write",
        };
        if principal.scopes.contains(required_scope) {
            return Ok(());
        }
        Err(GuardRejection::UnauthorizedScope)
    }

    fn collect_role(&self, role_name: &str, visited: &mut BTreeSet<String>) -> Result<Vec<String>, GuardRejection> {
        if !visited.insert(role_name.to_string()) {
            return Err(GuardRejection::InvalidArgument);
        }
        let role = self.roles.get(role_name).ok_or(GuardRejection::PermissionDenied)?;
        let mut resolved = vec![role_name.to_string()];
        for included in &role.include {
            resolved.extend(self.collect_role(included, visited)?);
        }
        visited.remove(role_name);
        Ok(resolved)
    }
}

/// Which rules, across all of a principal's roles, match one operation.
#[derive(Debug, Clone, Copy, Default)]
struct ToolRuleMatches {
    exact_allow: bool,
    wildcard_allow: bool,
    exact_deny: bool,
    wildcard_deny: bool,
}

impl ToolRuleMatches {
    fn record(&mut self, role: &PermissionRole, operation: &str) {
        self.exact_allow |= names_operation(&role.allow_tools, operation);
        self.wildcard_allow |= role.allow_tools.iter().any(|pattern| pattern == WILDCARD);
        self.exact_deny |= names_operation(&role.deny_tools, operation);
        self.wildcard_deny |= role.deny_tools.iter().any(|pattern| pattern == WILDCARD);
    }

    /// The more specific rule wins and a denial wins between equally specific rules.
    ///
    /// Roles that grant Tools by name and add `deny_tools = ["*"]` therefore keep exactly
    /// their named Tools, while a denial by name removes a Tool from a `*` grant.
    fn allows(self) -> bool {
        if self.exact_deny {
            false
        } else if self.exact_allow {
            true
        } else {
            self.wildcard_allow && !self.wildcard_deny
        }
    }
}

fn names_operation(patterns: &[String], operation: &str) -> bool {
    patterns
        .iter()
        .any(|pattern| pattern == operation || (operation == RESOURCE_READ && pattern == RESOURCE_READ_BACKING_TOOL))
}

fn matches_cluster(clusters: &BTreeSet<String>, cluster: &str) -> bool {
    clusters.contains(WILDCARD) || clusters.contains(cluster)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::catalog::ToolId;

    #[test]
    fn policy_combines_roles_scopes_and_cluster_allow_lists() {
        let policy = PolicyEngine {
            roles: BTreeMap::from([(
                "diagnose".to_string(),
                PermissionRole {
                    allowed_clusters: vec!["cluster-a".to_string()],
                    allow_tools: vec!["rocketmq_diagnose_consumer_lag".to_string()],
                    ..PermissionRole::default()
                },
            )]),
        };
        let principal = Principal {
            id: "sre@example.test".to_string(),
            tenant: None,
            roles: ["diagnose".to_string()].into_iter().collect(),
            scopes: ["rocketmq:diagnose".to_string()].into_iter().collect(),
            allowed_clusters: Some(["cluster-a".to_string()].into_iter().collect()),
        };

        assert!(policy
            .authorize_tool(
                &principal,
                "rocketmq_diagnose_consumer_lag",
                Some("cluster-a"),
                RiskLevel::Diagnose,
            )
            .is_ok());
        assert!(policy
            .authorize_tool(
                &principal,
                "rocketmq_diagnose_consumer_lag",
                Some("cluster-b"),
                RiskLevel::Diagnose,
            )
            .is_err());
    }

    // Tool names come from the catalog, so the tests cannot drift from the registered Tools.
    fn list_topics() -> &'static str {
        ToolId::ListTopics.descriptor().name
    }

    fn message_metadata() -> &'static str {
        ToolId::GetMessageMetadata.descriptor().name
    }

    fn diagnose_lag() -> &'static str {
        ToolId::DiagnoseConsumerLag.descriptor().name
    }

    fn role(include: &[&str], allow_tools: &[&str], deny_tools: &[&str]) -> PermissionRole {
        let owned = |values: &[&str]| values.iter().map(ToString::to_string).collect::<Vec<_>>();
        PermissionRole {
            include: owned(include),
            allowed_clusters: vec![WILDCARD.to_string()],
            allow_tools: owned(allow_tools),
            deny_tools: owned(deny_tools),
        }
    }

    fn policy(roles: &[(&str, PermissionRole)]) -> PolicyEngine {
        PolicyEngine {
            roles: roles
                .iter()
                .map(|(name, role)| (name.to_string(), role.clone()))
                .collect(),
        }
    }

    /// A principal holding every scope, so only the role rules decide the outcome.
    fn principal(roles: &[&str]) -> Principal {
        Principal {
            id: "sre@example.test".to_string(),
            tenant: None,
            roles: roles.iter().map(ToString::to_string).collect(),
            scopes: ["rocketmq:read", "rocketmq:diagnose", "rocketmq:plan"]
                .into_iter()
                .map(ToString::to_string)
                .collect(),
            allowed_clusters: None,
        }
    }

    fn allows(policy: &PolicyEngine, principal: &Principal, tool: &str) -> bool {
        policy
            .authorize_tool(principal, tool, Some("cluster-a"), RiskLevel::ReadOnly)
            .is_ok()
    }

    #[test]
    fn misspelled_permission_keys_are_rejected_instead_of_being_ignored() {
        let directory = tempfile::tempdir().unwrap();
        let load = |contents: &str| {
            let path = directory.path().join("permissions.toml");
            std::fs::write(&path, contents).unwrap();
            PolicyEngine::load(&path)
        };

        // A misspelled `deny_tools` used to leave the role with every Tool `*` grants.
        let role = load("[roles.ops]\nallow_tools = [\"*\"]\ndeny_tool = [\"rocketmq_get_message_metadata\"]\n")
            .expect_err("a misspelled role key must be rejected");
        assert_eq!(
            role.to_string(),
            "MCP configuration is invalid: unknown key `deny_tool` in `roles.ops`"
        );

        let top_level = load("[role.ops]\nallow_tools = [\"*\"]\n").expect_err("an unknown table must be rejected");
        assert_eq!(
            top_level.to_string(),
            "MCP configuration is invalid: unknown key `role`"
        );
    }

    #[test]
    fn example_permissions_keep_their_current_behavior() {
        let policy = PolicyEngine::load(
            &Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("conf")
                .join("permissions.example.toml"),
        )
        .unwrap();

        // `read_only` grants Tools by name next to `deny_tools = ["*"]`, and `diagnose` includes it.
        let read_only = principal(&["read_only"]);
        assert!(allows(&policy, &read_only, list_topics()));
        assert!(allows(&policy, &read_only, message_metadata()));
        assert!(!allows(&policy, &read_only, diagnose_lag()));

        let diagnose = principal(&["diagnose"]);
        assert!(allows(&policy, &diagnose, list_topics()));
        assert!(allows(&policy, &diagnose, diagnose_lag()));
        assert!(!allows(&policy, &diagnose, "rocketmq_plan_create_topic"));
        assert!(allows(&policy, &principal(&["operator"]), "rocketmq_plan_create_topic"));
    }

    #[test]
    fn exact_deny_overrides_wildcard_allow() {
        let policy = policy(&[("ops", role(&[], &[WILDCARD], &[message_metadata()]))]);
        let principal = principal(&["ops"]);

        assert!(!allows(&policy, &principal, message_metadata()));
        assert!(allows(&policy, &principal, list_topics()));
    }

    #[test]
    fn exact_deny_overrides_exact_allow() {
        let policy = policy(&[(
            "ops",
            role(&[], &[list_topics(), message_metadata()], &[message_metadata()]),
        )]);
        let principal = principal(&["ops"]);

        assert!(!allows(&policy, &principal, message_metadata()));
        assert!(allows(&policy, &principal, list_topics()));
    }

    #[test]
    fn wildcard_deny_overrides_wildcard_allow_but_not_a_grant_by_name() {
        let policy = policy(&[("ops", role(&[], &[WILDCARD, list_topics()], &[WILDCARD]))]);
        let principal = principal(&["ops"]);

        assert!(allows(&policy, &principal, list_topics()));
        assert!(!allows(&policy, &principal, message_metadata()));
    }

    #[test]
    fn deny_from_an_included_or_sibling_role_applies() {
        let policy = policy(&[
            ("base", role(&[], &[], &[message_metadata()])),
            ("ops", role(&["base"], &[WILDCARD], &[])),
            ("reader", role(&[], &[WILDCARD], &[])),
        ]);

        assert!(!allows(&policy, &principal(&["ops"]), message_metadata()));
        assert!(allows(&policy, &principal(&["ops"]), list_topics()));
        assert!(allows(&policy, &principal(&["reader"]), message_metadata()));
        assert!(!allows(&policy, &principal(&["reader", "base"]), message_metadata()));
    }

    #[test]
    fn denied_tool_is_hidden_from_discovery() {
        let policy = policy(&[("ops", role(&[], &[WILDCARD], &[message_metadata()]))]);
        let principal = principal(&["ops"]);

        assert!(!policy.allows_tool(&principal, message_metadata(), RiskLevel::ReadOnly));
        assert!(policy.allows_tool(&principal, list_topics(), RiskLevel::ReadOnly));
    }

    #[test]
    fn denying_the_overview_tool_denies_generic_resource_reads() {
        let granted = policy(&[("ops", role(&[], &[RESOURCE_READ_BACKING_TOOL], &[]))]);
        assert!(granted.authorize_resource(&principal(&["ops"]), "cluster-a").is_ok());

        let denied = policy(&[("ops", role(&[], &[WILDCARD], &[RESOURCE_READ_BACKING_TOOL]))]);
        assert!(matches!(
            denied.authorize_resource(&principal(&["ops"]), "cluster-a"),
            Err(GuardRejection::PermissionDenied)
        ));
    }
}
