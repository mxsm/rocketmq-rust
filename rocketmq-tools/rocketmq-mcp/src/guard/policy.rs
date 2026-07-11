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
use crate::guard::GuardError;
use crate::guard::RiskLevel;

#[derive(Debug, Clone, Deserialize)]
pub struct PermissionConfig {
    #[serde(default)]
    pub roles: BTreeMap<String, PermissionRole>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PermissionRole {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub allowed_clusters: Vec<String>,
    #[serde(default)]
    pub allow_tools: Vec<String>,
    #[serde(default)]
    pub deny_tools: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PolicyEngine {
    roles: BTreeMap<String, PermissionRole>,
}

impl PolicyEngine {
    pub fn load(path: &Path) -> Result<Self, GuardError> {
        let config = config::Config::builder()
            .add_source(config::File::from(path))
            .build()
            .map_err(|error| GuardError::InvalidArgument(format!("failed to load permissions: {error}")))?;
        let permissions = config
            .try_deserialize::<PermissionConfig>()
            .map_err(|error| GuardError::InvalidArgument(format!("invalid permissions configuration: {error}")))?;
        if permissions.roles.is_empty() {
            return Err(GuardError::InvalidArgument(
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
    ) -> Result<(), GuardError> {
        self.require_scope(principal, risk_level)?;
        self.authorize(principal, tool_name, cluster)
    }

    pub fn authorize_resource(&self, principal: &Principal, cluster: &str) -> Result<(), GuardError> {
        self.require_scope(principal, RiskLevel::ReadOnly)?;
        self.authorize(principal, "resource:read", Some(cluster))
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

    fn authorize(&self, principal: &Principal, operation: &str, cluster: Option<&str>) -> Result<(), GuardError> {
        let mut allowed = false;
        let mut denied = false;
        let mut allowed_clusters = BTreeSet::new();
        for role in &principal.roles {
            let mut visited = BTreeSet::new();
            for role_name in self.collect_role(role, &mut visited)? {
                let Some(role) = self.roles.get(&role_name) else {
                    continue;
                };
                let explicitly_allowed = matches_pattern(&role.allow_tools, operation)
                    || (operation == "resource:read"
                        && matches_pattern(&role.allow_tools, "rocketmq_get_cluster_overview"));
                denied |= matches_pattern(&role.deny_tools, operation) && !explicitly_allowed;
                if explicitly_allowed {
                    allowed = true;
                }
                allowed_clusters.extend(role.allowed_clusters.iter().cloned());
            }
        }

        if !allowed {
            let reason = if denied { " is denied" } else { " is not authorized" };
            return Err(GuardError::PermissionDenied(format!(
                "principal `{}`{reason} for `{operation}`",
                principal.id,
            )));
        }
        if let Some(cluster) = cluster {
            if !matches_cluster(&allowed_clusters, cluster)
                || principal
                    .allowed_clusters
                    .as_ref()
                    .is_some_and(|clusters| !matches_cluster(clusters, cluster))
            {
                return Err(GuardError::PermissionDenied(format!(
                    "principal `{}` is not authorized for cluster `{cluster}`",
                    principal.id
                )));
            }
        }
        Ok(())
    }

    fn require_scope(&self, principal: &Principal, risk_level: RiskLevel) -> Result<(), GuardError> {
        let required_scope = match risk_level {
            RiskLevel::ReadOnly => "rocketmq:read",
            RiskLevel::Diagnose => "rocketmq:diagnose",
            RiskLevel::Plan => "rocketmq:plan",
            RiskLevel::Destructive => "rocketmq:write",
        };
        if principal.scopes.contains(required_scope) {
            return Ok(());
        }
        Err(GuardError::PermissionDenied(format!(
            "principal `{}` lacks required scope `{required_scope}`",
            principal.id
        )))
    }

    fn collect_role(&self, role_name: &str, visited: &mut BTreeSet<String>) -> Result<Vec<String>, GuardError> {
        if !visited.insert(role_name.to_string()) {
            return Err(GuardError::InvalidArgument(format!(
                "permissions role include cycle contains `{role_name}`"
            )));
        }
        let role = self
            .roles
            .get(role_name)
            .ok_or_else(|| GuardError::PermissionDenied(format!("principal references unknown role `{role_name}`")))?;
        let mut resolved = vec![role_name.to_string()];
        for included in &role.include {
            resolved.extend(self.collect_role(included, visited)?);
        }
        visited.remove(role_name);
        Ok(resolved)
    }
}

fn matches_pattern(patterns: &[String], value: &str) -> bool {
    patterns.iter().any(|pattern| pattern == "*" || pattern == value)
}

fn matches_cluster(clusters: &BTreeSet<String>, cluster: &str) -> bool {
    clusters.contains("*") || clusters.contains(cluster)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
