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

use std::collections::BTreeSet;
use std::str::FromStr;

use crate::catalog::OperationCatalog;
use crate::config::MutationPolicyConfig;
use crate::config::REQUIRED_WRITE_SCOPE;
use crate::error::ControlError;
use crate::model::ClusterName;
use crate::model::ControlOperation;
use crate::model::MutationArguments;
use crate::model::Principal;

#[derive(Debug, Clone)]
pub struct MutationGuard {
    runtime_enabled: bool,
    default_dry_run: bool,
    allowed_operations: BTreeSet<ControlOperation>,
    allowed_clusters: BTreeSet<ClusterName>,
}

impl MutationGuard {
    pub fn new(policy: &MutationPolicyConfig) -> Self {
        Self {
            runtime_enabled: policy.mutations_enabled,
            default_dry_run: policy.dry_run,
            allowed_operations: policy.operation_allowlist(),
            allowed_clusters: policy.cluster_allowlist(),
        }
    }

    /// Authorizes a closed operation and cluster before request-schema processing.
    ///
    pub fn authorize_raw(
        &self,
        principal: &Principal,
        operation: &str,
        cluster: &str,
        catalog: &OperationCatalog,
    ) -> Result<AuthorizedMutation, ControlError> {
        if !principal.scopes.contains(REQUIRED_WRITE_SCOPE) {
            return Err(ControlError::permission_denied());
        }
        let cluster = ClusterName::try_new(cluster).map_err(|_| ControlError::cluster_not_allowed())?;
        if !principal.allowed_clusters.contains(&cluster) || !self.allowed_clusters.contains(&cluster) {
            return Err(ControlError::cluster_not_allowed());
        }
        let operation = ControlOperation::from_str(operation).map_err(|_| ControlError::operation_not_allowed())?;
        if !principal.allowed_operations.contains(&operation) || !self.allowed_operations.contains(&operation) {
            return Err(ControlError::operation_not_allowed());
        }
        if !self.runtime_enabled {
            return Err(ControlError::mutation_disabled());
        }
        if !cfg!(feature = "write-tools") || !catalog.is_registered(operation) {
            return Err(ControlError::operation_unavailable());
        }
        Ok(AuthorizedMutation {
            operation,
            cluster,
            operator: principal.subject.clone(),
        })
    }

    /// Parses common arguments only after an [`AuthorizedMutation`] has been minted.
    pub fn parse_arguments(
        &self,
        _authorized: &AuthorizedMutation,
        raw: &serde_json::Value,
    ) -> Result<MutationArguments, ControlError> {
        let dry_run_omitted = raw.as_object().is_some_and(|object| !object.contains_key("dry_run"));
        let mut arguments: MutationArguments =
            serde_json::from_value(raw.clone()).map_err(|_| ControlError::invalid_argument())?;
        if dry_run_omitted {
            arguments.dry_run = self.default_dry_run;
        }
        arguments.validate()?;
        Ok(arguments)
    }

    pub const fn default_dry_run(&self) -> bool {
        self.default_dry_run
    }

    pub fn allows_discovery(
        &self,
        principal: &Principal,
        operation: ControlOperation,
        configured_clusters: &BTreeSet<ClusterName>,
    ) -> bool {
        self.runtime_enabled
            && principal.scopes.contains(REQUIRED_WRITE_SCOPE)
            && principal.allowed_operations.contains(&operation)
            && self.allowed_operations.contains(&operation)
            && principal
                .allowed_clusters
                .iter()
                .any(|cluster| self.allowed_clusters.contains(cluster) && configured_clusters.contains(cluster))
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizedMutation {
    operation: ControlOperation,
    cluster: ClusterName,
    operator: String,
}

impl AuthorizedMutation {
    pub const fn operation(&self) -> ControlOperation {
        self.operation
    }

    pub fn cluster(&self) -> &ClusterName {
        &self.cluster
    }

    pub(crate) fn operator(&self) -> &str {
        &self.operator
    }

    #[cfg(test)]
    pub(crate) fn synthetic(operation: ControlOperation, cluster: ClusterName) -> Self {
        Self {
            operation,
            cluster,
            operator: "test-operator".to_owned(),
        }
    }
}

impl std::fmt::Debug for AuthorizedMutation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedMutation")
            .field("operation", &self.operation)
            .field("cluster", &self.cluster)
            .field("operator_validated", &true)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(enabled: bool) -> MutationPolicyConfig {
        MutationPolicyConfig {
            mutations_enabled: enabled,
            dry_run: true,
            allowed_operations: vec![ControlOperation::TopicUpsert],
            allowed_clusters: vec![ClusterName::try_new("cluster-a").unwrap()],
            operation_timeout_seconds: 24,
        }
    }

    fn principal(scopes: &[&str]) -> Principal {
        Principal {
            subject: "operator".to_string(),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            allowed_operations: BTreeSet::from([ControlOperation::TopicUpsert]),
            allowed_clusters: BTreeSet::from([ClusterName::try_new("cluster-a").unwrap()]),
        }
    }

    #[test]
    fn scope_and_intersections_are_checked_before_availability() {
        let guard = MutationGuard::new(&policy(true));
        let denied = guard
            .authorize_raw(&principal(&[]), "unknown", "bad.cluster", &OperationCatalog::default())
            .unwrap_err();
        assert_eq!(denied.code(), crate::error::ControlErrorCode::PermissionDenied);

        let unavailable = guard
            .authorize_raw(
                &principal(&[REQUIRED_WRITE_SCOPE]),
                "topic_upsert",
                "cluster-a",
                &OperationCatalog::default(),
            )
            .unwrap_err();
        assert_eq!(unavailable.code(), crate::error::ControlErrorCode::OperationUnavailable);
    }

    #[test]
    fn authorization_order_has_specific_codes_without_exposing_argument_schema() {
        let disabled = MutationGuard::new(&policy(false));
        let catalog = OperationCatalog::default();
        let cases = [
            (
                principal(&[]),
                "unknown",
                "bad.cluster",
                crate::error::ControlErrorCode::PermissionDenied,
            ),
            (
                principal(&[REQUIRED_WRITE_SCOPE]),
                "unknown",
                "bad.cluster",
                crate::error::ControlErrorCode::ClusterNotAllowed,
            ),
            (
                principal(&[REQUIRED_WRITE_SCOPE]),
                "unknown",
                "cluster-a",
                crate::error::ControlErrorCode::OperationNotAllowed,
            ),
            (
                principal(&[REQUIRED_WRITE_SCOPE]),
                "topic_upsert",
                "cluster-a",
                crate::error::ControlErrorCode::MutationDisabled,
            ),
        ];
        for (principal, operation, cluster, expected) in cases {
            let error = disabled
                .authorize_raw(&principal, operation, cluster, &catalog)
                .unwrap_err();
            assert_eq!(error.code(), expected);
        }
    }

    #[test]
    fn zero_catalog_rejects_before_argument_schema() {
        let guard = MutationGuard::new(&policy(true));
        let error = guard
            .authorize_raw(
                &principal(&[REQUIRED_WRITE_SCOPE]),
                "topic_upsert",
                "cluster-a",
                &OperationCatalog::default(),
            )
            .unwrap_err();
        assert_eq!(error.code(), crate::error::ControlErrorCode::OperationUnavailable);
    }

    #[test]
    fn unauthorized_and_zero_catalog_requests_never_parse_schema() {
        let guard = MutationGuard::new(&policy(true));
        let malformed = [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!({"unknown": true}),
            serde_json::json!({"dry_run": "not-a-boolean"}),
            serde_json::json!({"reason": ""}),
        ];
        let expected = ControlError::permission_denied().envelope();
        for _raw in malformed {
            let denied = guard
                .authorize_raw(
                    &principal(&[]),
                    "topic_upsert",
                    "cluster-a",
                    &OperationCatalog::default(),
                )
                .unwrap_err();
            assert_eq!(denied.envelope(), expected);
        }

        let unavailable = guard
            .authorize_raw(
                &principal(&[REQUIRED_WRITE_SCOPE]),
                "topic_upsert",
                "cluster-a",
                &OperationCatalog::default(),
            )
            .unwrap_err();
        assert_eq!(unavailable.code(), crate::error::ControlErrorCode::OperationUnavailable);
    }

    #[test]
    fn configured_dry_run_default_is_applied_only_when_omitted() {
        let mut configured = policy(true);
        configured.dry_run = false;
        let guard = MutationGuard::new(&configured);
        let authorized = AuthorizedMutation::synthetic(
            ControlOperation::TopicUpsert,
            ClusterName::try_new("cluster-a").unwrap(),
        );
        let omitted = guard
            .parse_arguments(
                &authorized,
                &serde_json::json!({
                    "schema_version": crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION,
                    "confirm": true,
                    "reason": "planned change"
                }),
            )
            .unwrap();
        assert!(!omitted.dry_run);

        let explicit = guard
            .parse_arguments(
                &authorized,
                &serde_json::json!({
                    "schema_version": crate::model::MUTATION_ARGUMENTS_SCHEMA_VERSION,
                    "dry_run": true
                }),
            )
            .unwrap();
        assert!(explicit.dry_run);
    }
}
