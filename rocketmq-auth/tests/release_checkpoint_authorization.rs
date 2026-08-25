// Copyright 2023 The RocketMQ Rust Authors
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
use std::fs;

use rocketmq_auth::MaintenancePolicyReference;
use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizationError;
use rocketmq_security_api::MaintenanceAuthorizer;
use rocketmq_security_api::MaintenanceCapability;
use rocketmq_security_api::MaintenancePolicy;
use rocketmq_security_api::MaintenancePrincipalBinding;
use rocketmq_security_api::MaintenanceRequestClass;
use rocketmq_security_api::MaintenanceResourceBudget;
use rocketmq_security_api::MaintenanceRole;
use rocketmq_security_api::MaintenanceRoleGrant;
use sha2::Digest;
use sha2::Sha256;
use tempfile::TempDir;

fn release_checkpoint_policy() -> MaintenancePolicy {
    MaintenancePolicy {
        schema_version: 1,
        policy_id: "rocketmq.production-maintenance".to_string(),
        policy_version: 7,
        require_authentication: true,
        require_authorization: true,
        require_fencing_token: true,
        max_request_lifetime_millis: 30_000,
        resource_budget: MaintenanceResourceBudget {
            max_checkpoint_bytes: 1_073_741_824,
            max_store_members: 16,
            max_concurrent_operations: 1,
        },
        principal_bindings: vec![
            MaintenancePrincipalBinding {
                principal: "release-operator".to_string(),
                roles: BTreeSet::from([MaintenanceRole::ReleaseOperator]),
            },
            MaintenancePrincipalBinding {
                principal: "ordinary-admin".to_string(),
                roles: BTreeSet::from([MaintenanceRole::Administrator]),
            },
        ],
        role_grants: vec![MaintenanceRoleGrant {
            role: MaintenanceRole::ReleaseOperator,
            capabilities: BTreeSet::from([MaintenanceCapability::ReleaseCheckpoint]),
        }],
    }
}

fn authorizer() -> (TempDir, MaintenanceAuthorizer) {
    let temp = TempDir::new().expect("create policy directory");
    let path = temp.path().join("maintenance-policy.json");
    let bytes = serde_json::to_vec_pretty(&release_checkpoint_policy()).expect("serialize policy");
    fs::write(&path, &bytes).expect("write policy");
    let reference = MaintenancePolicyReference {
        path: path.clone(),
        version: 7,
        sha256: hex::encode(Sha256::digest(&bytes)),
    };
    let loaded = reference
        .load_from(temp.path())
        .expect("load pinned maintenance policy");
    (temp, MaintenanceAuthorizer::new(loaded))
}

fn context(principal: Option<&str>) -> MaintenanceAuthorizationContext {
    MaintenanceAuthorizationContext {
        authentication_enabled: true,
        authorization_enabled: true,
        principal: principal.map(str::to_owned),
        request_class: MaintenanceRequestClass::PrivilegedMaintenance,
        capability: MaintenanceCapability::ReleaseCheckpoint,
        deadline_unix_millis: 120_000,
        fencing_token: Some(42),
    }
}

#[test]
fn release_checkpoint_requires_independent_release_operator_role() {
    let (_temp, authorizer) = authorizer();

    let grant = authorizer
        .authorize(Some(&context(Some("release-operator"))), 100_000)
        .expect("release operator should be authorized");

    assert_eq!(grant.role(), MaintenanceRole::ReleaseOperator);
    assert_eq!(grant.capability(), MaintenanceCapability::ReleaseCheckpoint);
    assert_eq!(grant.policy_version(), 7);
    assert_eq!(grant.fencing_token(), 42);
}

#[test]
fn release_checkpoint_fails_closed_for_missing_or_anonymous_context() {
    let (_temp, authorizer) = authorizer();

    assert_eq!(
        authorizer.authorize(None, 100_000),
        Err(MaintenanceAuthorizationError::MissingAuthorizationContext)
    );
    assert_eq!(
        authorizer.authorize(Some(&context(None)), 100_000),
        Err(MaintenanceAuthorizationError::Anonymous)
    );
}

#[test]
fn release_checkpoint_fails_closed_for_ordinary_admin_and_disabled_auth() {
    let (_temp, authorizer) = authorizer();

    assert!(matches!(
        authorizer.authorize(Some(&context(Some("ordinary-admin"))), 100_000),
        Err(MaintenanceAuthorizationError::MissingRole {
            role: MaintenanceRole::ReleaseOperator,
            ..
        })
    ));

    let mut auth_disabled = context(Some("release-operator"));
    auth_disabled.authentication_enabled = false;
    assert_eq!(
        authorizer.authorize(Some(&auth_disabled), 100_000),
        Err(MaintenanceAuthorizationError::AuthenticationDisabled)
    );

    let mut authorization_disabled = context(Some("release-operator"));
    authorization_disabled.authorization_enabled = false;
    assert_eq!(
        authorizer.authorize(Some(&authorization_disabled), 100_000),
        Err(MaintenanceAuthorizationError::AuthorizationDisabled)
    );
}

#[test]
fn release_checkpoint_rejects_expired_deadline_and_missing_fencing_token() {
    let (_temp, authorizer) = authorizer();

    let mut expired = context(Some("release-operator"));
    expired.deadline_unix_millis = 100_000;
    assert_eq!(
        authorizer.authorize(Some(&expired), 100_000),
        Err(MaintenanceAuthorizationError::DeadlineExpired)
    );

    let mut unfenced = context(Some("release-operator"));
    unfenced.fencing_token = Some(0);
    assert_eq!(
        authorizer.authorize(Some(&unfenced), 100_000),
        Err(MaintenanceAuthorizationError::MissingFencingToken)
    );
}

#[test]
fn release_checkpoint_policy_reference_detects_tampering_and_version_drift() {
    let temp = TempDir::new().expect("create policy directory");
    let path = temp.path().join("maintenance-policy.json");
    let bytes = serde_json::to_vec_pretty(&release_checkpoint_policy()).expect("serialize policy");
    fs::write(&path, &bytes).expect("write policy");

    let wrong_digest = MaintenancePolicyReference {
        path: path.clone(),
        version: 7,
        sha256: "0".repeat(64),
    };
    assert!(wrong_digest.load_from(temp.path()).is_err());

    let wrong_version = MaintenancePolicyReference {
        path,
        version: 8,
        sha256: hex::encode(Sha256::digest(&bytes)),
    };
    assert!(wrong_version.load_from(temp.path()).is_err());
}
