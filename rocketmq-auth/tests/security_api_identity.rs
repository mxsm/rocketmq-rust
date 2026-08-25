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

use rocketmq_auth::AuthorizationRequest;
use rocketmq_auth::Decision as LegacyPolicyDecision;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceAuthorizationContext as LegacyMaintenanceAuthorizationContext;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceAuthorizationError as LegacyMaintenanceAuthorizationError;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceAuthorizationGrant as LegacyMaintenanceAuthorizationGrant;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceAuthorizer as LegacyMaintenanceAuthorizer;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceCapability as LegacyMaintenanceCapability;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenancePolicy as LegacyMaintenancePolicy;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenancePrincipalBinding as LegacyMaintenancePrincipalBinding;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceRequestClass as LegacyMaintenanceRequestClass;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceResourceBudget as LegacyMaintenanceResourceBudget;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceRole as LegacyMaintenanceRole;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::MaintenanceRoleGrant as LegacyMaintenanceRoleGrant;
use rocketmq_auth::PolicyDecision;
use rocketmq_auth::PolicyResource;
use rocketmq_auth::RequestContext as LegacyAuthorizationRequest;
use rocketmq_auth::Resource as LegacyPolicyResource;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::SecurityPrincipal as LegacyPrincipal;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain type identity."
)]
use rocketmq_auth::SecurityResource as LegacyResource;
#[allow(
    deprecated,
    reason = "This compile-only compatibility test proves frozen 1.x aliases retain value identity."
)]
use rocketmq_auth::MAINTENANCE_POLICY_SCHEMA_VERSION as LEGACY_MAINTENANCE_POLICY_SCHEMA_VERSION;
use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizationError;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceAuthorizer;
use rocketmq_security_api::MaintenanceCapability;
use rocketmq_security_api::MaintenancePolicy;
use rocketmq_security_api::MaintenancePrincipalBinding;
use rocketmq_security_api::MaintenanceRequestClass;
use rocketmq_security_api::MaintenanceResourceBudget;
use rocketmq_security_api::MaintenanceRole;
use rocketmq_security_api::MaintenanceRoleGrant;
use rocketmq_security_api::Principal;
use rocketmq_security_api::Resource;
use rocketmq_security_api::MAINTENANCE_POLICY_SCHEMA_VERSION;

fn type_identity<T>(value: T) -> T {
    value
}

#[test]
fn auth_compatibility_path_exports_canonical_security_types() {
    fn principal_identity(value: Principal) -> LegacyPrincipal {
        value
    }
    fn resource_identity(value: Resource) -> LegacyResource {
        value
    }

    assert_eq!(principal_identity(Principal::new("alice")).id(), "alice");
    assert_eq!(resource_identity(Resource::topic("TopicA")).name(), "TopicA");
}

#[test]
fn frozen_maintenance_aliases_compile_with_canonical_type_identity() {
    let _: fn(PolicyDecision) -> LegacyPolicyDecision = type_identity::<PolicyDecision>;
    let _: fn(PolicyResource) -> LegacyPolicyResource = type_identity::<PolicyResource>;
    let _: fn(AuthorizationRequest) -> LegacyAuthorizationRequest = type_identity::<AuthorizationRequest>;
    let _: fn(MaintenanceAuthorizationContext) -> LegacyMaintenanceAuthorizationContext =
        type_identity::<MaintenanceAuthorizationContext>;
    let _: fn(MaintenanceAuthorizationError) -> LegacyMaintenanceAuthorizationError =
        type_identity::<MaintenanceAuthorizationError>;
    let _: fn(MaintenanceAuthorizationGrant) -> LegacyMaintenanceAuthorizationGrant =
        type_identity::<MaintenanceAuthorizationGrant>;
    let _: fn(MaintenanceAuthorizer) -> LegacyMaintenanceAuthorizer = type_identity::<MaintenanceAuthorizer>;
    let _: fn(MaintenanceCapability) -> LegacyMaintenanceCapability = type_identity::<MaintenanceCapability>;
    let _: fn(MaintenancePolicy) -> LegacyMaintenancePolicy = type_identity::<MaintenancePolicy>;
    let _: fn(MaintenancePrincipalBinding) -> LegacyMaintenancePrincipalBinding =
        type_identity::<MaintenancePrincipalBinding>;
    let _: fn(MaintenanceRequestClass) -> LegacyMaintenanceRequestClass = type_identity::<MaintenanceRequestClass>;
    let _: fn(MaintenanceResourceBudget) -> LegacyMaintenanceResourceBudget =
        type_identity::<MaintenanceResourceBudget>;
    let _: fn(MaintenanceRole) -> LegacyMaintenanceRole = type_identity::<MaintenanceRole>;
    let _: fn(MaintenanceRoleGrant) -> LegacyMaintenanceRoleGrant = type_identity::<MaintenanceRoleGrant>;

    assert_eq!(
        LEGACY_MAINTENANCE_POLICY_SCHEMA_VERSION,
        MAINTENANCE_POLICY_SCHEMA_VERSION
    );
}
