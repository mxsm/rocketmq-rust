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

mod acl;
mod authentication;
mod authorization;
mod bootstrap;
mod config;
mod credential_rotation;
mod layered_authorization;
mod maintenance;
mod migration;
mod permission;
mod remoting_auth_context;
mod runtime;
mod secret_provider;
/// Runtime-neutral contracts implemented by authentication and authorization providers.
mod security_api {
    pub use rocketmq_security_api::*;
}
pub(crate) mod runtime_bridge;

// Re-export commonly used authentication types
pub use authentication::acl_signer::cal_signature;
pub use authentication::acl_signer::cal_signature_segments;
pub use authentication::acl_signer::cal_signature_segments_with_algorithm;
pub use authentication::acl_signer::cal_signature_with_algorithm;
pub use authentication::acl_signer::SignatureAlgorithm;
pub use authentication::acl_signer::DEFAULT_CHARSET;
pub use authentication::builder::AuthenticationContextBuilder;
pub use authentication::builder::DefaultAuthenticationContextBuilder;
pub use authentication::context::default_authentication_context::DefaultAuthenticationContext;
pub use authentication::enums::subject_type::SubjectType;
pub use authentication::enums::user_status::UserStatus;
pub use authentication::enums::user_type::UserType;
pub use authentication::evaluator::AuthenticationEvaluator;
pub use authentication::factory::AuthenticationFactory;
pub use authentication::manager::AuthenticationMetadataManager;
pub use authentication::manager::AuthenticationMetadataManagerImpl;
pub use authentication::model::subject::Subject;
pub use authentication::model::user::User;
pub use authentication::provider::AuthenticationMetadataProvider;
pub use authentication::provider::AuthenticationProvider;
pub use authentication::provider::DefaultAuthenticationProvider;
pub use authentication::provider::LocalAuthenticationMetadataProvider;
pub use authentication::strategy::AllowAllAuthenticationStrategy;
pub use authentication::strategy::AuthenticationFuture;
pub use authentication::strategy::AuthenticationStrategy;
pub use authentication::strategy::StatefulAuthenticationStrategy;
pub use authentication::strategy::StatelessAuthenticationStrategy;
pub use authentication::AclClientRpcHook;

// Re-export commonly used authorization types
pub use acl::FileAclConfigStore;
pub use acl::WhiteList;
pub use authorization::chain::AclAuthorizationHandler;
pub use authorization::chain::AuthorizationHandler;
pub use authorization::chain::AuthorizationHandlerChain;
pub use authorization::context::authentication_context::AuthenticationContext;
pub use authorization::context::default_authorization_context::DefaultAuthorizationContext;
/// Canonical decision type for authorization policy models.
pub use authorization::enums::decision::Decision as PolicyDecision;
/// Frozen 1.x compatibility name for [`PolicyDecision`].
pub use authorization::enums::decision::Decision;
pub use authorization::enums::policy_type::PolicyType;
pub use authorization::evaluator::AuthorizationEvaluator;
pub use authorization::factory::AuthorizationFactory;
pub use authorization::manager::metadata_manager::AuthorizationMetadataManager;
pub use authorization::manager::AuthorizationMetadataManagerImpl;
pub use authorization::metadata_provider::AuthorizationMetadataProvider;
pub use authorization::metadata_provider::LocalAuthorizationMetadataProvider;
pub use authorization::metadata_provider::MetadataResult;
pub use authorization::metadata_provider::NoopMetadataProvider;
pub use authorization::model::acl::Acl;
pub use authorization::model::environment::Environment;
pub use authorization::model::policy::Policy;
pub use authorization::model::policy_entry::PolicyEntry;
/// Canonical request type for authorization policy models.
pub use authorization::model::request_context::RequestContext as AuthorizationRequest;
/// Frozen 1.x compatibility name for [`AuthorizationRequest`].
pub use authorization::model::request_context::RequestContext;
/// Canonical resource type for authorization policy models.
pub use authorization::model::resource::Resource as PolicyResource;
/// Frozen 1.x compatibility name for [`PolicyResource`].
pub use authorization::model::resource::Resource;
pub use authorization::provider::AuthorizationError;
pub use authorization::provider::AuthorizationProvider;
pub use authorization::provider::AuthorizationResult;
pub use authorization::provider::DefaultAuthorizationProvider;
pub use authorization::strategy::abstract_authorization_strategy::AuthorizationStrategy;
pub use authorization::strategy::StatefulAuthorizationStrategy;
pub use authorization::strategy::StatelessAuthorizationStrategy;
#[doc(hidden)]
pub use bench_support::AuthAclWatcherLifecycleProbe;
pub use bootstrap::BootstrapAdminIdentity;
pub use bootstrap::BootstrapAdminProvisioner;
pub use bootstrap::BootstrapAdminProvisioningError;
pub use bootstrap::BootstrapEnrollmentRequest;
pub use bootstrap::BootstrapEnrollmentResult;
pub use bootstrap::BootstrapError;
pub use bootstrap::BootstrapGrant;
pub use bootstrap::BootstrapStatus;
pub use bootstrap::BootstrapTransportContext;
pub use bootstrap::OneTimeBootstrap;
pub use config::AuthConfig;
pub use credential_rotation::BreakGlassReason;
pub use credential_rotation::BreakGlassStatus;
pub use credential_rotation::CredentialAuditAction;
pub use credential_rotation::CredentialAuditEvent;
pub use credential_rotation::CredentialAuditOutcome;
pub use credential_rotation::CredentialAuditSink;
pub use credential_rotation::CredentialAuditSinkError;
pub use credential_rotation::CredentialBundleParseError;
pub use credential_rotation::CredentialBundleParser;
pub use credential_rotation::CredentialDescriptor;
pub use credential_rotation::CredentialId;
pub use credential_rotation::CredentialRotationError;
pub use credential_rotation::CredentialRotationManager;
pub use credential_rotation::CredentialRotationSnapshot;
pub use credential_rotation::CredentialVerification;
pub use credential_rotation::CredentialVerificationSource;
pub use credential_rotation::RetiringCredentialSnapshot;
pub use credential_rotation::ValidatedCredential;
pub use layered_authorization::project_authorization_error;
pub use layered_authorization::project_authorization_result;
pub use layered_authorization::project_policy_decision;
pub use maintenance::LoadedMaintenancePolicy;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceAuthorizationContext; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceAuthorizationContext;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceAuthorizationError; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceAuthorizationError;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceAuthorizationGrant; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceAuthorizationGrant;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceAuthorizer; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceAuthorizer;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceCapability; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceCapability;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenancePolicy; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenancePolicy;
pub use maintenance::MaintenancePolicyError;
pub use maintenance::MaintenancePolicyReference;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenancePrincipalBinding; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenancePrincipalBinding;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceRequestClass; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceRequestClass;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceResourceBudget; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceResourceBudget;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceRole; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceRole;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MaintenanceRoleGrant; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MaintenanceRoleGrant;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::MAINTENANCE_POLICY_SCHEMA_VERSION; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use maintenance::MAINTENANCE_POLICY_SCHEMA_VERSION;
pub use permission::Permission;
pub use remoting_auth_context::RemotingAuthContext;
pub use rocketmq_observability::metrics::auth::AuthMetricSample;
pub use rocketmq_observability::metrics::auth::AuthMetrics;
pub use rocketmq_observability::metrics::auth::AuthMetricsSnapshot;
pub use runtime::AuthRuntime;
pub use runtime::AuthRuntimeBuilder;
pub use runtime::AuthenticationService;
pub use runtime::AuthorizationService;
pub use runtime::ProviderRegistry;
pub use secret_provider::EncryptedFileSecretProvider;
pub use secret_provider::EnvironmentSecretProvider;
pub use secret_provider::SecretProviderRegistry;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::Principal; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use security_api::Principal as SecurityPrincipal;
#[deprecated(
    since = "1.1.0",
    note = "use rocketmq_security_api::Resource; removal is intended for a future 2.0 boundary and remains subject to compatibility, migration, and release gates"
)]
pub use security_api::Resource as SecurityResource;

#[doc(hidden)]
pub mod bench_support {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::time::Instant;

    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQResult;
    use rocketmq_runtime::ChildServiceContext;
    use rocketmq_runtime::ShutdownReport;
    use serde::Serialize;

    use crate::authentication::provider::authentication_metadata_provider::AuthenticationMetadataProvider;
    use crate::config::AuthConfig;
    use crate::runtime::AuthRuntimeBuilder;

    static NEXT_ACL_WATCHER_PROBE_ID: AtomicU64 = AtomicU64::new(0);

    #[derive(Clone, Debug, Serialize)]
    pub struct AuthAclWatcherLifecycleProbe {
        pub scheduled_runs: u64,
        pub scheduled_skips: u64,
        pub scheduled_overlaps: u64,
        pub scheduled_failures: u64,
        pub reload_success: bool,
        pub shutdown_elapsed_us: u128,
        pub shutdown_report: Option<ShutdownReport>,
        pub healthy: bool,
    }

    pub async fn run_auth_acl_watcher_lifecycle_probe(
        service_context: ChildServiceContext,
    ) -> RocketMQResult<AuthAclWatcherLifecycleProbe> {
        let root = unique_acl_watcher_probe_root();
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).map_err(|error| {
            rocketmq_error::RocketMQError::storage_write_failed(root.display().to_string(), error.to_string())
        })?;
        let acl_file = root.join("plain_acl.yml");
        write_acl_file(&acl_file, "first")?;

        let runtime = AuthRuntimeBuilder::new(
            AuthConfig {
                acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
                acl_file_watch_enabled: true,
                acl_file_watch_interval_millis: 5,
                ..AuthConfig::default()
            },
            service_context,
        )
        .build()
        .await?;
        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        write_acl_file(&acl_file, "second")?;

        let deadline = Instant::now() + Duration::from_secs(2);
        let reload_success = loop {
            match authn_provider.get_user("alice").await {
                Ok(user) if user.password().map(|value| value.as_str()) == Some("second") => {
                    break true;
                }
                Ok(_) => {}
                Err(rocketmq_error::RocketMQError::Authentication(rocketmq_error::AuthError::UserNotFound(
                    username,
                ))) if username == "alice" => {}
                Err(error) => return Err(error),
            }
            if Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };

        let mut snapshots = runtime.acl_file_watcher_snapshot();
        for _ in 0..50 {
            if snapshots
                .iter()
                .any(|snapshot| snapshot.runs > 0 && snapshot.active_runs == 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
            snapshots = runtime.acl_file_watcher_snapshot();
        }
        let scheduled_runs = snapshots.iter().map(|snapshot| snapshot.runs).sum();
        let scheduled_skips = snapshots.iter().map(|snapshot| snapshot.skips).sum();
        let scheduled_overlaps = snapshots.iter().map(|snapshot| snapshot.overlaps).sum();
        let scheduled_failures = snapshots.iter().map(|snapshot| snapshot.failures).sum();

        let shutdown_started_at = Instant::now();
        let shutdown_report = runtime.shutdown_with_report().await?;
        let shutdown_elapsed_us = shutdown_started_at.elapsed().as_micros();
        let shutdown_healthy = shutdown_report
            .as_ref()
            .map(ShutdownReport::is_healthy)
            .unwrap_or(false);
        let healthy = reload_success
            && scheduled_runs > 0
            && scheduled_overlaps == 0
            && scheduled_failures == 0
            && shutdown_healthy;

        let _ = fs::remove_dir_all(root);
        Ok(AuthAclWatcherLifecycleProbe {
            scheduled_runs,
            scheduled_skips,
            scheduled_overlaps,
            scheduled_failures,
            reload_success,
            shutdown_elapsed_us,
            shutdown_report,
            healthy,
        })
    }

    fn unique_acl_watcher_probe_root() -> PathBuf {
        let id = NEXT_ACL_WATCHER_PROBE_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("rocketmq-auth-acl-watcher-{}-{id}", std::process::id()))
    }

    fn write_acl_file(path: &std::path::Path, secret: &str) -> RocketMQResult<()> {
        let content = format!(
            r#"
accounts:
  - accessKey: alice
    secretKey: {secret}
"#
        );
        let temp_file = temp_acl_file_path(path);
        fs::write(&temp_file, content).map_err(|error| {
            rocketmq_error::RocketMQError::storage_write_failed(temp_file.display().to_string(), error.to_string())
        })?;

        replace_acl_file(&temp_file, path)
    }

    fn temp_acl_file_path(path: &std::path::Path) -> PathBuf {
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("plain_acl.yml");
        let id = NEXT_ACL_WATCHER_PROBE_ID.fetch_add(1, Ordering::Relaxed);
        path.with_file_name(format!(".{file_name}.{id}.tmp"))
    }

    #[cfg(not(windows))]
    fn replace_acl_file(temp_file: &std::path::Path, path: &std::path::Path) -> RocketMQResult<()> {
        fs::rename(temp_file, path).map_err(|error| {
            rocketmq_error::RocketMQError::storage_write_failed(path.display().to_string(), error.to_string())
        })
    }

    #[cfg(windows)]
    fn replace_acl_file(temp_file: &std::path::Path, path: &std::path::Path) -> RocketMQResult<()> {
        fs::copy(temp_file, path).map_err(|error| {
            rocketmq_error::RocketMQError::storage_write_failed(path.display().to_string(), error.to_string())
        })?;
        let _ = fs::remove_file(temp_file);
        Ok(())
    }
}

#[cfg(test)]
mod bench_support_tests {
    use rocketmq_runtime::RuntimeContext;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_acl_watcher_probe_reports_clean_shutdown() {
        let runtime = RuntimeContext::from_current("auth-acl-watcher-probe-test");
        let probe = super::bench_support::run_auth_acl_watcher_lifecycle_probe(
            runtime.service_context("auth-acl-watcher-probe"),
        )
        .await
        .expect("auth ACL watcher lifecycle probe should run");

        assert!(probe.healthy, "{probe:?}");
        assert!(probe.reload_success, "{probe:?}");
        assert_eq!(probe.scheduled_overlaps, 0, "{probe:?}");
        assert_eq!(probe.scheduled_failures, 0, "{probe:?}");
    }
}
