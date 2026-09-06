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

use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::BlockingExecutor;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::MetadataIoActor;
use rocketmq_runtime::MetadataIoConfig;
use rocketmq_runtime::ScheduledTaskConfig;
use rocketmq_runtime::ScheduledTaskGroup;
use rocketmq_runtime::ScheduledTaskSnapshot;
use rocketmq_runtime::ShutdownReport;
use rocketmq_security_api::DetailedDecision;
use rocketmq_security_api::LayerEvaluation;
use rocketmq_security_api::LayerFailureKind;
use rocketmq_security_api::LayerRequirement;
use rocketmq_security_api::ResourcePattern;
use rocketmq_security_api::ResourceType;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::acl::AclConfigFingerprint;
use crate::acl::FileAclConfigLoader;
use crate::acl::WhiteList;
use crate::authentication::builder::default_authentication_context_builder::DefaultAuthenticationContextBuilder;
use crate::authentication::builder::AuthenticationContextBuilder;
use crate::authentication::enums::subject_type::SubjectType;
use crate::authentication::enums::user_status::UserStatus;
use crate::authentication::enums::user_type::UserType;
use crate::authentication::model::user::User;
use crate::authentication::provider::authentication_metadata_provider::AuthenticationMetadataProvider;
use crate::authentication::provider::AuthenticationProvider;
use crate::authentication::provider::DefaultAuthenticationProvider;
use crate::authentication::provider::LocalAuthenticationMetadataProvider;
use crate::authorization::enums::policy_type::PolicyType;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
use crate::authorization::model::acl::Acl;
use crate::authorization::model::policy::Policy;
use crate::authorization::model::policy_entry::PolicyEntry;
use crate::authorization::model::resource::Resource;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationProvider;
use crate::authorization::provider::DefaultAuthorizationProvider;
use crate::config::AuthConfig;
use crate::migration::alc::acl_config::AclConfig;
use crate::migration::alc::plain_access_config::PlainAccessConfig;
use crate::migration::alc::plain_permission_manager::PlainPermissionManager;
use crate::permission::Permission;
use crate::project_authorization_error;
use crate::project_authorization_result;
use crate::AuthMetrics;
use crate::AuthMetricsSnapshot;
use crate::RemotingAuthContext;

const ACCESS_KEY: &str = "AccessKey";

#[derive(Clone)]
pub struct ProviderRegistry {
    authentication_metadata_provider: Arc<LocalAuthenticationMetadataProvider>,
    authorization_metadata_provider: Arc<LocalAuthorizationMetadataProvider>,
    acl_white_list_snapshot: Arc<RwLock<WhiteList>>,
    acl_managed_access_keys: Arc<RwLock<HashSet<String>>>,
    acl_generation: Arc<AtomicU64>,
    acl_fingerprint: Arc<RwLock<Option<AclConfigFingerprint>>>,
    metrics: AuthMetrics,
}

impl ProviderRegistry {
    pub fn local(config: &AuthConfig) -> RocketMQResult<Self> {
        Self::local_with_metadata_io(config, None)
    }

    pub fn local_with_metadata_io(config: &AuthConfig, metadata_io: Option<MetadataIoActor>) -> RocketMQResult<Self> {
        validate_metadata_provider_name(
            "authenticationMetadataProvider",
            config.authentication_metadata_provider.as_str(),
            &[
                "LocalAuthenticationMetadataProvider",
                "FileSnapshotAuthenticationMetadataProvider",
                "local",
                "file",
            ],
        )?;
        validate_metadata_provider_name(
            "authorizationMetadataProvider",
            config.authorization_metadata_provider.as_str(),
            &[
                "LocalAuthorizationMetadataProvider",
                "FileSnapshotAuthorizationMetadataProvider",
                "local",
                "file",
            ],
        )?;

        let authentication_metadata_provider = Arc::new(
            LocalAuthenticationMetadataProvider::with_config_and_metadata_io(config, metadata_io.clone())?,
        );
        let mut authorization_metadata_provider = metadata_io.map_or_else(
            LocalAuthorizationMetadataProvider::new,
            LocalAuthorizationMetadataProvider::with_metadata_io,
        );
        authorization_metadata_provider
            .initialize(config.clone(), None)
            .map_err(map_authorization_error)?;

        Ok(Self {
            authentication_metadata_provider,
            authorization_metadata_provider: Arc::new(authorization_metadata_provider),
            acl_white_list_snapshot: Arc::new(RwLock::new(WhiteList::default())),
            acl_managed_access_keys: Arc::new(RwLock::new(HashSet::new())),
            acl_generation: Arc::new(AtomicU64::new(0)),
            acl_fingerprint: Arc::new(RwLock::new(None)),
            metrics: AuthMetrics::default(),
        })
    }

    /// Loads persistent authentication and authorization snapshots on the
    /// caller-provided bounded metadata I/O lane.
    pub async fn load_with_metadata_io(
        config: &AuthConfig,
        metadata_io: MetadataIoActor,
        blocking: BlockingExecutor,
    ) -> RocketMQResult<Self> {
        let config = config.clone();
        blocking
            .spawn_io("auth.provider-registry.load", move || {
                Self::local_with_metadata_io(&config, Some(metadata_io))
            })
            .await
            .map_err(|error| RocketMQError::auth_config_invalid("authMetadataBootstrap", error.to_string()))?
    }

    pub fn authentication_metadata_provider(&self) -> Arc<LocalAuthenticationMetadataProvider> {
        self.authentication_metadata_provider.clone()
    }

    pub fn authorization_metadata_provider(&self) -> Arc<LocalAuthorizationMetadataProvider> {
        self.authorization_metadata_provider.clone()
    }

    fn set_acl_white_list_snapshot(&self, snapshot: WhiteList) -> RocketMQResult<()> {
        let mut guard = self
            .acl_white_list_snapshot
            .write()
            .map_err(|_| RocketMQError::StorageLockFailed {
                path: "auth.acl.white_list_snapshot".to_owned(),
            })?;
        *guard = snapshot;
        Ok(())
    }

    fn acl_managed_access_keys(&self) -> RocketMQResult<HashSet<String>> {
        let guard = self
            .acl_managed_access_keys
            .read()
            .map_err(|_| RocketMQError::StorageLockFailed {
                path: "auth.acl.managed_access_keys".to_owned(),
            })?;
        Ok(guard.clone())
    }

    fn set_acl_managed_access_keys(&self, access_keys: HashSet<String>) -> RocketMQResult<()> {
        let mut guard = self
            .acl_managed_access_keys
            .write()
            .map_err(|_| RocketMQError::StorageLockFailed {
                path: "auth.acl.managed_access_keys".to_owned(),
            })?;
        *guard = access_keys;
        Ok(())
    }

    fn advance_acl_generation(&self) -> u64 {
        self.metrics.record_cache_invalidation();
        self.acl_generation.fetch_add(1, AtomicOrdering::AcqRel) + 1
    }

    pub fn acl_generation(&self) -> u64 {
        self.acl_generation.load(AtomicOrdering::Acquire)
    }

    pub fn acl_generation_counter(&self) -> Arc<AtomicU64> {
        self.acl_generation.clone()
    }

    pub fn metrics(&self) -> AuthMetrics {
        self.metrics.clone()
    }

    pub fn metrics_snapshot(&self) -> AuthMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn acl_fingerprint(&self) -> RocketMQResult<Option<AclConfigFingerprint>> {
        let guard = self
            .acl_fingerprint
            .read()
            .map_err(|_| RocketMQError::StorageLockFailed {
                path: "auth.acl.fingerprint".to_owned(),
            })?;
        Ok(*guard)
    }

    fn set_acl_fingerprint(&self, fingerprint: Option<AclConfigFingerprint>) -> RocketMQResult<()> {
        let mut guard = self
            .acl_fingerprint
            .write()
            .map_err(|_| RocketMQError::StorageLockFailed {
                path: "auth.acl.fingerprint".to_owned(),
            })?;
        *guard = fingerprint;
        Ok(())
    }

    pub fn is_acl_white_remote_address(
        &self,
        access_key: Option<&str>,
        source_ip: Option<&str>,
    ) -> RocketMQResult<bool> {
        let guard = self
            .acl_white_list_snapshot
            .read()
            .map_err(|_| RocketMQError::StorageLockFailed {
                path: "auth.acl.white_list_snapshot".to_owned(),
            })?;
        let matched = guard.matches(access_key, source_ip);
        self.metrics.record_whitelist_check(matched);
        Ok(matched)
    }

    pub fn update_global_white_remote_addresses<I, S>(&self, addresses: I) -> RocketMQResult<u64>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let updated_snapshot = {
            let guard = self
                .acl_white_list_snapshot
                .read()
                .map_err(|_| RocketMQError::StorageLockFailed {
                    path: "auth.acl.white_list_snapshot".to_owned(),
                })?;
            guard.with_global_patterns(addresses)
        };
        self.set_acl_white_list_snapshot(updated_snapshot)?;
        Ok(self.advance_acl_generation())
    }
}

fn validate_metadata_provider_name(key: &'static str, configured: &str, supported: &[&str]) -> RocketMQResult<()> {
    let configured = configured.trim();
    if configured.is_empty()
        || supported
            .iter()
            .any(|candidate| configured.eq_ignore_ascii_case(candidate))
    {
        return Ok(());
    }

    Err(RocketMQError::auth_config_invalid(
        key,
        format!(
            "unsupported metadata provider '{}'; supported providers: {}",
            configured,
            supported.join(", ")
        ),
    ))
}

pub struct AuthRuntimeBuilder {
    config: AuthConfig,
    service_context: ChildServiceContext,
    provider_registry: Option<ProviderRegistry>,
    metadata_io: Option<MetadataIoActor>,
}

impl AuthRuntimeBuilder {
    pub fn new(config: AuthConfig, service_context: ChildServiceContext) -> Self {
        Self {
            config,
            service_context,
            provider_registry: None,
            metadata_io: None,
        }
    }

    pub fn with_provider_registry(mut self, provider_registry: ProviderRegistry) -> Self {
        self.provider_registry = Some(provider_registry);
        self
    }

    pub fn with_metadata_io_actor(mut self, metadata_io: MetadataIoActor) -> Self {
        self.metadata_io = Some(metadata_io);
        self
    }

    pub async fn build(self) -> RocketMQResult<AuthRuntime> {
        let metadata_io = match self.metadata_io {
            Some(metadata_io) => metadata_io,
            None => MetadataIoConfig::default()
                .into_plan()
                .expect("default metadata I/O config is valid")
                .start(&self.service_context.component("auth.metadata-io"))
                .map_err(|error| RocketMQError::auth_config_invalid("authRuntime", error.to_string()))?,
        };
        let provider_registry = match self.provider_registry {
            Some(provider_registry) => provider_registry,
            None => {
                ProviderRegistry::load_with_metadata_io(
                    &self.config,
                    metadata_io,
                    self.service_context.metadata_io().clone(),
                )
                .await?
            }
        };

        seed_initial_users(&provider_registry, &self.config).await?;
        let migrated_acl_entries = migrate_auth_from_v1(&provider_registry, &self.config).await?;
        if migrated_acl_entries > 0 {
            info!(
                "Migrated {} legacy ACL account(s) into auth metadata",
                migrated_acl_entries
            );
        }
        let loaded_acl_entries = load_configured_acl_file(
            &provider_registry,
            &self.config,
            self.service_context.metadata_io().clone(),
            true,
        )
        .await?
        .account_count;
        if loaded_acl_entries > 0 {
            info!("Loaded {} ACL account(s) from configured ACL file", loaded_acl_entries);
        }

        let mut authentication_provider = DefaultAuthenticationProvider::new();
        authentication_provider.initialize_with_registry(self.config.clone(), provider_registry.clone())?;

        let mut authorization_provider = DefaultAuthorizationProvider::new();
        authorization_provider
            .initialize_with_registry(self.config.clone(), provider_registry.clone())
            .map_err(map_authorization_error)?;

        let authentication_service = AuthenticationService::new(
            self.config.clone(),
            Arc::new(authentication_provider),
            provider_registry.metrics(),
        );
        let authorization_service = AuthorizationService::new(
            self.config.clone(),
            Arc::new(authorization_provider),
            provider_registry.metrics(),
        );
        let acl_file_watch_handle =
            start_acl_file_watcher(&self.config, provider_registry.clone(), &self.service_context);

        Ok(AuthRuntime {
            config: self.config,
            service_context: self.service_context,
            provider_registry,
            authentication_service,
            authorization_service,
            acl_file_watch_handle,
        })
    }
}

#[derive(Clone)]
pub struct AuthRuntime {
    config: AuthConfig,
    service_context: ChildServiceContext,
    provider_registry: ProviderRegistry,
    authentication_service: AuthenticationService,
    authorization_service: AuthorizationService,
    acl_file_watch_handle: Option<AclFileWatchHandle>,
}

impl AuthRuntime {
    pub fn config(&self) -> &AuthConfig {
        &self.config
    }

    pub fn provider_registry(&self) -> &ProviderRegistry {
        &self.provider_registry
    }

    pub fn authentication_service(&self) -> &AuthenticationService {
        &self.authentication_service
    }

    pub fn authorization_service(&self) -> &AuthorizationService {
        &self.authorization_service
    }

    pub fn enabled(&self) -> bool {
        self.config.authentication_enabled || self.config.authorization_enabled
    }

    /// Returns whether this runtime requires a detailed authorization result.
    ///
    /// Disabled authorization is an explicit compatibility boundary and is the
    /// only configuration state represented by `Optional` here. Authentication
    /// failures and authorization failures remain fail-closed layer failures.
    #[must_use]
    pub const fn detailed_authorization_requirement(&self) -> LayerRequirement {
        if self.config.authorization_enabled {
            LayerRequirement::Required
        } else {
            LayerRequirement::Optional
        }
    }

    pub async fn reload_acl_file(&self) -> RocketMQResult<usize> {
        Ok(load_configured_acl_file(
            &self.provider_registry,
            &self.config,
            self.service_context.metadata_io().clone(),
            true,
        )
        .await?
        .account_count)
    }

    pub fn is_acl_white_remote_address(
        &self,
        access_key: Option<&str>,
        source_ip: Option<&str>,
    ) -> RocketMQResult<bool> {
        self.provider_registry
            .is_acl_white_remote_address(access_key, source_ip)
    }

    pub fn acl_generation(&self) -> u64 {
        self.provider_registry.acl_generation()
    }

    pub fn invalidate_acl_cache(&self) -> u64 {
        self.provider_registry.advance_acl_generation()
    }

    pub fn acl_generation_counter(&self) -> Arc<AtomicU64> {
        self.provider_registry.acl_generation_counter()
    }

    pub fn metrics(&self) -> AuthMetrics {
        self.provider_registry.metrics()
    }

    pub fn metrics_snapshot(&self) -> AuthMetricsSnapshot {
        self.provider_registry.metrics_snapshot()
    }

    pub fn update_global_white_remote_addresses<I, S>(&self, addresses: I) -> RocketMQResult<u64>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.provider_registry.update_global_white_remote_addresses(addresses)
    }

    pub async fn shutdown(&self) -> RocketMQResult<()> {
        let _ = self.shutdown_with_report().await?;
        Ok(())
    }

    pub async fn shutdown_with_report(&self) -> RocketMQResult<Option<ShutdownReport>> {
        let report = self.service_context.task_group().shutdown(Duration::from_secs(5)).await;
        report
            .assert_no_task_leak()
            .map_err(|error| RocketMQError::auth_hot_reload_failed("authRuntime", error))?;
        Ok(Some(report))
    }

    pub fn acl_file_watcher_snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.acl_file_watch_handle
            .as_ref()
            .map(AclFileWatchHandle::snapshot)
            .unwrap_or_default()
    }

    pub async fn check_remoting(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
    ) -> RocketMQResult<()> {
        self.check_remoting_for_code(auth_context, command, command.code())
            .await
    }

    /// Checks a remoting command using the immutable operation captured at
    /// ingress rather than the processor-mutable command code.
    pub async fn check_remoting_for_code(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
        original_code: i32,
    ) -> RocketMQResult<()> {
        let mut authoritative_command = command.clone();
        authoritative_command.set_code_mut(original_code);
        self.check_remoting_with_source_ip(
            auth_context,
            &authoritative_command,
            auth_context.source_ip(),
            auth_context.channel_id(),
        )
        .await
    }

    pub async fn check_remoting_with_source_ip(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
        source_ip: Option<&str>,
        channel_id: Option<&str>,
    ) -> RocketMQResult<()> {
        auth_context.validate()?;
        if source_ip != auth_context.source_ip() || channel_id != auth_context.channel_id() {
            return Err(RocketMQError::authentication_failed(
                "remoting authentication metadata does not match its typed ingress context",
            ));
        }
        if self.is_acl_white_remote_address(access_key_from_command(command), source_ip)? {
            return Ok(());
        }

        self.authentication_service
            .authenticate_remoting(command, channel_id)
            .await?;
        self.authorization_service
            .authorize_remoting(auth_context, command)
            .await
    }

    /// Evaluates authentication and detailed authorization for a remoting request.
    ///
    /// The global ACL whitelist remains an allow decision. When authorization is
    /// disabled, the authorization service returns `Abstain`; callers must resolve
    /// that value through the explicitly selected layer requirement. Errors never
    /// become an abstention and deliberately carry no request or credential data.
    pub async fn evaluate_remoting_detailed(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
    ) -> LayerEvaluation<DetailedDecision> {
        self.evaluate_remoting_detailed_for_code(auth_context, command, command.code())
            .await
    }

    /// Evaluates remoting authentication and authorization against the
    /// immutable operation code captured by trusted ingress.
    pub async fn evaluate_remoting_detailed_for_code(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
        original_code: i32,
    ) -> LayerEvaluation<DetailedDecision> {
        let mut authoritative_command = command.clone();
        authoritative_command.set_code_mut(original_code);
        let command = &authoritative_command;
        auth_context.validate().map_err(|_| LayerFailureKind::Error)?;
        let is_whitelisted = self
            .is_acl_white_remote_address(access_key_from_command(command), auth_context.source_ip())
            .map_err(|_| LayerFailureKind::Error)?;
        if is_whitelisted {
            return Ok(DetailedDecision::Allow);
        }

        self.authentication_service
            .authenticate_remoting(command, auth_context.channel_id())
            .await
            .map_err(|_| LayerFailureKind::Error)?;
        self.authorization_service
            .authorize_remoting_detailed(auth_context, command)
            .await
    }

    /// Authenticates a privileged maintenance request and returns the verified
    /// identity consumed by the independent maintenance policy.
    ///
    /// Unlike the ordinary remoting path, this method never treats disabled
    /// authentication or an authentication whitelist as success.
    pub async fn authenticate_maintenance_principal(
        &self,
        command: &RemotingCommand,
        channel_id: Option<&str>,
    ) -> RocketMQResult<CheetahString> {
        self.authentication_service
            .authenticate_maintenance_principal(command, channel_id)
            .await
    }
}

#[derive(Clone)]
struct AclFileWatchHandle {
    scheduled_tasks: ScheduledTaskGroup,
}

impl AclFileWatchHandle {
    fn snapshot(&self) -> Vec<ScheduledTaskSnapshot> {
        self.scheduled_tasks.snapshot()
    }
}

#[derive(Clone)]
pub struct AuthenticationService {
    config: AuthConfig,
    whitelist: HashSet<String>,
    provider: Arc<DefaultAuthenticationProvider>,
    builder: DefaultAuthenticationContextBuilder,
    metrics: AuthMetrics,
}

impl AuthenticationService {
    fn new(config: AuthConfig, provider: Arc<DefaultAuthenticationProvider>, metrics: AuthMetrics) -> Self {
        Self {
            whitelist: parse_whitelist(config.authentication_whitelist.as_str()),
            config,
            provider,
            builder: DefaultAuthenticationContextBuilder::new(),
            metrics,
        }
    }

    pub async fn authenticate_remoting(
        &self,
        command: &RemotingCommand,
        channel_id: Option<&str>,
    ) -> RocketMQResult<()> {
        if !self.config.authentication_enabled {
            return Ok(());
        }
        if self.whitelist.contains(&command.code().to_string()) {
            self.metrics.record_whitelist_check(true);
            return Ok(());
        }

        let context = self.builder.build_from_remoting(command, channel_id).map_err(|error| {
            self.metrics.record_authentication_result(false);
            RocketMQError::authentication_failed(error.to_string())
        })?;
        self.provider.authenticate(&context).await
    }

    /// Authenticates a privileged maintenance request without a bypass path.
    ///
    /// # Errors
    ///
    /// Returns an authentication error when authentication is disabled, the
    /// request has no canonical identity, the signed context is malformed, or
    /// credential verification fails.
    pub async fn authenticate_maintenance_principal(
        &self,
        command: &RemotingCommand,
        channel_id: Option<&str>,
    ) -> RocketMQResult<CheetahString> {
        if !self.config.authentication_enabled {
            return Err(RocketMQError::authentication_failed(
                "maintenance authentication is disabled",
            ));
        }
        let context = self.builder.build_from_remoting(command, channel_id).map_err(|error| {
            self.metrics.record_authentication_result(false);
            RocketMQError::authentication_failed(error.to_string())
        })?;
        let principal = context
            .username()
            .filter(|username| !username.trim().is_empty())
            .cloned()
            .ok_or_else(|| RocketMQError::authentication_failed("maintenance request is anonymous"))?;
        self.provider.authenticate(&context).await?;
        Ok(principal)
    }
}

#[derive(Clone)]
pub struct AuthorizationService {
    config: AuthConfig,
    whitelist: HashSet<String>,
    provider: Arc<DefaultAuthorizationProvider>,
    metrics: AuthMetrics,
}

impl AuthorizationService {
    fn new(config: AuthConfig, provider: Arc<DefaultAuthorizationProvider>, metrics: AuthMetrics) -> Self {
        Self {
            whitelist: parse_whitelist(config.authorization_whitelist.as_str()),
            config,
            provider,
            metrics,
        }
    }

    pub async fn authorize_remoting(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
    ) -> RocketMQResult<()> {
        auth_context.validate()?;
        if !self.config.authorization_enabled {
            return Ok(());
        }
        if self.whitelist.contains(&command.code().to_string()) {
            self.metrics.record_whitelist_check(true);
            return Ok(());
        }

        let contexts = self
            .provider
            .new_contexts_from_remoting_command(auth_context, command)
            .map_err(|error| {
                self.metrics.record_authorization_result(false);
                map_authorization_error(error)
            })?;

        for context in contexts {
            self.provider
                .authorize(&context)
                .await
                .map_err(map_authorization_error)?;
        }

        Ok(())
    }

    /// Evaluates remoting authorization using the layered detailed contract.
    ///
    /// This adapter leaves the legacy `authorize_remoting` API unchanged. A
    /// disabled authorization service explicitly abstains; policy denials remain
    /// denials, while provider and context errors retain a fail-closed failure kind.
    pub async fn authorize_remoting_detailed(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
    ) -> LayerEvaluation<DetailedDecision> {
        auth_context.validate().map_err(|_| LayerFailureKind::Error)?;
        if !self.config.authorization_enabled {
            return Ok(DetailedDecision::Abstain);
        }
        if self.whitelist.contains(&command.code().to_string()) {
            self.metrics.record_whitelist_check(true);
            return Ok(DetailedDecision::Allow);
        }

        let contexts = self
            .provider
            .new_contexts_from_remoting_command(auth_context, command)
            .map_err(|error| {
                self.metrics.record_authorization_result(false);
                project_authorization_error(&error)
            })?;

        for context in contexts {
            match project_authorization_result(self.provider.authorize(&context).await) {
                Ok(DetailedDecision::Allow) => {}
                Ok(DetailedDecision::Deny | DetailedDecision::Abstain) => {
                    return Ok(DetailedDecision::Deny);
                }
                Err(failure) => {
                    return Err(failure);
                }
            }
        }

        Ok(DetailedDecision::Allow)
    }
}

fn parse_whitelist(value: &str) -> HashSet<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AclReloadResult {
    account_count: usize,
    changed: bool,
}

async fn load_configured_acl_file(
    provider_registry: &ProviderRegistry,
    config: &AuthConfig,
    blocking: rocketmq_runtime::BlockingExecutor,
    force: bool,
) -> RocketMQResult<AclReloadResult> {
    let acl_file = config.acl_file.as_str().trim();
    if acl_file.is_empty() {
        return Ok(AclReloadResult {
            account_count: 0,
            changed: false,
        });
    }

    let metrics = provider_registry.metrics();
    metrics.record_acl_reload_attempt();
    let result = async {
        let loader = FileAclConfigLoader::new(acl_file.to_owned(), blocking);
        let (acl_config, fingerprint) = loader.load_with_fingerprint().await?;
        if !force && provider_registry.acl_fingerprint()? == Some(fingerprint) {
            return Ok(AclReloadResult {
                account_count: 0,
                changed: false,
            });
        }

        let count = apply_acl_config(provider_registry, &acl_config).await?;
        provider_registry.set_acl_fingerprint(Some(fingerprint))?;
        Ok(AclReloadResult {
            account_count: count,
            changed: true,
        })
    }
    .await;

    match &result {
        Ok(reload_result) if reload_result.changed => metrics.record_acl_reload_success(),
        Ok(_) => metrics.record_acl_reload_skipped(),
        Err(_) => metrics.record_acl_reload_failure(),
    }

    result
}

async fn migrate_auth_from_v1(provider_registry: &ProviderRegistry, config: &AuthConfig) -> RocketMQResult<usize> {
    if !config.migrate_auth_from_v1_enabled {
        return Ok(0);
    }
    let plain_permission_manager = PlainPermissionManager::new();
    migrate_auth_from_v1_manager(provider_registry, &plain_permission_manager).await
}

async fn migrate_auth_from_v1_manager(
    provider_registry: &ProviderRegistry,
    plain_permission_manager: &PlainPermissionManager,
) -> RocketMQResult<usize> {
    let acl_config = plain_permission_manager.get_all_acl_config()?;
    apply_acl_config(provider_registry, &acl_config).await
}

fn start_acl_file_watcher(
    config: &AuthConfig,
    provider_registry: ProviderRegistry,
    service_context: &ChildServiceContext,
) -> Option<AclFileWatchHandle> {
    let acl_file = config.acl_file.as_str().trim();
    if acl_file.is_empty() || !config.acl_file_watch_enabled {
        return None;
    }

    let watch_config = config.clone();
    let interval = Duration::from_millis(config.acl_file_watch_interval_millis.max(1));
    let watcher_context = service_context.component("auth.acl-file-watcher");
    let task_group = watcher_context.task_group().clone();
    let scheduled_tasks = ScheduledTaskGroup::new(task_group.clone());
    let blocking = service_context.metadata_io().clone();
    if let Err(error) = scheduled_tasks.schedule_fixed_rate_no_overlap(
        ScheduledTaskConfig::fixed_rate_no_overlap("auth.acl-file-watcher.reload", interval),
        move || {
            let provider_registry = provider_registry.clone();
            let watch_config = watch_config.clone();
            let blocking = blocking.clone();
            async move {
                match load_configured_acl_file(&provider_registry, &watch_config, blocking, false).await {
                    Ok(result) if result.changed => {
                        debug!(
                            "Reloaded {} ACL account(s) from configured ACL file",
                            result.account_count
                        )
                    }
                    Ok(_) => debug!("ACL file unchanged; skipped reload"),
                    Err(error) => warn!("Failed to reload ACL file: {error}"),
                }
            }
        },
    ) {
        warn!("Failed to spawn ACL file watcher: {error}");
        return None;
    }

    Some(AclFileWatchHandle { scheduled_tasks })
}

async fn apply_acl_config(provider_registry: &ProviderRegistry, acl_config: &AclConfig) -> RocketMQResult<usize> {
    let prepared_config = prepare_acl_config(acl_config)?;
    let previous_access_keys = provider_registry.acl_managed_access_keys()?;
    let authn_provider = provider_registry.authentication_metadata_provider();
    let authz_provider = provider_registry.authorization_metadata_provider();

    for prepared_account in &prepared_config.accounts {
        upsert_user(authn_provider.clone(), prepared_account.user.clone()).await?;
        match &prepared_account.acl {
            Some(acl) => upsert_acl(authz_provider.clone(), &prepared_account.user, acl.clone()).await?,
            None => authz_provider
                .delete_acl(&prepared_account.user)
                .await
                .map_err(map_authorization_error)?,
        }
    }

    for stale_access_key in previous_access_keys.difference(&prepared_config.access_keys) {
        let user = User::of(stale_access_key.as_str());
        authz_provider
            .delete_acl(&user)
            .await
            .map_err(map_authorization_error)?;
        authn_provider.delete_user(stale_access_key).await?;
    }

    provider_registry.set_acl_white_list_snapshot(prepared_config.white_list_snapshot)?;
    provider_registry.set_acl_managed_access_keys(prepared_config.access_keys)?;
    provider_registry.advance_acl_generation();

    Ok(prepared_config.accounts_len)
}

struct PreparedAclConfig {
    white_list_snapshot: WhiteList,
    access_keys: HashSet<String>,
    accounts: Vec<PreparedAclAccount>,
    accounts_len: usize,
}

struct PreparedAclAccount {
    user: User,
    acl: Option<Acl>,
}

fn prepare_acl_config(acl_config: &AclConfig) -> RocketMQResult<PreparedAclConfig> {
    let mut access_keys = HashSet::new();
    let mut accounts = Vec::new();

    if let Some(plain_accounts) = acl_config.plain_access_configs() {
        for account in plain_accounts {
            let access_key = required_plain_field(account.access_key(), "accessKey", "<missing>")?.to_owned();
            let user = user_from_plain_account(account)?;
            let acl = acl_from_plain_account(account)?;
            access_keys.insert(access_key);
            accounts.push(PreparedAclAccount { user, acl });
        }
    }

    let accounts_len = accounts.len();
    Ok(PreparedAclConfig {
        white_list_snapshot: WhiteList::from_acl_config(acl_config),
        access_keys,
        accounts,
        accounts_len,
    })
}

async fn upsert_user(provider: Arc<LocalAuthenticationMetadataProvider>, user: User) -> RocketMQResult<()> {
    let username = user.username().to_string();
    if provider.get_user(username.as_str()).await.is_ok() {
        provider.update_user(user).await
    } else {
        provider.create_user(user).await
    }
}

async fn upsert_acl(provider: Arc<LocalAuthorizationMetadataProvider>, user: &User, acl: Acl) -> RocketMQResult<()> {
    match provider.get_acl(user).await.map_err(map_authorization_error)? {
        Some(_) => provider.update_acl(acl).await.map_err(map_authorization_error),
        None => provider.create_acl(acl).await.map_err(map_authorization_error),
    }
}

fn user_from_plain_account(account: &PlainAccessConfig) -> RocketMQResult<User> {
    let access_key = required_plain_field(account.access_key(), "accessKey", "<missing>")?;
    let secret_key = required_plain_field(account.secret_key(), "secretKey", access_key)?;
    let user_type = if account.is_admin() {
        UserType::Super
    } else {
        UserType::Normal
    };
    let mut user = User::of_with_type(access_key, secret_key, user_type);
    user.set_user_status(UserStatus::Enable);
    Ok(user)
}

fn acl_from_plain_account(account: &PlainAccessConfig) -> RocketMQResult<Option<Acl>> {
    let access_key = required_plain_field(account.access_key(), "accessKey", "<missing>")?;
    let mut policies = Vec::new();
    let mut default_entries = Vec::new();
    let mut custom_entries = Vec::new();

    if let Some(permission) = account.default_topic_perm() {
        push_default_entry(&mut default_entries, ResourceType::Topic, permission.as_str());
    }
    if let Some(permission) = account.default_group_perm() {
        push_default_entry(&mut default_entries, ResourceType::Group, permission.as_str());
    }
    if let Some(topic_perms) = account.topic_perms() {
        push_named_entries(&mut custom_entries, ResourceType::Topic, topic_perms);
    }
    if let Some(group_perms) = account.group_perms() {
        push_named_entries(&mut custom_entries, ResourceType::Group, group_perms);
    }
    if let Some(permission) = account.cluster_perm() {
        push_cluster_entry(&mut default_entries, permission.as_str())?;
    }

    if !custom_entries.is_empty() {
        policies.push(Policy::of_entries(PolicyType::Custom, custom_entries));
    }
    if !default_entries.is_empty() {
        policies.push(Policy::of_entries(PolicyType::Default, default_entries));
    }

    if policies.is_empty() {
        return Ok(None);
    }

    Ok(Some(Acl::of_with_policies(
        access_key.to_owned(),
        SubjectType::User,
        policies,
    )))
}

fn push_cluster_entry(entries: &mut Vec<PolicyEntry>, permission: &str) -> RocketMQResult<()> {
    let permission = permission.trim();
    let (actions, decision) = match permission {
        "GET" => (
            vec![rocketmq_security_api::Action::Get],
            crate::authorization::enums::decision::Decision::Allow,
        ),
        "DENY" => (
            vec![rocketmq_security_api::Action::All],
            crate::authorization::enums::decision::Decision::Deny,
        ),
        _ => {
            return Err(RocketMQError::ConfigInvalidValue {
                key: "aclConfig",
                value: "clusterPerm=<redacted>".to_string(),
                reason: "clusterPerm must be GET or DENY".to_string(),
            });
        }
    };
    entries.push(PolicyEntry::of(
        Resource::of(ResourceType::Cluster, None, ResourcePattern::Any),
        actions,
        None,
        decision,
    ));
    Ok(())
}

fn push_default_entry(entries: &mut Vec<PolicyEntry>, resource_type: ResourceType, permission: &str) {
    let (actions, decision) = Permission::migration_actions_and_decision(Some(permission));
    entries.push(PolicyEntry::of(
        Resource::of(resource_type, None, ResourcePattern::Any),
        actions,
        None,
        decision,
    ));
}

fn push_named_entries(
    entries: &mut Vec<PolicyEntry>,
    resource_type: ResourceType,
    permissions: &[cheetah_string::CheetahString],
) {
    for permission_entry in permissions {
        let raw = permission_entry.as_str().trim();
        if raw.is_empty() {
            continue;
        }
        let Some((resource_name, permission)) = raw.split_once('=') else {
            warn!("Skipping ACL policy entry without permission assignment: {raw}");
            continue;
        };
        let resource_name = resource_name.trim();
        if resource_name.is_empty() {
            warn!("Skipping ACL policy entry with blank resource name");
            continue;
        }

        let resource = match resource_type {
            ResourceType::Topic => Resource::of_topic(resource_name),
            ResourceType::Group => Resource::of_group(resource_name.to_owned()),
            _ => Resource::of(resource_type, Some(resource_name.to_owned()), ResourcePattern::Literal),
        };
        let (actions, decision) = Permission::migration_actions_and_decision(Some(permission));
        entries.push(PolicyEntry::of(resource, actions, None, decision));
    }
}

fn required_plain_field<'a>(
    value: Option<&'a cheetah_string::CheetahString>,
    field_name: &'static str,
    access_key: &str,
) -> RocketMQResult<&'a str> {
    value
        .map(|value| value.as_str().trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RocketMQError::ConfigInvalidValue {
            key: "aclConfig",
            value: format!("account={access_key}"),
            reason: format!("{field_name} must not be blank"),
        })
}

async fn seed_initial_users(provider_registry: &ProviderRegistry, config: &AuthConfig) -> RocketMQResult<()> {
    seed_init_authentication_user(provider_registry.authentication_metadata_provider(), config).await?;
    seed_inner_client_user(provider_registry.authentication_metadata_provider(), config).await
}

async fn seed_init_authentication_user(
    provider: Arc<LocalAuthenticationMetadataProvider>,
    config: &AuthConfig,
) -> RocketMQResult<()> {
    let init_user = config.init_authentication_user.as_str().trim();
    if init_user.is_empty() {
        return Ok(());
    }

    if init_user.starts_with('{') {
        if let Ok(mut user) = serde_json::from_str::<User>(init_user) {
            user.set_user_type(UserType::Super);
            if user.user_status().is_none() {
                user.set_user_status(UserStatus::Enable);
            }
            return create_user_if_absent(provider, user).await;
        }
    }

    let parts: Vec<&str> = init_user.splitn(2, ':').collect();
    if parts.len() == 2 {
        let username = parts[0].trim();
        let password = parts[1].trim();
        if !username.is_empty() && !password.is_empty() {
            let mut user = User::of_with_type(username, password, UserType::Super);
            user.set_user_status(UserStatus::Enable);
            return create_user_if_absent(provider, user).await;
        }
    }

    Ok(())
}

async fn seed_inner_client_user(
    provider: Arc<LocalAuthenticationMetadataProvider>,
    config: &AuthConfig,
) -> RocketMQResult<()> {
    #[derive(serde::Deserialize)]
    struct SessionCredentials {
        #[serde(rename = "accessKey")]
        access_key: String,
        #[serde(rename = "secretKey")]
        secret_key: String,
    }

    let credentials = config.inner_client_authentication_credentials.as_str().trim();
    if credentials.is_empty() {
        return Ok(());
    }

    let Ok(credentials) = serde_json::from_str::<SessionCredentials>(credentials) else {
        return Ok(());
    };

    let mut user = User::of_with_type(credentials.access_key, credentials.secret_key, UserType::Super);
    user.set_user_status(UserStatus::Enable);
    create_user_if_absent(provider, user).await
}

async fn create_user_if_absent(provider: Arc<LocalAuthenticationMetadataProvider>, user: User) -> RocketMQResult<()> {
    if provider.get_user(user.username().as_str()).await.is_ok() {
        return Ok(());
    }
    provider.create_user(user).await
}

fn map_authorization_error(error: AuthorizationError) -> RocketMQError {
    RocketMQError::from(error)
}

fn access_key_from_command(command: &RemotingCommand) -> Option<&str> {
    command.ext_fields().and_then(|fields| {
        fields
            .get(&CheetahString::from_static_str(ACCESS_KEY))
            .map(|value| value.as_str().trim())
            .filter(|value| !value.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::time::Duration;
    use std::time::Instant;

    use cheetah_string::CheetahString;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_security_api::Action;
    use tempfile::TempDir;
    use tokio::time::sleep;

    use super::*;
    use crate::authentication::acl_signer;
    use crate::authentication::enums::subject_type::SubjectType;
    use crate::authorization::enums::decision::Decision;
    use crate::authorization::enums::policy_type::PolicyType;
    use crate::authorization::model::acl::Acl;
    use crate::authorization::model::policy::Policy;
    use crate::authorization::model::resource::Resource;

    struct AuthRuntimeBuilder;

    impl AuthRuntimeBuilder {
        #[allow(
            clippy::new_ret_no_self,
            reason = "test facade preserves concise existing builder call sites"
        )]
        fn new(config: AuthConfig) -> super::AuthRuntimeBuilder {
            let runtime = rocketmq_runtime::RuntimeContext::from_current("auth-runtime-test");
            super::AuthRuntimeBuilder::new(config, runtime.service_context("auth-runtime"))
        }
    }

    fn send_message_command(topic: &str, access_key: &str, signature: &str) -> RemotingCommand {
        let mut ext_fields = HashMap::new();
        ext_fields.insert(CheetahString::from_static_str("topic"), CheetahString::from(topic));
        ext_fields.insert(
            CheetahString::from_static_str("AccessKey"),
            CheetahString::from(access_key),
        );
        ext_fields.insert(
            CheetahString::from_static_str("Signature"),
            CheetahString::from(signature),
        );
        RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32()).set_ext_fields(ext_fields)
    }

    fn send_message_command_without_credentials(topic: &str) -> RemotingCommand {
        let mut ext_fields = HashMap::new();
        ext_fields.insert(CheetahString::from_static_str("topic"), CheetahString::from(topic));
        RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32()).set_ext_fields(ext_fields)
    }

    fn signed_command(
        request_code: RequestCode,
        access_key: &str,
        secret_key: &str,
        fields: &[(&str, &str)],
    ) -> RemotingCommand {
        let mut ext_fields = HashMap::new();
        ext_fields.insert(
            CheetahString::from_static_str("AccessKey"),
            CheetahString::from(access_key),
        );
        let mut signed_values = std::collections::BTreeMap::new();
        signed_values.insert("AccessKey", access_key);
        for (name, value) in fields {
            ext_fields.insert(CheetahString::from(*name), CheetahString::from(*value));
            signed_values.insert(*name, *value);
        }
        let content = signed_values.values().copied().collect::<String>();
        let signature = acl_signer::cal_signature(content.as_bytes(), secret_key).unwrap();
        ext_fields.insert(
            CheetahString::from_static_str("Signature"),
            CheetahString::from(signature),
        );
        RemotingCommand::create_remoting_command(request_code.to_i32()).set_ext_fields(ext_fields)
    }

    #[test]
    fn provider_registry_rejects_unsupported_metadata_provider() {
        let config = AuthConfig {
            authentication_metadata_provider: CheetahString::from_static_str("RocksDBAuthenticationMetadataProvider"),
            ..AuthConfig::default()
        };

        let error = match ProviderRegistry::local(&config) {
            Ok(_) => panic!("unsupported provider should fail fast"),
            Err(error) => error,
        };

        assert!(
            matches!(error, RocketMQError::AuthConfigInvalid { key, .. } if key == "authenticationMetadataProvider")
        );
        assert!(error.to_string().contains("unsupported metadata provider"));
    }

    #[test]
    fn provider_registry_accepts_file_snapshot_metadata_provider_aliases() {
        let config = AuthConfig {
            authentication_metadata_provider: CheetahString::from_static_str(
                "FileSnapshotAuthenticationMetadataProvider",
            ),
            authorization_metadata_provider: CheetahString::from_static_str(
                "FileSnapshotAuthorizationMetadataProvider",
            ),
            ..AuthConfig::default()
        };

        let registry = ProviderRegistry::local(&config).expect("file snapshot aliases should use local providers");

        assert_eq!(registry.acl_generation(), 0);
    }

    #[test]
    fn migrated_sre_reader_can_get_but_cannot_mutate_cluster_resources() {
        let mut account = PlainAccessConfig::new();
        account.set_access_key(CheetahString::from_static_str("sre-reader"));
        account.set_cluster_perm(CheetahString::from_static_str("GET"));
        account.set_default_topic_perm(CheetahString::from_static_str("GET"));
        account.set_default_group_perm(CheetahString::from_static_str("GET"));

        let acl = acl_from_plain_account(&account).unwrap().unwrap();
        let entries = acl.get_policy(PolicyType::Default).unwrap().entries();

        for resource_type in [ResourceType::Cluster, ResourceType::Topic, ResourceType::Group] {
            let entry = entries
                .iter()
                .find(|entry| entry.resource().resource_type() == resource_type)
                .unwrap();
            assert_eq!(entry.decision(), Decision::Allow);
            assert!(entry.is_match_action(&[Action::Get]));
            assert!(!entry.is_match_action(&[Action::Update]));
            assert!(!entry.is_match_action(&[Action::Delete]));
        }
    }

    #[test]
    fn migrated_cluster_permission_rejects_non_read_permissions() {
        let mut account = PlainAccessConfig::new();
        account.set_access_key(CheetahString::from_static_str("sre-reader"));
        account.set_cluster_perm(CheetahString::from_static_str("UPDATE"));

        let error = acl_from_plain_account(&account).unwrap_err();

        assert!(matches!(error, RocketMQError::ConfigInvalidValue { .. }));
        assert!(error.to_string().contains("clusterPerm must be GET or DENY"));
    }

    #[tokio::test]
    async fn migrated_sre_reader_accepts_broker_get_and_rejects_topic_mutation_rpc() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: sre-reader
    secretKey: reader-secret
    admin: false
    defaultTopicPerm: GET
    defaultGroupPerm: GET
    clusterPerm: GET
"#,
        )
        .unwrap();
        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            cluster_name: CheetahString::from_static_str("SreDev"),
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            authentication_enabled: true,
            authorization_enabled: true,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        let broker_get = signed_command(RequestCode::GetBrokerRuntimeInfo, "sre-reader", "reader-secret", &[]);
        runtime
            .check_remoting(&RemotingAuthContext::embedded("test-session"), &broker_get)
            .await
            .unwrap();

        let topic_mutation = signed_command(
            RequestCode::UpdateAndCreateTopic,
            "sre-reader",
            "reader-secret",
            &[("topic", "Orders")],
        );
        let error = runtime
            .check_remoting(&RemotingAuthContext::embedded("test-session"), &topic_mutation)
            .await
            .unwrap_err();
        assert!(
            matches!(error, RocketMQError::BrokerPermissionDenied { .. }),
            "unexpected mutation denial error: {error:?}"
        );

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn build_runtime_seeds_initial_super_user() {
        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            init_authentication_user: CheetahString::from_static_str("admin:secret"),
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        let user = runtime
            .provider_registry()
            .authentication_metadata_provider()
            .get_user("admin")
            .await
            .unwrap();
        assert_eq!(user.user_type(), Some(UserType::Super));
        assert_eq!(user.user_status(), Some(UserStatus::Enable));
    }

    #[tokio::test]
    async fn runtime_authenticates_and_authorizes_remoting_command() {
        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            authentication_enabled: true,
            authorization_enabled: true,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        let mut user = User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        authn_provider.create_user(user).await.unwrap();

        let authz_provider = runtime.provider_registry().authorization_metadata_provider();
        let acl = Acl::of(
            "alice",
            SubjectType::User,
            Policy::of(
                vec![Resource::of_topic("topic-a")],
                vec![Action::Pub],
                None,
                Decision::Allow,
            ),
        );
        authz_provider.create_acl(acl).await.unwrap();

        let signature = acl_signer::cal_signature("alicetopic-a".as_bytes(), "secret").unwrap();
        let command = send_message_command("topic-a", "alice", &signature);

        runtime
            .check_remoting(&RemotingAuthContext::embedded("test-session"), &command)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn remoting_runtime_rejects_missing_or_mismatched_ingress_metadata() {
        let runtime = AuthRuntimeBuilder::new(AuthConfig::default()).build().await.unwrap();
        let command = RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32());

        runtime
            .check_remoting(&RemotingAuthContext::default(), &command)
            .await
            .expect_err("missing session and source metadata must fail closed");

        let context = RemotingAuthContext::network("192.0.2.10", "session-a");
        runtime
            .check_remoting_with_source_ip(&context, &command, Some("192.0.2.11"), Some("session-a"))
            .await
            .expect_err("metadata overrides must match the typed ingress context");
    }

    #[tokio::test]
    async fn authoritative_operation_code_cannot_be_replaced_by_a_mutated_command_code() {
        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            authentication_enabled: true,
            authentication_whitelist: CheetahString::from(RequestCode::HeartBeat.to_i32().to_string()),
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();
        let context = RemotingAuthContext::embedded("test-session");
        let mutated = RemotingCommand::create_remoting_command(RequestCode::HeartBeat.to_i32());

        runtime
            .check_remoting(&context, &mutated)
            .await
            .expect("the test command code is explicitly whitelisted");
        runtime
            .check_remoting_for_code(&context, &mutated, RequestCode::SendMessage.to_i32())
            .await
            .expect_err("the immutable SendMessage operation must still require authentication");
    }

    #[tokio::test]
    async fn detailed_authorization_records_each_provider_decision_once() {
        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            authentication_enabled: true,
            authorization_enabled: true,
            ..AuthConfig::default()
        })
        .build()
        .await
        .expect("test auth runtime should initialize");

        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        let mut user = User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        authn_provider
            .create_user(user)
            .await
            .expect("test user should be stored");

        let authz_provider = runtime.provider_registry().authorization_metadata_provider();
        authz_provider
            .create_acl(Acl::of(
                "alice",
                SubjectType::User,
                Policy::of(
                    vec![Resource::of_topic("topic-a")],
                    vec![Action::Pub],
                    None,
                    Decision::Allow,
                ),
            ))
            .await
            .expect("test ACL should be stored");

        let command = send_message_command(
            "topic-a",
            "alice",
            &acl_signer::cal_signature("alicetopic-a".as_bytes(), "secret")
                .expect("test signature should be generated"),
        );
        assert_eq!(
            runtime
                .evaluate_remoting_detailed(&RemotingAuthContext::embedded("test-session"), &command)
                .await,
            Ok(DetailedDecision::Allow)
        );

        let snapshot = runtime.metrics_snapshot();
        assert_eq!(snapshot.authorization_successes, 1);
        assert_eq!(snapshot.authorization_failures, 0);
        runtime.shutdown().await.expect("test auth runtime should shut down");
    }

    #[tokio::test]
    async fn build_runtime_loads_configured_acl_file() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: secret
    admin: false
    defaultTopicPerm: DENY
    defaultGroupPerm: SUB
    topicPerms:
      - TopicA=PUB
    groupPerms:
      - GroupA=SUB
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        let user = authn_provider.get_user("alice").await.unwrap();
        assert_eq!(user.password().map(|value| value.as_str()), Some("secret"));
        assert_eq!(user.user_type(), Some(UserType::Normal));
        assert_eq!(user.user_status(), Some(UserStatus::Enable));

        let authz_provider = runtime.provider_registry().authorization_metadata_provider();
        let acl = authz_provider.get_acl(&User::of("alice")).await.unwrap().unwrap();
        assert!(acl.get_policy(PolicyType::Custom).is_some());
        assert!(acl.get_policy(PolicyType::Default).is_some());
    }

    #[tokio::test]
    async fn migrate_auth_from_v1_manager_loads_legacy_plain_acl_files() {
        let temp = TempDir::new().unwrap();
        let auth_config = AuthConfig {
            auth_config_path: CheetahString::from(temp.path().join("auth-store").to_string_lossy().as_ref()),
            ..AuthConfig::default()
        };
        let context = rocketmq_runtime::RuntimeContext::try_from_current("auth-v1-migration-test").unwrap();
        let metadata_io = MetadataIoConfig::default()
            .into_plan()
            .expect("default metadata I/O config is valid")
            .start(&context.service_context("auth.v1-migration"))
            .unwrap();
        let registry = ProviderRegistry::local_with_metadata_io(&auth_config, Some(metadata_io)).unwrap();
        let acl_file = temp.path().join("conf").join("acl").join("legacy.yml");
        fs::create_dir_all(acl_file.parent().unwrap()).unwrap();
        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 10.1.*.*
accounts:
  - accessKey: legacy
    secretKey: secret
    admin: false
    topicPerms:
      - TopicA=PUB
"#,
        )
        .unwrap();
        let manager = PlainPermissionManager {
            file_home: temp.path().to_string_lossy().into_owned(),
            default_acl_dir: temp.path().join("conf").join("acl").to_string_lossy().into_owned(),
            default_acl_file: temp
                .path()
                .join("conf")
                .join("plain_acl.yml")
                .to_string_lossy()
                .into_owned(),
            file_list: vec![acl_file.to_string_lossy().into_owned()],
        };

        let migrated = migrate_auth_from_v1_manager(&registry, &manager).await.unwrap();

        assert_eq!(migrated, 1);
        let user = registry
            .authentication_metadata_provider()
            .get_user("legacy")
            .await
            .unwrap();
        assert_eq!(user.password().map(|value| value.as_str()), Some("secret"));
        let acl = registry
            .authorization_metadata_provider()
            .get_acl(&User::of("legacy"))
            .await
            .unwrap()
            .unwrap();
        assert!(acl.get_policy(PolicyType::Custom).is_some());
        assert!(registry.is_acl_white_remote_address(None, Some("10.1.2.3")).unwrap());
    }

    #[tokio::test]
    async fn migrate_auth_from_v1_is_disabled_by_default() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from(temp.path().join("auth-store").to_string_lossy().as_ref()),
            ..AuthConfig::default()
        };
        let registry = ProviderRegistry::local(&config).unwrap();

        let migrated = migrate_auth_from_v1(&registry, &config).await.unwrap();

        assert_eq!(migrated, 0);
    }

    #[tokio::test]
    async fn acl_white_remote_address_short_circuits_remoting_auth_and_authorization() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: secret
    whiteRemoteAddress: 192.168.0.*
    defaultTopicPerm: DENY
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            authentication_enabled: true,
            authorization_enabled: true,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        assert!(runtime.is_acl_white_remote_address(None, Some("10.10.1.2")).unwrap());
        assert!(runtime
            .is_acl_white_remote_address(Some("alice"), Some("192.168.0.7"))
            .unwrap());
        assert!(!runtime
            .is_acl_white_remote_address(Some("alice"), Some("192.168.1.7"))
            .unwrap());

        let global_command = send_message_command_without_credentials("TopicA");
        runtime
            .check_remoting(
                &RemotingAuthContext::network("10.10.1.2", "test-session"),
                &global_command,
            )
            .await
            .unwrap();

        let account_command = send_message_command("TopicA", "alice", "");
        runtime
            .check_remoting(
                &RemotingAuthContext::network("192.168.0.7", "test-session"),
                &account_command,
            )
            .await
            .unwrap();

        runtime
            .check_remoting(
                &RemotingAuthContext::network("192.168.1.7", "test-session"),
                &account_command,
            )
            .await
            .expect_err("non-whitelisted source should still require a valid signature");
    }

    #[tokio::test]
    async fn runtime_global_white_remote_address_update_preserves_account_whitelist_and_advances_generation() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: secret
    whiteRemoteAddress: 192.168.0.*
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();
        let generation = runtime.acl_generation();

        runtime
            .update_global_white_remote_addresses(vec!["172.16.*.*"])
            .unwrap();

        assert!(runtime.acl_generation() > generation);
        assert!(!runtime.is_acl_white_remote_address(None, Some("10.10.1.2")).unwrap());
        assert!(runtime.is_acl_white_remote_address(None, Some("172.16.1.2")).unwrap());
        assert!(runtime
            .is_acl_white_remote_address(Some("alice"), Some("192.168.0.7"))
            .unwrap());
    }

    #[tokio::test]
    async fn provider_registry_local_loads_persisted_authentication_users() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from(temp.path().join("auth.json").to_string_lossy().as_ref()),
            ..AuthConfig::default()
        };

        let mut provider = LocalAuthenticationMetadataProvider::new();
        provider.initialize(config.clone(), None).await.unwrap();
        provider
            .create_user(User::of_with_password("persisted", "secret"))
            .await
            .unwrap();

        let registry = ProviderRegistry::local(&config).unwrap();
        let restored = registry
            .authentication_metadata_provider()
            .get_user("persisted")
            .await
            .unwrap();

        assert_eq!(restored.password().map(|value| value.as_str()), Some("secret"));
    }

    #[tokio::test]
    async fn auth_runtime_restores_persisted_metadata_after_restart() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from(temp.path().join("auth.json").to_string_lossy().as_ref()),
            authentication_enabled: true,
            authorization_enabled: true,
            ..AuthConfig::default()
        };

        let runtime = AuthRuntimeBuilder::new(config.clone()).build().await.unwrap();
        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        let mut user = User::of_with_type("alice", "secret", UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        authn_provider.create_user(user).await.unwrap();

        let authz_provider = runtime.provider_registry().authorization_metadata_provider();
        authz_provider
            .create_acl(Acl::of(
                "alice",
                SubjectType::User,
                Policy::of(
                    vec![Resource::of_topic("topic-a")],
                    vec![Action::Pub],
                    None,
                    Decision::Allow,
                ),
            ))
            .await
            .unwrap();
        runtime.shutdown().await.unwrap();

        let restarted = AuthRuntimeBuilder::new(config).build().await.unwrap();
        let signature = acl_signer::cal_signature("alicetopic-a".as_bytes(), "secret").unwrap();
        let command = send_message_command("topic-a", "alice", &signature);

        restarted
            .check_remoting(&RemotingAuthContext::embedded("test-session"), &command)
            .await
            .unwrap();
        restarted.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn auth_runtime_rejects_corrupted_persisted_users_snapshot() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from(temp.path().join("auth.json").to_string_lossy().as_ref()),
            authentication_enabled: true,
            ..AuthConfig::default()
        };
        let snapshot = temp.path().join("auth").join("users.json");
        fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        fs::write(&snapshot, b"{not valid json").unwrap();

        let error = match AuthRuntimeBuilder::new(config).build().await {
            Ok(_) => panic!("runtime must reject corrupted users snapshot"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("users.json"));
    }

    #[tokio::test]
    async fn auth_runtime_rejects_corrupted_persisted_acls_snapshot() {
        let temp = TempDir::new().unwrap();
        let config = AuthConfig {
            auth_config_path: CheetahString::from(temp.path().join("auth.json").to_string_lossy().as_ref()),
            authorization_enabled: true,
            ..AuthConfig::default()
        };
        let snapshot = temp.path().join("auth").join("acls.json");
        fs::create_dir_all(snapshot.parent().unwrap()).unwrap();
        fs::write(&snapshot, b"{not valid json").unwrap();

        let error = match AuthRuntimeBuilder::new(config).build().await {
            Ok(_) => panic!("runtime must reject corrupted ACL snapshot"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("acls.json"));
        assert!(matches!(error, RocketMQError::Serialization(_)));
    }

    #[tokio::test]
    async fn acl_file_watcher_reloads_changed_user_secret() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: first
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 25,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        let user = authn_provider.get_user("alice").await.unwrap();
        assert_eq!(user.password().map(|value| value.as_str()), Some("first"));

        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: second
"#,
        )
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let user = authn_provider.get_user("alice").await.unwrap();
            if user.password().map(|value| value.as_str()) == Some("second") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "ACL file watcher did not reload the updated user secret"
            );
            sleep(Duration::from_millis(25)).await;
        }

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn acl_file_watcher_skips_unchanged_file_without_advancing_generation() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: first
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 20,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();
        let generation = runtime.acl_generation();

        sleep(Duration::from_millis(120)).await;

        assert_eq!(
            runtime.acl_generation(),
            generation,
            "watcher must not advance generation when ACL file content is unchanged"
        );

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn manual_acl_reload_failure_preserves_previous_snapshot_and_generation() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: first
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();
        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        let generation = runtime.acl_generation();
        assert!(runtime.is_acl_white_remote_address(None, Some("10.10.1.2")).unwrap());
        let user = authn_provider.get_user("alice").await.unwrap();
        assert_eq!(user.password().map(|value| value.as_str()), Some("first"));

        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 172.16.*.*
accounts:
  - accessKey: alice
"#,
        )
        .unwrap();

        let error = runtime.reload_acl_file().await.unwrap_err();
        assert!(error.to_string().contains("secretKey must not be blank"));
        assert_eq!(runtime.acl_generation(), generation);

        let user = authn_provider.get_user("alice").await.unwrap();
        assert_eq!(user.password().map(|value| value.as_str()), Some("first"));
        assert!(runtime.is_acl_white_remote_address(None, Some("10.10.1.2")).unwrap());
        assert!(!runtime.is_acl_white_remote_address(None, Some("172.16.1.2")).unwrap());
    }

    #[tokio::test]
    async fn acl_file_watcher_failure_preserves_snapshot_and_recovers_after_fix() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: first
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 25,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();
        let authn_provider = runtime.provider_registry().authentication_metadata_provider();
        let generation = runtime.acl_generation();

        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 172.16.*.*
accounts:
  - accessKey: alice
"#,
        )
        .unwrap();

        sleep(Duration::from_millis(150)).await;

        assert_eq!(runtime.acl_generation(), generation);
        let user = authn_provider.get_user("alice").await.unwrap();
        assert_eq!(user.password().map(|value| value.as_str()), Some("first"));
        assert!(runtime.is_acl_white_remote_address(None, Some("10.10.1.2")).unwrap());
        assert!(!runtime.is_acl_white_remote_address(None, Some("172.16.1.2")).unwrap());

        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 172.16.*.*
accounts:
  - accessKey: alice
    secretKey: second
"#,
        )
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let user = authn_provider.get_user("alice").await.unwrap();
            if user.password().map(|value| value.as_str()) == Some("second")
                && runtime.is_acl_white_remote_address(None, Some("172.16.1.2")).unwrap()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "ACL file watcher did not recover after a failed reload"
            );
            sleep(Duration::from_millis(25)).await;
        }

        assert!(runtime.acl_generation() > generation);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn manual_acl_reload_updates_watcher_fingerprint() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: first
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 20,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: second
"#,
        )
        .unwrap();

        runtime.reload_acl_file().await.unwrap();
        let generation = runtime.acl_generation();

        sleep(Duration::from_millis(120)).await;

        assert_eq!(
            runtime.acl_generation(),
            generation,
            "watcher must not re-apply a file that was already manually reloaded"
        );

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn acl_file_watcher_reloads_white_remote_address_snapshot() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: first
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 25,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        assert!(!runtime.is_acl_white_remote_address(None, Some("10.10.1.2")).unwrap());

        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: first
"#,
        )
        .unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if runtime.is_acl_white_remote_address(None, Some("10.10.1.2")).unwrap() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "ACL file watcher did not reload the updated white remote address"
            );
            sleep(Duration::from_millis(25)).await;
        }

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn acl_file_watcher_shutdown_is_idempotent_and_stops_future_reloads() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: first
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            acl_file_watch_enabled: true,
            acl_file_watch_interval_millis: 20,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();
        runtime.shutdown().await.unwrap();
        runtime.shutdown().await.unwrap();

        let generation = runtime.acl_generation();
        let reload_attempts = runtime.metrics_snapshot().acl_reload_attempts;
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: second
"#,
        )
        .unwrap();
        sleep(Duration::from_millis(120)).await;

        assert_eq!(runtime.acl_generation(), generation);
        assert_eq!(
            runtime.metrics_snapshot().acl_reload_attempts,
            reload_attempts,
            "shutdown watcher must not poll or reload after shutdown",
        );
    }

    #[tokio::test]
    async fn auth_metrics_track_reload_whitelist_signature_and_auth_outcomes() {
        let temp = TempDir::new().unwrap();
        let acl_file = temp.path().join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: secret
    topicPerms:
      - TopicA=PUB
"#,
        )
        .unwrap();

        let runtime = AuthRuntimeBuilder::new(AuthConfig {
            acl_file: CheetahString::from(acl_file.to_string_lossy().as_ref()),
            authentication_enabled: true,
            authorization_enabled: true,
            ..AuthConfig::default()
        })
        .build()
        .await
        .unwrap();

        assert!(runtime.is_acl_white_remote_address(None, Some("10.10.1.2")).unwrap());

        let signature = acl_signer::cal_signature("aliceTopicA".as_bytes(), "secret").unwrap();
        let valid_command = send_message_command("TopicA", "alice", &signature);
        runtime
            .check_remoting(&RemotingAuthContext::network("127.0.0.1", "channel-a"), &valid_command)
            .await
            .unwrap();

        let invalid_command = send_message_command("TopicA", "alice", "bad-signature");
        runtime
            .check_remoting(
                &RemotingAuthContext::network("127.0.0.1", "channel-b"),
                &invalid_command,
            )
            .await
            .expect_err("invalid signature should fail authentication");

        let snapshot = runtime.metrics_snapshot();
        assert_eq!(snapshot.acl_reload_attempts, 1);
        assert_eq!(snapshot.acl_reload_successes, 1);
        assert!(snapshot.cache_invalidations >= 1);
        assert!(snapshot.whitelist_hits >= 1);
        assert!(snapshot.whitelist_misses >= 2);
        assert_eq!(snapshot.signature_successes, 1);
        assert_eq!(snapshot.signature_failures, 1);
        assert_eq!(snapshot.authentication_successes, 1);
        assert_eq!(snapshot.authentication_failures, 1);
        assert_eq!(snapshot.authorization_successes, 1);

        runtime.shutdown().await.unwrap();
    }
}
