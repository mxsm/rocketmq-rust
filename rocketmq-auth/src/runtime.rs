use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::resource::resource_pattern::ResourcePattern;
use rocketmq_common::common::resource::resource_type::ResourceType;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use tokio::sync::watch;
use tokio::task::JoinHandle;
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
use crate::observability::AuthMetrics;
use crate::observability::AuthMetricsSnapshot;
use crate::permission::Permission;

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
        let authentication_metadata_provider = Arc::new(LocalAuthenticationMetadataProvider::with_config(config)?);
        let mut authorization_metadata_provider = LocalAuthorizationMetadataProvider::new();
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
            .map_err(|_| RocketMQError::Internal("ACL white list snapshot lock is poisoned".to_owned()))?;
        *guard = snapshot;
        Ok(())
    }

    fn acl_managed_access_keys(&self) -> RocketMQResult<HashSet<String>> {
        let guard = self
            .acl_managed_access_keys
            .read()
            .map_err(|_| RocketMQError::Internal("ACL managed access key lock is poisoned".to_owned()))?;
        Ok(guard.clone())
    }

    fn set_acl_managed_access_keys(&self, access_keys: HashSet<String>) -> RocketMQResult<()> {
        let mut guard = self
            .acl_managed_access_keys
            .write()
            .map_err(|_| RocketMQError::Internal("ACL managed access key lock is poisoned".to_owned()))?;
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
            .map_err(|_| RocketMQError::Internal("ACL fingerprint lock is poisoned".to_owned()))?;
        Ok(*guard)
    }

    fn set_acl_fingerprint(&self, fingerprint: Option<AclConfigFingerprint>) -> RocketMQResult<()> {
        let mut guard = self
            .acl_fingerprint
            .write()
            .map_err(|_| RocketMQError::Internal("ACL fingerprint lock is poisoned".to_owned()))?;
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
            .map_err(|_| RocketMQError::Internal("ACL white list snapshot lock is poisoned".to_owned()))?;
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
                .map_err(|_| RocketMQError::Internal("ACL white list snapshot lock is poisoned".to_owned()))?;
            guard.with_global_patterns(addresses)
        };
        self.set_acl_white_list_snapshot(updated_snapshot)?;
        Ok(self.advance_acl_generation())
    }
}

pub struct AuthRuntimeBuilder {
    config: AuthConfig,
    provider_registry: Option<ProviderRegistry>,
}

impl AuthRuntimeBuilder {
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            provider_registry: None,
        }
    }

    pub fn with_provider_registry(mut self, provider_registry: ProviderRegistry) -> Self {
        self.provider_registry = Some(provider_registry);
        self
    }

    pub async fn build(self) -> RocketMQResult<AuthRuntime> {
        let provider_registry = match self.provider_registry {
            Some(provider_registry) => provider_registry,
            None => ProviderRegistry::local(&self.config)?,
        };

        seed_initial_users(&provider_registry, &self.config).await?;
        let loaded_acl_entries = load_configured_acl_file(&provider_registry, &self.config, true)
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
        let acl_file_watch_handle = start_acl_file_watcher(&self.config, provider_registry.clone());

        Ok(AuthRuntime {
            config: self.config,
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

    pub async fn reload_acl_file(&self) -> RocketMQResult<usize> {
        Ok(load_configured_acl_file(&self.provider_registry, &self.config, true)
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
        if let Some(handle) = &self.acl_file_watch_handle {
            handle.shutdown().await?;
        }
        Ok(())
    }

    pub async fn check_remoting(
        &self,
        channel_context: &(dyn std::any::Any + Send + Sync),
        command: &RemotingCommand,
    ) -> RocketMQResult<()> {
        let source_ip = source_ip_from_channel_context(channel_context);
        let channel_id = channel_id_from_channel_context(channel_context);
        self.check_remoting_with_source_ip(channel_context, command, source_ip.as_deref(), channel_id.as_deref())
            .await
    }

    pub async fn check_remoting_with_source_ip(
        &self,
        channel_context: &(dyn std::any::Any + Send + Sync),
        command: &RemotingCommand,
        source_ip: Option<&str>,
        channel_id: Option<&str>,
    ) -> RocketMQResult<()> {
        if self.is_acl_white_remote_address(access_key_from_command(command), source_ip)? {
            return Ok(());
        }

        self.authentication_service
            .authenticate_remoting(command, channel_id)
            .await?;
        self.authorization_service
            .authorize_remoting(channel_context, command)
            .await
    }
}

#[derive(Clone)]
struct AclFileWatchHandle {
    shutdown_tx: watch::Sender<bool>,
    join_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl AclFileWatchHandle {
    async fn shutdown(&self) -> RocketMQResult<()> {
        let _ = self.shutdown_tx.send(true);
        let join_handle = {
            let mut guard = self.join_handle.lock().await;
            guard.take()
        };
        if let Some(join_handle) = join_handle {
            join_handle
                .await
                .map_err(|error| RocketMQError::auth_hot_reload_failed("aclFile", error.to_string()))?;
        }
        Ok(())
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
        channel_context: &(dyn std::any::Any + Send + Sync),
        command: &RemotingCommand,
    ) -> RocketMQResult<()> {
        if !self.config.authorization_enabled {
            return Ok(());
        }
        if self.whitelist.contains(&command.code().to_string()) {
            self.metrics.record_whitelist_check(true);
            return Ok(());
        }

        let contexts = self
            .provider
            .new_contexts_from_remoting_command(channel_context, command)
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
        let loader = FileAclConfigLoader::new(acl_file.to_owned());
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

fn start_acl_file_watcher(config: &AuthConfig, provider_registry: ProviderRegistry) -> Option<AclFileWatchHandle> {
    let acl_file = config.acl_file.as_str().trim();
    if acl_file.is_empty() || !config.acl_file_watch_enabled {
        return None;
    }

    let watch_config = config.clone();
    let interval = Duration::from_millis(config.acl_file_watch_interval_millis.max(1));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let join_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        debug!("ACL file watcher stopped");
                        break;
                    }
                }
                _ = ticker.tick() => {
                    match load_configured_acl_file(&provider_registry, &watch_config, false).await {
                        Ok(result) if result.changed => {
                            debug!("Reloaded {} ACL account(s) from configured ACL file", result.account_count)
                        }
                        Ok(_) => debug!("ACL file unchanged; skipped reload"),
                        Err(error) => warn!("Failed to reload ACL file: {error}"),
                    }
                }
            }
        }
    });

    Some(AclFileWatchHandle {
        shutdown_tx,
        join_handle: Arc::new(tokio::sync::Mutex::new(Some(join_handle))),
    })
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

fn source_ip_from_channel_context(channel_context: &(dyn std::any::Any + Send + Sync)) -> Option<String> {
    if let Some(ctx) = channel_context.downcast_ref::<ConnectionHandlerContext>() {
        return Some(ctx.remote_address().ip().to_string());
    }
    if let Some(ctx) = channel_context.downcast_ref::<ConnectionHandlerContextWrapper>() {
        return Some(ctx.remote_address().ip().to_string());
    }
    if let Some(channel) = channel_context.downcast_ref::<Channel>() {
        return Some(channel.remote_address().ip().to_string());
    }
    None
}

fn channel_id_from_channel_context(channel_context: &(dyn std::any::Any + Send + Sync)) -> Option<String> {
    if let Some(ctx) = channel_context.downcast_ref::<ConnectionHandlerContext>() {
        return Some(ctx.channel().channel_id().to_owned());
    }
    if let Some(ctx) = channel_context.downcast_ref::<ConnectionHandlerContextWrapper>() {
        return Some(ctx.channel().channel_id().to_owned());
    }
    if let Some(channel) = channel_context.downcast_ref::<Channel>() {
        return Some(channel.channel_id().to_owned());
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::time::Duration;
    use std::time::Instant;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::action::Action;
    use rocketmq_remoting::code::request_code::RequestCode;
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

        runtime.check_remoting(&(), &command).await.unwrap();
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
            .check_remoting_with_source_ip(&(), &global_command, Some("10.10.1.2"), None)
            .await
            .unwrap();

        let account_command = send_message_command("TopicA", "alice", "");
        runtime
            .check_remoting_with_source_ip(&(), &account_command, Some("192.168.0.7"), None)
            .await
            .unwrap();

        runtime
            .check_remoting_with_source_ip(&(), &account_command, Some("192.168.1.7"), None)
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

        restarted.check_remoting(&(), &command).await.unwrap();
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
            .check_remoting_with_source_ip(&(), &valid_command, Some("127.0.0.1"), Some("channel-a"))
            .await
            .unwrap();

        let invalid_command = send_message_command("TopicA", "alice", "bad-signature");
        runtime
            .check_remoting_with_source_ip(&(), &invalid_command, Some("127.0.0.1"), Some("channel-b"))
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
