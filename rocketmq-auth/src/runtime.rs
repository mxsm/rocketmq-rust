use std::collections::HashMap;
use std::collections::HashSet;
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

use crate::acl::FileAclConfigLoader;
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
use crate::authorization::model::environment::source_ip_matches;
use crate::authorization::model::policy::Policy;
use crate::authorization::model::policy_entry::PolicyEntry;
use crate::authorization::model::resource::Resource;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationProvider;
use crate::authorization::provider::DefaultAuthorizationProvider;
use crate::config::AuthConfig;
use crate::migration::alc::acl_config::AclConfig;
use crate::migration::alc::plain_access_config::PlainAccessConfig;
use crate::permission::Permission;

const ACCESS_KEY: &str = "AccessKey";

#[derive(Clone)]
pub struct ProviderRegistry {
    authentication_metadata_provider: Arc<LocalAuthenticationMetadataProvider>,
    authorization_metadata_provider: Arc<LocalAuthorizationMetadataProvider>,
    acl_white_list_snapshot: Arc<RwLock<AclWhiteListSnapshot>>,
}

impl ProviderRegistry {
    pub fn local(config: &AuthConfig) -> RocketMQResult<Self> {
        let authentication_metadata_provider = Arc::new(LocalAuthenticationMetadataProvider::new());
        let mut authorization_metadata_provider = LocalAuthorizationMetadataProvider::new();
        authorization_metadata_provider
            .initialize(config.clone(), None)
            .map_err(map_authorization_error)?;

        Ok(Self {
            authentication_metadata_provider,
            authorization_metadata_provider: Arc::new(authorization_metadata_provider),
            acl_white_list_snapshot: Arc::new(RwLock::new(AclWhiteListSnapshot::default())),
        })
    }

    pub fn authentication_metadata_provider(&self) -> Arc<LocalAuthenticationMetadataProvider> {
        self.authentication_metadata_provider.clone()
    }

    pub fn authorization_metadata_provider(&self) -> Arc<LocalAuthorizationMetadataProvider> {
        self.authorization_metadata_provider.clone()
    }

    fn set_acl_white_list_snapshot(&self, snapshot: AclWhiteListSnapshot) -> RocketMQResult<()> {
        let mut guard = self
            .acl_white_list_snapshot
            .write()
            .map_err(|_| RocketMQError::Internal("ACL white list snapshot lock is poisoned".to_owned()))?;
        *guard = snapshot;
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
        Ok(guard.matches(access_key, source_ip))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct AclWhiteListSnapshot {
    global_white_remote_addresses: Vec<String>,
    account_white_remote_addresses: HashMap<String, String>,
}

impl AclWhiteListSnapshot {
    fn from_acl_config(acl_config: &AclConfig) -> Self {
        let global_white_remote_addresses = acl_config
            .global_white_addrs()
            .unwrap_or_default()
            .iter()
            .map(|value| value.as_str().trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect();

        let account_white_remote_addresses = acl_config
            .plain_access_configs()
            .unwrap_or_default()
            .iter()
            .filter_map(|account| {
                let access_key = account.access_key()?.as_str().trim();
                let white_remote_address = account.white_remote_address()?.as_str().trim();
                if access_key.is_empty() || white_remote_address.is_empty() {
                    return None;
                }
                Some((access_key.to_owned(), white_remote_address.to_owned()))
            })
            .collect();

        Self {
            global_white_remote_addresses,
            account_white_remote_addresses,
        }
    }

    fn matches(&self, access_key: Option<&str>, source_ip: Option<&str>) -> bool {
        let Some(source_ip) = valid_source_ip(source_ip) else {
            return false;
        };

        if self
            .global_white_remote_addresses
            .iter()
            .any(|pattern| source_ip_matches(pattern, source_ip))
        {
            return true;
        }

        let Some(access_key) = access_key.map(str::trim).filter(|value| !value.is_empty()) else {
            return false;
        };

        self.account_white_remote_addresses
            .get(access_key)
            .is_some_and(|pattern| source_ip_matches(pattern, source_ip))
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
        let loaded_acl_entries = load_configured_acl_file(&provider_registry, &self.config).await?;
        if loaded_acl_entries > 0 {
            info!("Loaded {} ACL account(s) from configured ACL file", loaded_acl_entries);
        }

        let mut authentication_provider = DefaultAuthenticationProvider::new();
        authentication_provider.initialize_with_registry(self.config.clone(), provider_registry.clone())?;

        let mut authorization_provider = DefaultAuthorizationProvider::new();
        authorization_provider
            .initialize_with_registry(self.config.clone(), provider_registry.clone())
            .map_err(map_authorization_error)?;

        let authentication_service = AuthenticationService::new(self.config.clone(), Arc::new(authentication_provider));
        let authorization_service = AuthorizationService::new(self.config.clone(), Arc::new(authorization_provider));
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
        load_configured_acl_file(&self.provider_registry, &self.config).await
    }

    pub fn is_acl_white_remote_address(
        &self,
        access_key: Option<&str>,
        source_ip: Option<&str>,
    ) -> RocketMQResult<bool> {
        self.provider_registry
            .is_acl_white_remote_address(access_key, source_ip)
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
                .map_err(|error| RocketMQError::Internal(format!("ACL file watcher task failed: {error}")))?;
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
}

impl AuthenticationService {
    fn new(config: AuthConfig, provider: Arc<DefaultAuthenticationProvider>) -> Self {
        Self {
            whitelist: parse_whitelist(config.authentication_whitelist.as_str()),
            config,
            provider,
            builder: DefaultAuthenticationContextBuilder::new(),
        }
    }

    pub async fn authenticate_remoting(
        &self,
        command: &RemotingCommand,
        channel_id: Option<&str>,
    ) -> RocketMQResult<()> {
        if !self.config.authentication_enabled || self.whitelist.contains(&command.code().to_string()) {
            return Ok(());
        }

        let context = self
            .builder
            .build_from_remoting(command, channel_id)
            .map_err(|error| RocketMQError::authentication_failed(error.to_string()))?;
        self.provider.authenticate(&context).await
    }
}

#[derive(Clone)]
pub struct AuthorizationService {
    config: AuthConfig,
    whitelist: HashSet<String>,
    provider: Arc<DefaultAuthorizationProvider>,
}

impl AuthorizationService {
    fn new(config: AuthConfig, provider: Arc<DefaultAuthorizationProvider>) -> Self {
        Self {
            whitelist: parse_whitelist(config.authorization_whitelist.as_str()),
            config,
            provider,
        }
    }

    pub async fn authorize_remoting(
        &self,
        channel_context: &(dyn std::any::Any + Send + Sync),
        command: &RemotingCommand,
    ) -> RocketMQResult<()> {
        if !self.config.authorization_enabled || self.whitelist.contains(&command.code().to_string()) {
            return Ok(());
        }

        let contexts = self
            .provider
            .new_contexts_from_remoting_command(channel_context, command)
            .map_err(map_authorization_error)?;

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

async fn load_configured_acl_file(provider_registry: &ProviderRegistry, config: &AuthConfig) -> RocketMQResult<usize> {
    let acl_file = config.acl_file.as_str().trim();
    if acl_file.is_empty() {
        return Ok(0);
    }

    let loader = FileAclConfigLoader::new(acl_file.to_owned());
    let acl_config = loader.load().await?;
    apply_acl_config(provider_registry, &acl_config).await
}

fn start_acl_file_watcher(config: &AuthConfig, provider_registry: ProviderRegistry) -> Option<AclFileWatchHandle> {
    let acl_file = config.acl_file.as_str().trim();
    if acl_file.is_empty() || !config.acl_file_watch_enabled {
        return None;
    }

    let loader = FileAclConfigLoader::new(acl_file.to_owned());
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
                    match loader.load().await {
                        Ok(acl_config) => match apply_acl_config(&provider_registry, &acl_config).await {
                            Ok(count) => debug!("Reloaded {} ACL account(s) from configured ACL file", count),
                            Err(error) => warn!("Failed to apply reloaded ACL file: {error}"),
                        },
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
    provider_registry.set_acl_white_list_snapshot(AclWhiteListSnapshot::from_acl_config(acl_config))?;
    let Some(accounts) = acl_config.plain_access_configs() else {
        return Ok(0);
    };

    let authn_provider = provider_registry.authentication_metadata_provider();
    let authz_provider = provider_registry.authorization_metadata_provider();
    let mut applied = 0;

    for account in accounts {
        let user = user_from_plain_account(account)?;
        upsert_user(authn_provider.clone(), user.clone()).await?;

        match acl_from_plain_account(account)? {
            Some(acl) => upsert_acl(authz_provider.clone(), &user, acl).await?,
            None => authz_provider
                .delete_acl(&user)
                .await
                .map_err(map_authorization_error)?,
        }
        applied += 1;
    }

    Ok(applied)
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
    RocketMQError::BrokerPermissionDenied {
        operation: error.to_string(),
    }
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

fn valid_source_ip(source_ip: Option<&str>) -> Option<&str> {
    source_ip
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("unknown"))
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
}
