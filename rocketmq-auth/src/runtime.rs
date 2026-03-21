use std::collections::HashSet;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

use crate::authentication::builder::default_authentication_context_builder::DefaultAuthenticationContextBuilder;
use crate::authentication::builder::AuthenticationContextBuilder;
use crate::authentication::enums::user_status::UserStatus;
use crate::authentication::enums::user_type::UserType;
use crate::authentication::model::user::User;
use crate::authentication::provider::authentication_metadata_provider::AuthenticationMetadataProvider;
use crate::authentication::provider::AuthenticationProvider;
use crate::authentication::provider::DefaultAuthenticationProvider;
use crate::authentication::provider::LocalAuthenticationMetadataProvider;
use crate::authorization::metadata_provider::AuthorizationMetadataProvider;
use crate::authorization::metadata_provider::LocalAuthorizationMetadataProvider;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationProvider;
use crate::authorization::provider::DefaultAuthorizationProvider;
use crate::config::AuthConfig;

#[derive(Clone)]
pub struct ProviderRegistry {
    authentication_metadata_provider: Arc<LocalAuthenticationMetadataProvider>,
    authorization_metadata_provider: Arc<LocalAuthorizationMetadataProvider>,
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
        })
    }

    pub fn authentication_metadata_provider(&self) -> Arc<LocalAuthenticationMetadataProvider> {
        self.authentication_metadata_provider.clone()
    }

    pub fn authorization_metadata_provider(&self) -> Arc<LocalAuthorizationMetadataProvider> {
        self.authorization_metadata_provider.clone()
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

        let mut authentication_provider = DefaultAuthenticationProvider::new();
        authentication_provider.initialize_with_registry(self.config.clone(), provider_registry.clone())?;

        let mut authorization_provider = DefaultAuthorizationProvider::new();
        authorization_provider
            .initialize_with_registry(self.config.clone(), provider_registry.clone())
            .map_err(map_authorization_error)?;

        let authentication_service = AuthenticationService::new(self.config.clone(), Arc::new(authentication_provider));
        let authorization_service = AuthorizationService::new(self.config.clone(), Arc::new(authorization_provider));

        Ok(AuthRuntime {
            config: self.config,
            provider_registry,
            authentication_service,
            authorization_service,
        })
    }
}

#[derive(Clone)]
pub struct AuthRuntime {
    config: AuthConfig,
    provider_registry: ProviderRegistry,
    authentication_service: AuthenticationService,
    authorization_service: AuthorizationService,
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

    pub async fn check_remoting(
        &self,
        channel_context: &(dyn std::any::Any + Send + Sync),
        command: &RemotingCommand,
    ) -> RocketMQResult<()> {
        self.authentication_service.authenticate_remoting(command, None).await?;
        self.authorization_service
            .authorize_remoting(channel_context, command)
            .await
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::action::Action;
    use rocketmq_remoting::code::request_code::RequestCode;

    use super::*;
    use crate::authentication::acl_signer;
    use crate::authentication::enums::subject_type::SubjectType;
    use crate::authorization::enums::decision::Decision;
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

        let signature = acl_signer::cal_signature("AccessKeyalicetopictopic-a".as_bytes(), "secret").unwrap();
        let command = send_message_command("topic-a", "alice", &signature);

        runtime.check_remoting(&(), &command).await.unwrap();
    }
}
