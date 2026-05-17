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

use std::collections::HashSet;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_auth::authentication::builder::default_authentication_context_builder::DefaultAuthenticationContextBuilder;
use rocketmq_auth::authentication::builder::AuthenticationContextBuilder;
use rocketmq_auth::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use rocketmq_auth::authentication::enums::subject_type::SubjectType;
#[cfg(test)]
use rocketmq_auth::authentication::model::user::User;
#[cfg(test)]
use rocketmq_auth::authentication::provider::AuthenticationMetadataProvider;
use rocketmq_auth::authentication::provider::AuthenticationProvider;
use rocketmq_auth::authorization::context::default_authorization_context::DefaultAuthorizationContext;
#[cfg(test)]
use rocketmq_auth::authorization::metadata_provider::AuthorizationMetadataProvider;
#[cfg(test)]
use rocketmq_auth::authorization::model::acl::Acl;
use rocketmq_auth::authorization::model::resource::Resource;
use rocketmq_auth::authorization::provider::AuthorizationError;
use rocketmq_auth::authorization::provider::AuthorizationProvider;
use rocketmq_auth::authorization::provider::DefaultAuthorizationProvider;
use rocketmq_auth::AuthRuntime;
use rocketmq_auth::AuthRuntimeBuilder;
use rocketmq_auth::DefaultAuthenticationProvider;
use rocketmq_auth::ProviderRegistry;
use rocketmq_common::common::action::Action;
use rocketmq_error::AuthError;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
use tonic::Request;

use crate::config::ProxyAuthConfig;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::processor::AckMessageRequest;
use crate::processor::ChangeInvisibleDurationRequest;
use crate::processor::EndTransactionRequest;
use crate::processor::ForwardMessageToDeadLetterQueueRequest;
use crate::processor::GetOffsetRequest;
use crate::processor::PullMessageRequest;
use crate::processor::QueryAssignmentRequest;
use crate::processor::QueryOffsetRequest;
use crate::processor::QueryRouteRequest;
use crate::processor::RecallMessageRequest;
use crate::processor::ReceiveMessageRequest;
use crate::processor::SendMessageRequest;
use crate::processor::UpdateOffsetRequest;
use crate::proto::v2;
use crate::service::ResourceIdentity;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    username: String,
    source_ip: String,
    channel_id: Option<String>,
    white_listed: bool,
}

impl AuthenticatedPrincipal {
    fn new(username: String, source_ip: String, channel_id: Option<String>) -> Self {
        Self {
            username,
            source_ip,
            channel_id,
            white_listed: false,
        }
    }

    fn white_listed(username: Option<String>, source_ip: String, channel_id: Option<String>) -> Self {
        Self {
            username: username.unwrap_or_default(),
            source_ip,
            channel_id,
            white_listed: true,
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn source_ip(&self) -> &str {
        &self.source_ip
    }

    pub fn channel_id(&self) -> Option<&str> {
        self.channel_id.as_deref()
    }

    pub fn is_white_listed(&self) -> bool {
        self.white_listed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationContextSpec {
    resource: Resource,
    actions: Vec<Action>,
}

impl AuthorizationContextSpec {
    pub fn topic(resource: &ResourceIdentity, actions: Vec<Action>) -> Self {
        Self {
            resource: Resource::of_topic(resource.to_string().as_str()),
            actions,
        }
    }

    pub fn retry_aware_topic(
        resource: &ResourceIdentity,
        topic_actions: Vec<Action>,
        retry_actions: Vec<Action>,
    ) -> Self {
        let resource_name = resource.to_string();
        if NamespaceUtil::is_retry_topic(resource_name.as_str()) {
            Self {
                resource: Resource::of_group(resource_name),
                actions: retry_actions,
            }
        } else {
            Self {
                resource: Resource::of_topic(resource_name.as_str()),
                actions: topic_actions,
            }
        }
    }

    pub fn group(resource: &ResourceIdentity, actions: Vec<Action>) -> Self {
        Self {
            resource: Resource::of_group(resource.to_string()),
            actions,
        }
    }
}

#[derive(Clone)]
pub struct ProxyAuthRuntime {
    config: ProxyAuthConfig,
    #[cfg_attr(not(test), allow(dead_code))]
    auth_runtime: AuthRuntime,
    #[cfg_attr(not(test), allow(dead_code))]
    provider_registry: ProviderRegistry,
    authentication_provider: Arc<DefaultAuthenticationProvider>,
    authorization_provider: Arc<DefaultAuthorizationProvider>,
    authentication_builder: DefaultAuthenticationContextBuilder,
    authentication_whitelist: HashSet<String>,
    authorization_whitelist: HashSet<String>,
}

impl ProxyAuthRuntime {
    pub async fn from_proxy_config(config: &ProxyAuthConfig) -> ProxyResult<Option<Self>> {
        if !config.enabled() {
            return Ok(None);
        }

        let auth_config = config.to_auth_config();
        let auth_runtime = AuthRuntimeBuilder::new(auth_config.clone())
            .build()
            .await
            .map_err(ProxyError::from)?;
        let provider_registry = auth_runtime.provider_registry().clone();

        let mut authentication_provider = DefaultAuthenticationProvider::new();
        authentication_provider
            .initialize_with_registry(auth_config.clone(), provider_registry.clone())
            .map_err(ProxyError::from)?;

        let mut authorization_provider = DefaultAuthorizationProvider::new();
        authorization_provider
            .initialize_with_registry(auth_config, provider_registry.clone())
            .map_err(map_authorization_error)?;

        Ok(Some(Self {
            config: config.clone(),
            auth_runtime,
            provider_registry,
            authentication_provider: Arc::new(authentication_provider),
            authorization_provider: Arc::new(authorization_provider),
            authentication_builder: DefaultAuthenticationContextBuilder::new(),
            authentication_whitelist: parse_whitelist(&config.authentication_whitelist),
            authorization_whitelist: parse_whitelist(&config.authorization_whitelist),
        }))
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled()
    }

    pub async fn shutdown(&self) -> ProxyResult<()> {
        self.auth_runtime.shutdown().await.map_err(ProxyError::from)
    }

    pub fn authentication_required(&self, rpc_name: &str) -> bool {
        self.config.authentication_enabled && !self.authentication_whitelist.contains(rpc_name)
    }

    pub fn authorization_required(&self, rpc_name: &str) -> bool {
        self.config.authorization_enabled && !self.authorization_whitelist.contains(rpc_name)
    }

    pub async fn authenticate_request<T: 'static>(
        &self,
        rpc_name: &str,
        request: &Request<T>,
    ) -> ProxyResult<Option<AuthenticatedPrincipal>> {
        let requires_authentication = self.authentication_required(rpc_name);
        let requires_authorization = self.authorization_required(rpc_name);
        if !(requires_authentication || requires_authorization) {
            return Ok(None);
        }

        let authentication_context = self.build_authentication_context(rpc_name, request)?;
        let source_ip = request
            .remote_addr()
            .map(|addr| addr.ip().to_string())
            .unwrap_or_else(|| "unknown".to_owned());
        let channel_id = authentication_context
            .base
            .channel_id()
            .map(ToString::to_string)
            .or_else(|| metadata_string(request, "x-mq-channel-id"));
        let username = authentication_context.username().map(ToString::to_string);

        if self
            .auth_runtime
            .is_acl_white_remote_address(username.as_deref(), Some(source_ip.as_str()))
            .map_err(ProxyError::from)?
        {
            return Ok(Some(AuthenticatedPrincipal::white_listed(
                username, source_ip, channel_id,
            )));
        }

        if requires_authentication {
            self.authentication_provider
                .authenticate(&authentication_context)
                .await
                .map_err(ProxyError::from)?;
        }

        let username = username.ok_or_else(|| {
            ProxyError::from(RocketMQError::authentication_failed(format!(
                "gRPC request {rpc_name} is missing credential information",
            )))
        })?;

        Ok(Some(AuthenticatedPrincipal::new(username, source_ip, channel_id)))
    }

    pub async fn authorize_request(
        &self,
        rpc_name: &str,
        principal: Option<&AuthenticatedPrincipal>,
        contexts: &[AuthorizationContextSpec],
    ) -> ProxyResult<()> {
        if !self.authorization_required(rpc_name) || contexts.is_empty() {
            return Ok(());
        }

        let principal = principal.ok_or_else(|| {
            ProxyError::from(RocketMQError::authentication_failed(format!(
                "gRPC request {rpc_name} does not carry an authenticated principal",
            )))
        })?;

        if principal.is_white_listed() {
            return Ok(());
        }

        for context in contexts {
            let mut builder = DefaultAuthorizationContext::builder()
                .subject(principal.username(), SubjectType::User)
                .resource(context.resource.clone())
                .actions(context.actions.clone())
                .source_ip(principal.source_ip())
                .rpc_code(rpc_name.to_owned());

            if let Some(channel_id) = principal.channel_id() {
                builder = builder.channel_id(channel_id.to_owned());
            }

            self.authorization_provider
                .authorize(&builder.build())
                .await
                .map_err(map_authorization_error)?;
        }

        Ok(())
    }

    pub async fn authenticate_remoting(
        &self,
        command: &RemotingCommand,
        channel_id: Option<&str>,
        source_ip: Option<&str>,
    ) -> ProxyResult<Option<AuthenticatedPrincipal>> {
        let code = command.code().to_string();
        let requires_authentication = self.authentication_required(code.as_str());
        let requires_authorization = self.authorization_required(code.as_str());
        if !(requires_authentication || requires_authorization) {
            return Ok(None);
        }

        let authentication_context = self
            .authentication_builder
            .build_from_remoting(command, channel_id)
            .map_err(|error| ProxyError::from(RocketMQError::authentication_failed(error.to_string())))?;
        let username = authentication_context.username().map(ToString::to_string);
        let source_ip = source_ip.unwrap_or("unknown").to_owned();
        let channel_id = authentication_context
            .base
            .channel_id()
            .map(ToString::to_string)
            .or_else(|| channel_id.map(ToOwned::to_owned));

        if self
            .auth_runtime
            .is_acl_white_remote_address(username.as_deref(), Some(source_ip.as_str()))
            .map_err(ProxyError::from)?
        {
            return Ok(Some(AuthenticatedPrincipal::white_listed(
                username, source_ip, channel_id,
            )));
        }

        if requires_authentication {
            self.authentication_provider
                .authenticate(&authentication_context)
                .await
                .map_err(ProxyError::from)?;
        }

        let username = username.ok_or_else(|| {
            ProxyError::from(RocketMQError::authentication_failed(format!(
                "remoting request {} is missing credential information",
                command.code()
            )))
        })?;

        Ok(Some(AuthenticatedPrincipal::new(username, source_ip, channel_id)))
    }

    pub async fn authorize_remoting(
        &self,
        channel_context: &(dyn std::any::Any + Send + Sync),
        command: &RemotingCommand,
    ) -> ProxyResult<()> {
        let code = command.code().to_string();
        if !self.authorization_required(code.as_str()) {
            return Ok(());
        }

        let source_ip = source_ip_from_channel_context(channel_context);
        if self
            .auth_runtime
            .is_acl_white_remote_address(access_key_from_command(command), source_ip.as_deref())
            .map_err(ProxyError::from)?
        {
            return Ok(());
        }

        let contexts = self
            .authorization_provider
            .new_contexts_from_remoting_command(channel_context, command)
            .map_err(map_authorization_error)?;
        for context in contexts {
            self.authorization_provider
                .authorize(&context)
                .await
                .map_err(map_authorization_error)?;
        }
        Ok(())
    }

    fn build_authentication_context<T: 'static>(
        &self,
        rpc_name: &str,
        request: &Request<T>,
    ) -> ProxyResult<DefaultAuthenticationContext> {
        let mut context = self
            .authentication_builder
            .build_from_grpc(request.metadata(), request.get_ref())
            .map_err(|error| ProxyError::from(RocketMQError::authentication_failed(error.to_string())))?;

        context.base.set_rpc_code(Some(CheetahString::from(rpc_name)));
        if context.base.channel_id().is_none() {
            context
                .base
                .set_channel_id(metadata_string(request, "x-mq-channel-id").map(CheetahString::from));
        }

        Ok(context)
    }

    #[cfg(test)]
    pub(crate) async fn create_user(&self, user: User) -> ProxyResult<()> {
        self.provider_registry
            .authentication_metadata_provider()
            .create_user(user)
            .await
            .map_err(ProxyError::from)
    }

    #[cfg(test)]
    pub(crate) async fn create_acl(&self, acl: Acl) -> ProxyResult<()> {
        self.provider_registry
            .authorization_metadata_provider()
            .create_acl(acl)
            .await
            .map_err(map_authorization_error)
    }
}

pub fn query_route_contexts(request: &QueryRouteRequest) -> Vec<AuthorizationContextSpec> {
    vec![AuthorizationContextSpec::retry_aware_topic(
        &request.topic,
        vec![Action::Pub, Action::Sub, Action::Get],
        vec![Action::Sub, Action::Get],
    )]
}

pub fn query_assignment_contexts(request: &QueryAssignmentRequest) -> Vec<AuthorizationContextSpec> {
    vec![
        AuthorizationContextSpec::topic(&request.topic, vec![Action::Sub]),
        AuthorizationContextSpec::group(&request.group, vec![Action::Sub]),
    ]
}

pub fn send_message_contexts(request: &SendMessageRequest) -> Vec<AuthorizationContextSpec> {
    request
        .messages
        .iter()
        .map(|entry| AuthorizationContextSpec::retry_aware_topic(&entry.topic, vec![Action::Pub], vec![Action::Sub]))
        .collect()
}

pub fn recall_message_contexts(request: &RecallMessageRequest) -> Vec<AuthorizationContextSpec> {
    vec![AuthorizationContextSpec::topic(&request.topic, vec![Action::Pub])]
}

pub fn receive_message_contexts(request: &ReceiveMessageRequest) -> Vec<AuthorizationContextSpec> {
    vec![
        AuthorizationContextSpec::retry_aware_topic(&request.target.topic, vec![Action::Sub], vec![Action::Sub]),
        AuthorizationContextSpec::group(&request.group, vec![Action::Sub]),
    ]
}

pub fn pull_message_contexts(request: &PullMessageRequest) -> Vec<AuthorizationContextSpec> {
    vec![
        AuthorizationContextSpec::retry_aware_topic(&request.target.topic, vec![Action::Sub], vec![Action::Sub]),
        AuthorizationContextSpec::group(&request.group, vec![Action::Sub]),
    ]
}

pub fn ack_message_contexts(request: &AckMessageRequest) -> Vec<AuthorizationContextSpec> {
    vec![
        AuthorizationContextSpec::retry_aware_topic(&request.topic, vec![Action::Sub], vec![Action::Sub]),
        AuthorizationContextSpec::group(&request.group, vec![Action::Sub]),
    ]
}

pub fn forward_message_to_dead_letter_queue_contexts(
    request: &ForwardMessageToDeadLetterQueueRequest,
) -> Vec<AuthorizationContextSpec> {
    vec![AuthorizationContextSpec::group(&request.group, vec![Action::Sub])]
}

pub fn change_invisible_duration_contexts(request: &ChangeInvisibleDurationRequest) -> Vec<AuthorizationContextSpec> {
    vec![
        AuthorizationContextSpec::retry_aware_topic(&request.topic, vec![Action::Sub], vec![Action::Sub]),
        AuthorizationContextSpec::group(&request.group, vec![Action::Sub]),
    ]
}

pub fn update_offset_contexts(request: &UpdateOffsetRequest) -> Vec<AuthorizationContextSpec> {
    vec![
        AuthorizationContextSpec::retry_aware_topic(
            &request.target.topic,
            vec![Action::Sub, Action::Update],
            vec![Action::Sub, Action::Update],
        ),
        AuthorizationContextSpec::group(&request.group, vec![Action::Sub, Action::Update]),
    ]
}

pub fn get_offset_contexts(request: &GetOffsetRequest) -> Vec<AuthorizationContextSpec> {
    vec![
        AuthorizationContextSpec::retry_aware_topic(
            &request.target.topic,
            vec![Action::Sub, Action::Get],
            vec![Action::Sub, Action::Get],
        ),
        AuthorizationContextSpec::group(&request.group, vec![Action::Sub, Action::Get]),
    ]
}

pub fn query_offset_contexts(request: &QueryOffsetRequest) -> Vec<AuthorizationContextSpec> {
    vec![AuthorizationContextSpec::retry_aware_topic(
        &request.target.topic,
        vec![Action::Sub, Action::Get],
        vec![Action::Sub, Action::Get],
    )]
}

pub fn end_transaction_contexts(request: &EndTransactionRequest) -> Vec<AuthorizationContextSpec> {
    vec![AuthorizationContextSpec::topic(&request.topic, vec![Action::Pub])]
}

pub fn heartbeat_contexts(request: &v2::HeartbeatRequest) -> Vec<AuthorizationContextSpec> {
    request
        .group
        .as_ref()
        .map(|group| AuthorizationContextSpec::group(&resource_identity(group), vec![Action::Sub]))
        .into_iter()
        .collect()
}

pub fn notify_client_termination_contexts(
    request: &v2::NotifyClientTerminationRequest,
) -> Vec<AuthorizationContextSpec> {
    request
        .group
        .as_ref()
        .map(|group| AuthorizationContextSpec::group(&resource_identity(group), vec![Action::Sub]))
        .into_iter()
        .collect()
}

pub fn sync_lite_subscription_contexts(request: &v2::SyncLiteSubscriptionRequest) -> Vec<AuthorizationContextSpec> {
    let mut contexts = Vec::new();
    if let Some(group) = request.group.as_ref() {
        contexts.push(AuthorizationContextSpec::group(
            &resource_identity(group),
            vec![Action::Sub],
        ));
    }
    if let Some(topic) = request.topic.as_ref() {
        contexts.push(AuthorizationContextSpec::topic(
            &resource_identity(topic),
            vec![Action::Sub],
        ));
    }
    contexts
}

pub fn telemetry_command_contexts(command: &v2::TelemetryCommand) -> Vec<AuthorizationContextSpec> {
    match command.command.as_ref() {
        Some(v2::telemetry_command::Command::Settings(settings)) => settings_contexts(settings),
        _ => Vec::new(),
    }
}

fn settings_contexts(settings: &v2::Settings) -> Vec<AuthorizationContextSpec> {
    match settings.pub_sub.as_ref() {
        Some(v2::settings::PubSub::Publishing(publishing)) => publishing
            .topics
            .iter()
            .map(|topic| AuthorizationContextSpec::topic(&resource_identity(topic), vec![Action::Pub]))
            .collect(),
        Some(v2::settings::PubSub::Subscription(subscription)) => {
            let mut contexts = Vec::new();
            if let Some(group) = subscription.group.as_ref() {
                contexts.push(AuthorizationContextSpec::group(
                    &resource_identity(group),
                    vec![Action::Sub],
                ));
            }
            contexts.extend(
                subscription
                    .subscriptions
                    .iter()
                    .filter_map(|entry| entry.topic.as_ref())
                    .map(|topic| AuthorizationContextSpec::topic(&resource_identity(topic), vec![Action::Sub])),
            );
            contexts
        }
        None => Vec::new(),
    }
}

fn resource_identity(resource: &v2::Resource) -> ResourceIdentity {
    ResourceIdentity::new(resource.resource_namespace.as_str(), resource.name.as_str())
}

fn metadata_string<T>(request: &Request<T>, key: &'static str) -> Option<String> {
    request
        .metadata()
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn map_authorization_error(error: AuthorizationError) -> ProxyError {
    ProxyError::from(RocketMQError::from(error))
}

pub fn is_auth_error(error: &RocketMQError) -> bool {
    matches!(
        error,
        RocketMQError::Authentication(
            AuthError::AuthenticationFailed(_)
                | AuthError::InvalidCredential(_)
                | AuthError::UserNotFound(_)
                | AuthError::InvalidSignature(_)
        ) | RocketMQError::BrokerPermissionDenied { .. }
    )
}

fn parse_whitelist(entries: &[String]) -> HashSet<String> {
    entries
        .iter()
        .map(String::as_str)
        .flat_map(|entry| entry.split(','))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn access_key_from_command(command: &RemotingCommand) -> Option<&str> {
    command.ext_fields().and_then(|fields| {
        fields
            .get(&CheetahString::from_static_str("AccessKey"))
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

    use rocketmq_auth::authentication::acl_signer;
    use rocketmq_auth::authentication::enums::user_status::UserStatus;
    use rocketmq_auth::authentication::enums::user_type::UserType;
    use rocketmq_auth::authorization::enums::decision::Decision;
    use rocketmq_auth::authorization::model::policy::Policy;
    use rocketmq_auth::authorization::model::resource::Resource;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;

    use super::*;

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).expect("test temp directory should be created");
        dir
    }

    fn send_message_command(topic: &str, access_key: &str, secret_key: &str) -> RemotingCommand {
        let signature = acl_signer::cal_signature(format!("{access_key}{topic}").as_bytes(), secret_key)
            .expect("signature should be calculated");
        let mut ext_fields = HashMap::new();
        ext_fields.insert(
            CheetahString::from_static_str("AccessKey"),
            CheetahString::from(access_key),
        );
        ext_fields.insert(
            CheetahString::from_static_str("Signature"),
            CheetahString::from(signature),
        );
        ext_fields.insert(CheetahString::from_static_str("topic"), CheetahString::from(topic));
        RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32()).set_ext_fields(ext_fields)
    }

    fn normal_user(username: &str, password: &str) -> User {
        let mut user = User::of_with_type(username, password, UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        user
    }

    #[test]
    fn map_authorization_error_preserves_proxy_error_category() {
        let denied = map_authorization_error(AuthorizationError::PermissionDenied {
            subject: "User:alice".to_string(),
            resource: "Topic:test".to_string(),
            reason: "denied".to_string(),
        });
        assert!(matches!(
            denied,
            ProxyError::RocketMQ(RocketMQError::BrokerPermissionDenied { .. })
        ));

        let invalid = map_authorization_error(AuthorizationError::InvalidContext("missing resource".to_string()));
        assert!(matches!(
            invalid,
            ProxyError::RocketMQ(RocketMQError::IllegalArgument(_))
        ));

        let config = map_authorization_error(AuthorizationError::ConfigurationError("missing provider".to_string()));
        assert!(matches!(
            config,
            ProxyError::RocketMQ(RocketMQError::ConfigInvalidValue {
                key: "auth.authorization",
                ..
            })
        ));

        let internal =
            map_authorization_error(AuthorizationError::PolicyEvaluationFailed("invalid policy".to_string()));
        assert!(matches!(internal, ProxyError::RocketMQ(RocketMQError::Internal(_))));
    }

    #[tokio::test]
    async fn authenticate_remoting_accepts_acl_account_white_remote_address() {
        let test_dir = unique_test_dir("proxy-auth-white-remote-address");
        let acl_file = test_dir.join("plain_acl.yml");
        fs::write(
            &acl_file,
            r#"
accounts:
  - accessKey: alice
    secretKey: secret
    whiteRemoteAddress: 192.168.0.*
    defaultTopicPerm: DENY
"#,
        )
        .expect("acl file should be written");

        let runtime = ProxyAuthRuntime::from_proxy_config(&ProxyAuthConfig {
            acl_file: acl_file.to_string_lossy().into_owned(),
            authentication_enabled: true,
            authorization_enabled: true,
            auth_config_path: test_dir.join("auth-store").to_string_lossy().into_owned(),
            ..ProxyAuthConfig::default()
        })
        .await
        .expect("runtime should build")
        .expect("runtime should be enabled");

        let mut command = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new("TopicA", None),
        );
        command.make_custom_header_to_net();
        command.add_ext_field("AccessKey", "alice");

        let principal = runtime
            .authenticate_remoting(&command, Some("channel-a"), Some("192.168.0.7"))
            .await
            .expect("white remote address should bypass signature")
            .expect("principal should be returned");

        assert_eq!(principal.username(), "alice");
        assert!(principal.is_white_listed());

        runtime.shutdown().await.expect("runtime should shut down");
        let _ = fs::remove_dir_all(test_dir);
    }

    #[tokio::test]
    async fn authorize_request_skips_white_listed_principal() {
        let test_dir = unique_test_dir("proxy-auth-authorize-white-listed-principal");
        let runtime = ProxyAuthRuntime::from_proxy_config(&ProxyAuthConfig {
            authorization_enabled: true,
            auth_config_path: test_dir.join("auth-store").to_string_lossy().into_owned(),
            ..ProxyAuthConfig::default()
        })
        .await
        .expect("runtime should build")
        .expect("runtime should be enabled");

        let principal = AuthenticatedPrincipal::white_listed(None, "10.10.1.2".to_owned(), None);
        let contexts = vec![AuthorizationContextSpec {
            resource: Resource::of_topic("TopicA"),
            actions: vec![Action::Pub],
        }];

        runtime
            .authorize_request("QueryRoute", Some(&principal), &contexts)
            .await
            .expect("white listed principal should bypass authorization metadata lookup");

        runtime.shutdown().await.expect("runtime should shut down");
        let _ = fs::remove_dir_all(test_dir);
    }

    #[tokio::test]
    async fn remoting_auth_restores_persisted_metadata_after_restart() {
        let test_dir = unique_test_dir("proxy-auth-persisted-remoting");
        let config = ProxyAuthConfig {
            authentication_enabled: true,
            authorization_enabled: true,
            auth_config_path: test_dir.join("auth-store").to_string_lossy().into_owned(),
            ..ProxyAuthConfig::default()
        };

        let runtime = ProxyAuthRuntime::from_proxy_config(&config)
            .await
            .expect("runtime should build")
            .expect("runtime should be enabled");
        runtime.create_user(normal_user("alice", "secret")).await.unwrap();
        runtime
            .create_acl(Acl::of(
                "alice",
                SubjectType::User,
                Policy::of(
                    vec![Resource::of_topic("TopicA")],
                    vec![Action::Pub],
                    None,
                    Decision::Allow,
                ),
            ))
            .await
            .unwrap();
        runtime.shutdown().await.expect("runtime should shut down");

        let restarted = ProxyAuthRuntime::from_proxy_config(&config)
            .await
            .expect("runtime should rebuild")
            .expect("runtime should be enabled");
        let command = send_message_command("TopicA", "alice", "secret");
        let principal = restarted
            .authenticate_remoting(&command, Some("channel-a"), Some("127.0.0.1"))
            .await
            .expect("authentication should use persisted user")
            .expect("principal should exist");
        assert_eq!(principal.username(), "alice");

        restarted
            .authorize_remoting(&(), &command)
            .await
            .expect("authorization should use persisted ACL");

        restarted.shutdown().await.expect("runtime should shut down");
        let _ = fs::remove_dir_all(test_dir);
    }

    #[tokio::test]
    async fn remoting_authorization_denies_insufficient_permission() {
        let test_dir = unique_test_dir("proxy-auth-remoting-deny");
        let runtime = ProxyAuthRuntime::from_proxy_config(&ProxyAuthConfig {
            authentication_enabled: true,
            authorization_enabled: true,
            auth_config_path: test_dir.join("auth-store").to_string_lossy().into_owned(),
            ..ProxyAuthConfig::default()
        })
        .await
        .expect("runtime should build")
        .expect("runtime should be enabled");
        runtime.create_user(normal_user("alice", "secret")).await.unwrap();
        runtime
            .create_acl(Acl::of(
                "alice",
                SubjectType::User,
                Policy::of(
                    vec![Resource::of_topic("TopicA")],
                    vec![Action::Pub],
                    None,
                    Decision::Deny,
                ),
            ))
            .await
            .unwrap();

        let command = send_message_command("TopicA", "alice", "secret");
        runtime
            .authenticate_remoting(&command, Some("channel-a"), Some("127.0.0.1"))
            .await
            .expect("authentication should pass");
        let error = runtime
            .authorize_remoting(&(), &command)
            .await
            .expect_err("authorization should deny");
        assert!(matches!(
            error,
            ProxyError::RocketMQ(RocketMQError::BrokerPermissionDenied { .. })
        ));

        runtime.shutdown().await.expect("runtime should shut down");
        let _ = fs::remove_dir_all(test_dir);
    }
}
