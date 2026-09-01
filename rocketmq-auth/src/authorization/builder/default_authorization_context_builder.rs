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

use std::collections::HashMap;
use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::body::delete_subscription_group_list_request_body::DeleteSubscriptionGroupListRequestBody;
use rocketmq_protocol::protocol::body::delete_topic_list_request_body::DeleteTopicListRequestBody;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::supervised_mutation::{
    ExpectedMessageRequestMode, ExpectedState, GetMessageRequestModeRequestBody, SetMessageRequestModeCasRequestBody,
    SupervisedSubscriptionGroupConfigCasRequestBody, SupervisedTopicConfigCasRequestBody,
};
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_protocol::protocol::header::get_subscription_group_config_request_header::GetSubscriptionGroupConfigRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_conditional_header::UpdateConsumerOffsetConditionalHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::subscription::subscription_group_config::validate_subscription_group_name;
use rocketmq_security_api::Action;
use rocketmq_security_api::ResourcePattern;
use rocketmq_security_api::ResourceType;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authorization::builder::AuthorizationContextBuilder;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::model::resource::Resource;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationResult;
use crate::config::AuthConfig;
use crate::RemotingAuthContext;

const ACCESS_KEY: &str = "AccessKey";
const TOPIC: &str = "topic";
const GROUP: &str = "group";
const CONSUMER_GROUP: &str = "consumerGroup";
const B: &str = "b";

#[derive(Clone, Debug)]
pub struct DefaultAuthorizationContextBuilder {
    auth_config: AuthConfig,
}

impl DefaultAuthorizationContextBuilder {
    pub fn new(auth_config: AuthConfig) -> Self {
        Self { auth_config }
    }

    fn build_context(
        &self,
        subject_key: Option<&str>,
        resource: Resource,
        actions: Vec<Action>,
        source_ip: &str,
        rpc_code: &str,
    ) -> DefaultAuthorizationContext {
        let mut context = DefaultAuthorizationContext::default();
        if let Some(subject_key) = subject_key {
            context.set_subject(subject_key, SubjectType::User);
        }
        context.set_resource(resource);
        context.set_actions(actions);
        context.set_source_ip(source_ip);
        context.set_rpc_code(rpc_code);
        context
    }

    fn field_value<'a>(&self, fields: &'a HashMap<CheetahString, CheetahString>, key: &str) -> Option<&'a str> {
        fields
            .get(&CheetahString::from(key))
            .map(|value| value.as_str())
            .filter(|value| !value.is_empty())
    }

    fn raw_field_value<'a>(&self, fields: &'a HashMap<CheetahString, CheetahString>, key: &str) -> Option<&'a str> {
        fields.get(&CheetahString::from(key)).map(|value| value.as_str())
    }

    fn subject_key(&self, command: &RemotingCommand) -> Option<String> {
        command
            .ext_fields()
            .and_then(|fields| self.field_value(fields, ACCESS_KEY))
            .map(|username| format!("{}:{}", SubjectType::User.name(), username))
    }

    fn source_ip(auth_context: &RemotingAuthContext) -> AuthorizationResult<String> {
        auth_context
            .validate()
            .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
        Ok(auth_context.source_ip().unwrap_or("embedded").to_owned())
    }

    fn require_topic(topic: &str) -> AuthorizationResult<&str> {
        if TopicValidator::validate_topic(topic).valid() {
            Ok(topic)
        } else {
            Err(AuthorizationError::InvalidContext(
                "supervised mutation topic is invalid".to_owned(),
            ))
        }
    }

    fn require_group(group: &str) -> AuthorizationResult<&str> {
        validate_subscription_group_name(group).map_err(|_| {
            AuthorizationError::InvalidContext("supervised mutation consumer group is invalid".to_owned())
        })?;
        Ok(group)
    }

    fn supervised_request_requires_context(request_code: RequestCode) -> bool {
        matches!(
            request_code,
            RequestCode::UpdateTopicConfigStateCas
                | RequestCode::UpdateSubscriptionGroupConfigStateCas
                | RequestCode::UpdateConsumerOffsetConditional
                | RequestCode::GetBrokerMutationConfig
                | RequestCode::GetMessageRequestMode
                | RequestCode::SetMessageRequestModeCas
        )
    }

    fn require_body<'a>(command: &'a RemotingCommand, label: &str) -> AuthorizationResult<&'a [u8]> {
        command
            .body()
            .map(AsRef::as_ref)
            .ok_or_else(|| AuthorizationError::InvalidContext(format!("{label} body is missing")))
    }

    fn push_topic_sub_if_not_retry(
        &self,
        contexts: &mut Vec<DefaultAuthorizationContext>,
        subject_key: Option<&str>,
        topic: &str,
        actions: Vec<Action>,
        source_ip: &str,
        rpc_code: &str,
    ) {
        if NamespaceUtil::is_retry_topic(topic) {
            return;
        }
        contexts.push(self.build_context(subject_key, Resource::of_topic(topic), actions, source_ip, rpc_code));
    }

    fn push_java_annotation_resource(
        &self,
        contexts: &mut Vec<DefaultAuthorizationContext>,
        subject_key: Option<&str>,
        resource_type: ResourceType,
        resource_value: &str,
        actions: Vec<Action>,
        source_ip: &str,
        rpc_code: &str,
    ) {
        let resource = match resource_type {
            ResourceType::Cluster => Resource::of_cluster(resource_value),
            ResourceType::Topic if NamespaceUtil::is_retry_topic(resource_value) => {
                Resource::of_group(resource_value.to_string())
            }
            ResourceType::Topic => Resource::of(
                ResourceType::Topic,
                Some(resource_value.to_string()),
                ResourcePattern::Literal,
            ),
            ResourceType::Group => Resource::of_group(resource_value.to_string()),
            other => Resource::of(other, Some(resource_value.to_string()), ResourcePattern::Literal),
        };
        contexts.push(self.build_context(subject_key, resource, actions, source_ip, rpc_code));
    }

    fn push_java_annotation_field(
        &self,
        contexts: &mut Vec<DefaultAuthorizationContext>,
        subject_key: Option<&str>,
        fields: &HashMap<CheetahString, CheetahString>,
        field_name: &str,
        splitter: Option<&str>,
        resource_type: ResourceType,
        actions: Vec<Action>,
        source_ip: &str,
        rpc_code: &str,
    ) {
        let Some(value) = self.raw_field_value(fields, field_name) else {
            return;
        };

        let values: Vec<&str> = match splitter {
            Some(splitter) => value.split(splitter).filter(|value| !value.is_empty()).collect(),
            None => vec![value],
        };

        for value in values {
            self.push_java_annotation_resource(
                contexts,
                subject_key,
                resource_type,
                value,
                actions.clone(),
                source_ip,
                rpc_code,
            );
        }
    }

    fn push_java_annotation_cluster_default(
        &self,
        contexts: &mut Vec<DefaultAuthorizationContext>,
        subject_key: Option<&str>,
        actions: Vec<Action>,
        source_ip: &str,
        rpc_code: &str,
    ) {
        self.push_java_annotation_resource(
            contexts,
            subject_key,
            ResourceType::Cluster,
            self.auth_config.cluster_name.as_str(),
            actions,
            source_ip,
            rpc_code,
        );
    }

    fn build_context_by_java_annotation_mapping(
        &self,
        contexts: &mut Vec<DefaultAuthorizationContext>,
        subject_key: Option<&str>,
        fields: &HashMap<CheetahString, CheetahString>,
        source_ip: &str,
        rpc_code: &str,
        request_code: RequestCode,
    ) {
        match request_code {
            RequestCode::UpdateAndCreateTopic => self.push_java_annotation_field(
                contexts,
                subject_key,
                fields,
                TOPIC,
                None,
                ResourceType::Topic,
                vec![Action::Create],
                source_ip,
                rpc_code,
            ),
            RequestCode::DeleteTopicInBroker => self.push_java_annotation_field(
                contexts,
                subject_key,
                fields,
                TOPIC,
                None,
                ResourceType::Topic,
                vec![Action::Delete],
                source_ip,
                rpc_code,
            ),
            RequestCode::DeleteSubscriptionGroup => self.push_java_annotation_field(
                contexts,
                subject_key,
                fields,
                "groupName",
                None,
                ResourceType::Group,
                vec![Action::Delete],
                source_ip,
                rpc_code,
            ),
            RequestCode::GetTopicConfig
            | RequestCode::GetTopicStatsInfo
            | RequestCode::GetMaxOffset
            | RequestCode::GetMinOffset
            | RequestCode::GetEarliestMsgStoreTime
            | RequestCode::ViewMessageById
            | RequestCode::SearchOffsetByTimestamp
            | RequestCode::QueryTopicConsumeByWho
            | RequestCode::CheckRocksdbCqWriteProgress => self.push_java_annotation_field(
                contexts,
                subject_key,
                fields,
                TOPIC,
                None,
                ResourceType::Topic,
                vec![Action::Get],
                source_ip,
                rpc_code,
            ),
            RequestCode::CheckTransactionState => self.push_java_annotation_field(
                contexts,
                subject_key,
                fields,
                TOPIC,
                None,
                ResourceType::Topic,
                vec![Action::Pub],
                source_ip,
                rpc_code,
            ),
            RequestCode::ResumeCheckHalfMessage => self.push_java_annotation_field(
                contexts,
                subject_key,
                fields,
                TOPIC,
                None,
                ResourceType::Topic,
                vec![Action::Update],
                source_ip,
                rpc_code,
            ),
            RequestCode::GetSubscriptionGroupConfig
            | RequestCode::GetConsumerConnectionList
            | RequestCode::GetConsumerRunningInfo
            | RequestCode::QueryTopicsByConsumer => {
                let field = match request_code {
                    RequestCode::GetConsumerConnectionList | RequestCode::GetConsumerRunningInfo => CONSUMER_GROUP,
                    _ => GROUP,
                };
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    field,
                    None,
                    ResourceType::Group,
                    vec![Action::Get],
                    source_ip,
                    rpc_code,
                );
            }
            RequestCode::UpdateAndGetGroupForbidden => {
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    GROUP,
                    None,
                    ResourceType::Group,
                    vec![Action::Update],
                    source_ip,
                    rpc_code,
                );
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    TOPIC,
                    None,
                    ResourceType::Topic,
                    vec![Action::Update],
                    source_ip,
                    rpc_code,
                );
            }
            RequestCode::GetConsumeStats => {
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    CONSUMER_GROUP,
                    None,
                    ResourceType::Group,
                    vec![Action::Get],
                    source_ip,
                    rpc_code,
                );
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    TOPIC,
                    None,
                    ResourceType::Topic,
                    vec![Action::Get],
                    source_ip,
                    rpc_code,
                );
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    "topicList",
                    Some(";"),
                    ResourceType::Topic,
                    vec![Action::Get],
                    source_ip,
                    rpc_code,
                );
            }
            RequestCode::QueryCorrectionOffset => {
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    "filterGroups",
                    Some(","),
                    ResourceType::Group,
                    vec![Action::Get],
                    source_ip,
                    rpc_code,
                );
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    "compareGroup",
                    None,
                    ResourceType::Group,
                    vec![Action::Get],
                    source_ip,
                    rpc_code,
                );
                self.push_java_annotation_field(
                    contexts,
                    subject_key,
                    fields,
                    TOPIC,
                    None,
                    ResourceType::Topic,
                    vec![Action::Get],
                    source_ip,
                    rpc_code,
                );
            }
            RequestCode::AuthCreateUser
            | RequestCode::AuthUpdateUser
            | RequestCode::AuthDeleteUser
            | RequestCode::AuthCreateAcl
            | RequestCode::AuthUpdateAcl
            | RequestCode::AuthDeleteAcl
            | RequestCode::AddBroker
            | RequestCode::RemoveBroker
            | RequestCode::RegisterBroker
            | RequestCode::UnregisterBroker
            | RequestCode::RegisterTopicInNamesrv
            | RequestCode::DeleteTopicInNamesrv
            | RequestCode::UpdateBrokerConfig
            | RequestCode::UpdateBrokerConfigCas
            | RequestCode::BeginProxyDrain
            | RequestCode::CancelProxyDrain
            | RequestCode::PutKvConfig
            | RequestCode::DeleteKvConfig
            | RequestCode::UpdateNamesrvConfig
            | RequestCode::AddWritePermOfBroker
            | RequestCode::WipeWritePermOfBroker
            | RequestCode::ControllerAlterSyncStateSet
            | RequestCode::ControllerElectMaster
            | RequestCode::ControllerRegisterBroker
            | RequestCode::ControllerApplyBrokerId
            | RequestCode::BrokerHeartbeat
            | RequestCode::NotifyBrokerRoleChanged
            | RequestCode::NotifyMinBrokerIdChange
            | RequestCode::ExchangeBrokerHaInfo
            | RequestCode::ResetMasterFlushOffset
            | RequestCode::CleanBrokerData => self.push_java_annotation_cluster_default(
                contexts,
                subject_key,
                vec![Action::Update],
                source_ip,
                rpc_code,
            ),
            RequestCode::AuthGetUser
            | RequestCode::AuthListUsers
            | RequestCode::AuthGetAcl
            | RequestCode::AuthListAcl
            | RequestCode::GetAllProducerInfo
            | RequestCode::GetProducerConnectionList
            | RequestCode::GetBrokerConsumeStats
            | RequestCode::GetKvConfig
            | RequestCode::GetKvlistByNamespace
            | RequestCode::QueryDataVersion
            | RequestCode::GetBrokerClusterInfo
            | RequestCode::GetAllTopicListFromNameserver
            | RequestCode::GetSystemTopicListFromNs
            | RequestCode::GetUnitTopicList
            | RequestCode::GetHasUnitSubTopicList
            | RequestCode::GetHasUnitSubUnunitTopicList
            | RequestCode::GetNamesrvConfig
            | RequestCode::GetBrokerRuntimeInfo
            | RequestCode::GetProxyDrainState
            | RequestCode::ViewBrokerStatsData
            | RequestCode::ExportRocksdbConfigToJson
            | RequestCode::ControllerGetReplicaInfo
            | RequestCode::ControllerGetNextBrokerId
            | RequestCode::GetBrokerHaStatus => {
                self.push_java_annotation_cluster_default(contexts, subject_key, vec![Action::Get], source_ip, rpc_code)
            }
            RequestCode::GetBrokerMemberGroup => self.push_java_annotation_field(
                contexts,
                subject_key,
                fields,
                "clusterName",
                None,
                ResourceType::Cluster,
                vec![Action::Get],
                source_ip,
                rpc_code,
            ),
            RequestCode::GetTopicsByCluster => self.push_java_annotation_field(
                contexts,
                subject_key,
                fields,
                "cluster",
                None,
                ResourceType::Cluster,
                vec![Action::List],
                source_ip,
                rpc_code,
            ),
            _ => {}
        }
    }
}

impl AuthorizationContextBuilder for DefaultAuthorizationContextBuilder {
    fn build_from_remoting(
        &self,
        auth_context: &RemotingAuthContext,
        command: &RemotingCommand,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>> {
        auth_context
            .validate()
            .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
        let mut contexts = Vec::new();
        let empty_fields = HashMap::new();
        let request_code = RequestCode::from(command.code());
        let fields = match command.ext_fields() {
            Some(fields) => fields,
            None if matches!(
                request_code,
                RequestCode::DeleteTopicInBrokerList | RequestCode::DeleteSubscriptionGroupList
            ) || Self::supervised_request_requires_context(request_code) =>
            {
                &empty_fields
            }
            None => return Ok(contexts),
        };

        let subject_key = self.subject_key(command);
        let subject_key = subject_key.as_deref();
        let source_ip = Self::source_ip(auth_context)?;
        let rpc_code = command.code().to_string();

        match request_code {
            RequestCode::UpdateTopicConfigStateCas => {
                let header = command
                    .decode_command_custom_header::<GetTopicConfigRequestHeader>()
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let topic = Self::require_topic(header.topic.as_str())?;
                let body = serde_json::from_slice::<SupervisedTopicConfigCasRequestBody>(Self::require_body(
                    command,
                    "supervised Topic replacement",
                )?)
                .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let action = match body.expected_state {
                    ExpectedState::Absent => Action::Create,
                    ExpectedState::Present { .. } => Action::Update,
                };
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_topic(topic),
                    vec![action],
                    &source_ip,
                    &rpc_code,
                ));
            }
            RequestCode::UpdateSubscriptionGroupConfigStateCas => {
                let header = command
                    .decode_command_custom_header::<GetSubscriptionGroupConfigRequestHeader>()
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let group = Self::require_group(header.group.as_str())?;
                let body = serde_json::from_slice::<SupervisedSubscriptionGroupConfigCasRequestBody>(
                    Self::require_body(command, "supervised Subscription Group replacement")?,
                )
                .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let action = match body.expected_state {
                    ExpectedState::Absent => Action::Create,
                    ExpectedState::Present { .. } => Action::Update,
                };
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_group(group.to_owned()),
                    vec![action],
                    &source_ip,
                    &rpc_code,
                ));
            }
            RequestCode::UpdateConsumerOffsetConditional => {
                let header = command
                    .decode_command_custom_header::<UpdateConsumerOffsetConditionalHeader>()
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let topic = Self::require_topic(header.topic.as_str())?;
                let group = Self::require_group(header.consumer_group.as_str())?;
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_topic(topic),
                    vec![Action::Sub, Action::Update],
                    &source_ip,
                    &rpc_code,
                ));
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_group(group.to_owned()),
                    vec![Action::Sub, Action::Update],
                    &source_ip,
                    &rpc_code,
                ));
            }
            RequestCode::GetBrokerMutationConfig => self.push_java_annotation_cluster_default(
                &mut contexts,
                subject_key,
                vec![Action::Get],
                &source_ip,
                &rpc_code,
            ),
            RequestCode::GetMessageRequestMode => {
                let body = serde_json::from_slice::<GetMessageRequestModeRequestBody>(Self::require_body(
                    command,
                    "request-mode query",
                )?)
                .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let topic = Self::require_topic(&body.topic)?;
                let group = Self::require_group(&body.consumer_group)?;
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_topic(topic),
                    vec![Action::Get],
                    &source_ip,
                    &rpc_code,
                ));
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_group(group.to_owned()),
                    vec![Action::Get],
                    &source_ip,
                    &rpc_code,
                ));
            }
            RequestCode::SetMessageRequestModeCas => {
                let body = serde_json::from_slice::<SetMessageRequestModeCasRequestBody>(Self::require_body(
                    command,
                    "request-mode replacement",
                )?)
                .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let topic = Self::require_topic(&body.topic)?;
                let group = Self::require_group(&body.consumer_group)?;
                if let ExpectedMessageRequestMode::Present { mode, .. } = &body.expected_state {
                    if !matches!(mode.as_str(), "PULL" | "POP") {
                        return Err(AuthorizationError::InvalidContext(
                            "expected request mode is invalid".to_owned(),
                        ));
                    }
                }
                if !matches!(body.replacement.mode.as_str(), "PULL" | "POP") {
                    return Err(AuthorizationError::InvalidContext(
                        "replacement request mode is invalid".to_owned(),
                    ));
                }
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_topic(topic),
                    vec![Action::Update],
                    &source_ip,
                    &rpc_code,
                ));
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_group(group.to_owned()),
                    vec![Action::Update],
                    &source_ip,
                    &rpc_code,
                ));
            }
            RequestCode::GetRouteinfoByTopic => {
                if let Some(topic) = self.field_value(fields, TOPIC) {
                    if NamespaceUtil::is_retry_topic(topic) {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_group(topic.to_string()),
                            vec![Action::Sub, Action::Get],
                            &source_ip,
                            &rpc_code,
                        ));
                    } else {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_topic(topic),
                            vec![Action::Pub, Action::Sub, Action::Get],
                            &source_ip,
                            &rpc_code,
                        ));
                    }
                }
            }
            RequestCode::SendMessage => {
                if let Some(topic) = self.field_value(fields, TOPIC) {
                    if NamespaceUtil::is_retry_topic(topic) {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_group(topic.to_string()),
                            vec![Action::Sub],
                            &source_ip,
                            &rpc_code,
                        ));
                    } else {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_topic(topic),
                            vec![Action::Pub],
                            &source_ip,
                            &rpc_code,
                        ));
                    }
                }
            }
            RequestCode::SendMessageV2 | RequestCode::SendBatchMessage => {
                if let Some(topic) = self.field_value(fields, B) {
                    if NamespaceUtil::is_retry_topic(topic) {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_group(topic.to_string()),
                            vec![Action::Sub],
                            &source_ip,
                            &rpc_code,
                        ));
                    } else {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_topic(topic),
                            vec![Action::Pub],
                            &source_ip,
                            &rpc_code,
                        ));
                    }
                }
            }
            RequestCode::RecallMessage => {
                if let Some(topic) = self.field_value(fields, TOPIC) {
                    contexts.push(self.build_context(
                        subject_key,
                        Resource::of_topic(topic),
                        vec![Action::Pub],
                        &source_ip,
                        &rpc_code,
                    ));
                }
            }
            RequestCode::EndTransaction => {
                if let Some(topic) = self.field_value(fields, TOPIC) {
                    contexts.push(self.build_context(
                        subject_key,
                        Resource::of_topic(topic),
                        vec![Action::Pub],
                        &source_ip,
                        &rpc_code,
                    ));
                }
            }
            RequestCode::ConsumerSendMsgBack => {
                if let Some(group) = self.field_value(fields, GROUP) {
                    contexts.push(self.build_context(
                        subject_key,
                        Resource::of_group(group.to_string()),
                        vec![Action::Sub],
                        &source_ip,
                        &rpc_code,
                    ));
                }
            }
            RequestCode::PullMessage => {
                if let Some(topic) = self.field_value(fields, TOPIC) {
                    self.push_topic_sub_if_not_retry(
                        &mut contexts,
                        subject_key,
                        topic,
                        vec![Action::Sub],
                        &source_ip,
                        &rpc_code,
                    );
                }
                if let Some(group) = self.field_value(fields, CONSUMER_GROUP) {
                    contexts.push(self.build_context(
                        subject_key,
                        Resource::of_group(group.to_string()),
                        vec![Action::Sub],
                        &source_ip,
                        &rpc_code,
                    ));
                }
            }
            RequestCode::QueryMessage => {
                if let Some(topic) = self.field_value(fields, TOPIC) {
                    contexts.push(self.build_context(
                        subject_key,
                        Resource::of_topic(topic),
                        vec![Action::Sub, Action::Get],
                        &source_ip,
                        &rpc_code,
                    ));
                }
            }
            RequestCode::HeartBeat => {
                if let Some(body) = command.body() {
                    let heartbeat = SerdeJsonUtils::from_json_bytes::<HeartbeatData>(body)
                        .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                    for consumer in heartbeat.consumer_data_set {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_group(consumer.group_name.to_string()),
                            vec![Action::Sub],
                            &source_ip,
                            &rpc_code,
                        ));
                        for subscription in consumer.subscription_data_set {
                            self.push_topic_sub_if_not_retry(
                                &mut contexts,
                                subject_key,
                                subscription.topic.as_str(),
                                vec![Action::Sub],
                                &source_ip,
                                &rpc_code,
                            );
                        }
                    }
                }
            }
            RequestCode::UnregisterClient => {
                let header = command
                    .decode_command_custom_header::<UnregisterClientRequestHeader>()
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                if let Some(group) = header.consumer_group.as_deref() {
                    contexts.push(self.build_context(
                        subject_key,
                        Resource::of_group(group.to_string()),
                        vec![Action::Sub],
                        &source_ip,
                        &rpc_code,
                    ));
                }
            }
            RequestCode::GetConsumerListByGroup => {
                let header = command
                    .decode_command_custom_header::<GetConsumerListByGroupRequestHeader>()
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_group(header.consumer_group.to_string()),
                    vec![Action::Sub, Action::Get],
                    &source_ip,
                    &rpc_code,
                ));
            }
            RequestCode::QueryConsumerOffset => {
                let header = command
                    .decode_command_custom_header::<QueryConsumerOffsetRequestHeader>()
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                self.push_topic_sub_if_not_retry(
                    &mut contexts,
                    subject_key,
                    header.topic.as_str(),
                    vec![Action::Sub, Action::Get],
                    &source_ip,
                    &rpc_code,
                );
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_group(header.consumer_group.to_string()),
                    vec![Action::Sub, Action::Get],
                    &source_ip,
                    &rpc_code,
                ));
            }
            RequestCode::UpdateConsumerOffset => {
                let header = command
                    .decode_command_custom_header::<UpdateConsumerOffsetRequestHeader>()
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                self.push_topic_sub_if_not_retry(
                    &mut contexts,
                    subject_key,
                    header.topic.as_str(),
                    vec![Action::Sub, Action::Update],
                    &source_ip,
                    &rpc_code,
                );
                contexts.push(self.build_context(
                    subject_key,
                    Resource::of_group(header.consumer_group.to_string()),
                    vec![Action::Sub, Action::Update],
                    &source_ip,
                    &rpc_code,
                ));
            }
            RequestCode::LockBatchMq => {
                if let Some(body) = command.body() {
                    let body = SerdeJsonUtils::from_json_bytes::<LockBatchRequestBody>(body)
                        .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                    if let Some(group) = body.consumer_group.as_deref() {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_group(group.to_string()),
                            vec![Action::Sub],
                            &source_ip,
                            &rpc_code,
                        ));
                    }
                    for mq in body.mq_set {
                        self.push_topic_sub_if_not_retry(
                            &mut contexts,
                            subject_key,
                            mq.topic().as_str(),
                            vec![Action::Sub],
                            &source_ip,
                            &rpc_code,
                        );
                    }
                }
            }
            RequestCode::UnlockBatchMq => {
                if let Some(body) = command.body() {
                    let body = serde_json::from_slice::<UnlockBatchRequestBody>(body)
                        .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                    if let Some(group) = body.consumer_group.as_deref() {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_group(group.to_string()),
                            vec![Action::Sub],
                            &source_ip,
                            &rpc_code,
                        ));
                    }
                    for mq in body.mq_set {
                        self.push_topic_sub_if_not_retry(
                            &mut contexts,
                            subject_key,
                            mq.topic().as_str(),
                            vec![Action::Sub],
                            &source_ip,
                            &rpc_code,
                        );
                    }
                }
            }
            RequestCode::DeleteTopicInBrokerList => {
                let body = command
                    .body()
                    .ok_or_else(|| AuthorizationError::InvalidContext("batch topic delete body is missing".into()))?;
                let body = SerdeJsonUtils::from_json_bytes::<DeleteTopicListRequestBody>(body)
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let mut seen = HashSet::new();
                for topic in body.topic_list {
                    if seen.insert(topic.clone()) {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_topic(topic.as_str()),
                            vec![Action::Delete],
                            &source_ip,
                            &rpc_code,
                        ));
                    }
                }
            }
            RequestCode::DeleteSubscriptionGroupList => {
                let body = command
                    .body()
                    .ok_or_else(|| AuthorizationError::InvalidContext("batch group delete body is missing".into()))?;
                let body = SerdeJsonUtils::from_json_bytes::<DeleteSubscriptionGroupListRequestBody>(body)
                    .map_err(|error| AuthorizationError::InvalidContext(error.to_string()))?;
                let mut seen = HashSet::new();
                for group in body.group_name_list {
                    if seen.insert(group.clone()) {
                        contexts.push(self.build_context(
                            subject_key,
                            Resource::of_group(group.to_string()),
                            vec![Action::Delete],
                            &source_ip,
                            &rpc_code,
                        ));
                    }
                }
            }
            request_code => self.build_context_by_java_annotation_mapping(
                &mut contexts,
                subject_key,
                fields,
                &source_ip,
                &rpc_code,
                request_code,
            ),
        }

        if contexts.is_empty() && Self::supervised_request_requires_context(request_code) {
            return Err(AuthorizationError::InvalidContext(
                "supervised mutation authorization context is missing".to_owned(),
            ));
        }

        Ok(contexts)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use rocketmq_model::common::message::message_queue::MessageQueue;
    use rocketmq_protocol::code::request_code::RequestCode;

    use super::*;
    use crate::config::AuthConfig;

    fn command_with_fields(code: RequestCode, fields: &[(&str, &str)]) -> RemotingCommand {
        let ext_fields = fields
            .iter()
            .map(|(key, value)| (CheetahString::from(*key), CheetahString::from(*value)))
            .collect::<HashMap<_, _>>();
        RemotingCommand::create_remoting_command(code.to_i32()).set_ext_fields(ext_fields)
    }

    #[test]
    fn test_build_send_message_context() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig::default());
        let command = command_with_fields(
            RequestCode::SendMessage,
            &[("AccessKey", "alice"), ("topic", "test-topic")],
        );

        let contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &command)
            .unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].subject_key(), Some("User:alice"));
        assert_eq!(contexts[0].resource_key(), Some("Topic:test-topic".to_string()));
        assert_eq!(contexts[0].actions(), &[Action::Pub]);
    }

    #[test]
    fn test_build_pull_message_contexts() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig::default());
        let command = command_with_fields(
            RequestCode::PullMessage,
            &[
                ("AccessKey", "alice"),
                ("topic", "test-topic"),
                ("consumerGroup", "group-a"),
            ],
        );

        let contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &command)
            .unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].subject_key(), Some("User:alice"));
        assert_eq!(contexts[1].resource_key(), Some("Group:group-a".to_string()));
    }

    #[test]
    fn test_build_heartbeat_contexts() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig::default());
        let mut ext_fields = HashMap::new();
        ext_fields.insert(CheetahString::from("AccessKey"), CheetahString::from("alice"));

        let mut heartbeat = HeartbeatData::default();
        let mut consumer = rocketmq_protocol::protocol::heartbeat::consumer_data::ConsumerData {
            group_name: CheetahString::from("group-a"),
            ..Default::default()
        };
        let subscription = rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData {
            topic: CheetahString::from("topic-a"),
            ..Default::default()
        };
        consumer.subscription_data_set.insert(subscription);
        heartbeat.consumer_data_set.insert(consumer);

        let command = RemotingCommand::create_remoting_command(RequestCode::HeartBeat.to_i32())
            .set_ext_fields(ext_fields)
            .set_body(serde_json::to_vec(&heartbeat).unwrap());

        let contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &command)
            .unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].subject_key(), Some("User:alice"));
        assert_eq!(contexts[0].resource_key(), Some("Group:group-a".to_string()));
        assert_eq!(contexts[1].resource_key(), Some("Topic:topic-a".to_string()));
    }

    #[test]
    fn test_build_lock_batch_contexts() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig::default());
        let mut ext_fields = HashMap::new();
        ext_fields.insert(CheetahString::from("AccessKey"), CheetahString::from("alice"));

        let mut mq_set = HashSet::new();
        mq_set.insert(MessageQueue::from_parts("topic-a", "broker-a", 0));
        let body = LockBatchRequestBody {
            consumer_group: Some(CheetahString::from("group-a")),
            mq_set,
            ..Default::default()
        };

        let command = RemotingCommand::create_remoting_command(RequestCode::LockBatchMq.to_i32())
            .set_ext_fields(ext_fields)
            .set_body(serde_json::to_vec(&body).unwrap());

        let contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &command)
            .unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].resource_key(), Some("Group:group-a".to_string()));
        assert_eq!(contexts[1].resource_key(), Some("Topic:topic-a".to_string()));
    }

    #[test]
    fn test_build_annotation_fallback_topic_and_cluster_contexts() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig {
            cluster_name: CheetahString::from_static_str("DefaultCluster"),
            ..AuthConfig::default()
        });

        let topic_command = command_with_fields(
            RequestCode::UpdateAndCreateTopic,
            &[("AccessKey", "rocketmq"), ("topic", "topic")],
        );
        let topic_contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &topic_command)
            .unwrap();
        assert_eq!(topic_contexts.len(), 1);
        assert_eq!(topic_contexts[0].subject_key(), Some("User:rocketmq"));
        assert_eq!(topic_contexts[0].resource_key(), Some("Topic:topic".to_string()));
        assert_eq!(topic_contexts[0].actions(), &[Action::Create]);
        assert_eq!(
            topic_contexts[0].rpc_code(),
            Some(RequestCode::UpdateAndCreateTopic.to_i32().to_string().as_str())
        );

        let cluster_command = command_with_fields(
            RequestCode::AuthCreateUser,
            &[("AccessKey", "rocketmq"), ("username", "alice")],
        );
        let cluster_contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &cluster_command)
            .unwrap();
        assert_eq!(cluster_contexts.len(), 1);
        assert_eq!(cluster_contexts[0].subject_key(), Some("User:rocketmq"));
        assert_eq!(
            cluster_contexts[0].resource_key(),
            Some("Cluster:DefaultCluster".to_string())
        );
        assert_eq!(cluster_contexts[0].actions(), &[Action::Update]);
        assert_eq!(
            cluster_contexts[0].rpc_code(),
            Some(RequestCode::AuthCreateUser.to_i32().to_string().as_str())
        );
    }

    #[test]
    fn supervised_config_and_proxy_drain_codes_require_cluster_permissions() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig {
            cluster_name: CheetahString::from_static_str("DefaultCluster"),
            ..AuthConfig::default()
        });

        for code in [
            RequestCode::UpdateBrokerConfigCas,
            RequestCode::BeginProxyDrain,
            RequestCode::CancelProxyDrain,
        ] {
            let command = command_with_fields(code, &[("AccessKey", "rocketmq")]);
            let contexts = builder
                .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &command)
                .unwrap();
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].resource_key(), Some("Cluster:DefaultCluster".to_string()));
            assert_eq!(contexts[0].actions(), &[Action::Update]);
        }

        let query = command_with_fields(RequestCode::GetProxyDrainState, &[("AccessKey", "rocketmq")]);
        let contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &query)
            .unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].resource_key(), Some("Cluster:DefaultCluster".to_string()));
        assert_eq!(contexts[0].actions(), &[Action::Get]);
    }

    #[test]
    fn nameserver_admin_reads_never_produce_an_empty_authorization_context() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig {
            cluster_name: CheetahString::from_static_str("DefaultCluster"),
            ..AuthConfig::default()
        });

        for code in [
            RequestCode::GetBrokerClusterInfo,
            RequestCode::GetAllTopicListFromNameserver,
            RequestCode::GetSystemTopicListFromNs,
            RequestCode::GetUnitTopicList,
            RequestCode::GetHasUnitSubTopicList,
            RequestCode::GetHasUnitSubUnunitTopicList,
            RequestCode::GetNamesrvConfig,
        ] {
            let command = command_with_fields(code, &[("AccessKey", "reader")]);
            let contexts = builder
                .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &command)
                .unwrap();
            assert_eq!(contexts.len(), 1, "missing authorization context for {code:?}");
            assert_eq!(contexts[0].resource_key(), Some("Cluster:DefaultCluster".to_string()));
            assert_eq!(contexts[0].actions(), &[Action::Get]);
        }

        let member = command_with_fields(
            RequestCode::GetBrokerMemberGroup,
            &[("AccessKey", "reader"), ("clusterName", "ClusterA")],
        );
        let contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &member)
            .unwrap();
        assert_eq!(contexts[0].resource_key(), Some("Cluster:ClusterA".to_string()));
        assert_eq!(contexts[0].actions(), &[Action::Get]);

        let topics = command_with_fields(
            RequestCode::GetTopicsByCluster,
            &[("AccessKey", "reader"), ("cluster", "ClusterA")],
        );
        let contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &topics)
            .unwrap();
        assert_eq!(contexts[0].resource_key(), Some("Cluster:ClusterA".to_string()));
        assert_eq!(contexts[0].actions(), &[Action::List]);
    }

    #[test]
    fn supervised_mutation_codes_build_exact_non_empty_contexts() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig {
            cluster_name: CheetahString::from_static_str("DefaultCluster"),
            ..AuthConfig::default()
        });
        let auth_context = RemotingAuthContext::embedded("test-session");
        let access = [("AccessKey", "operator")];

        for (expected_state, action) in [
            (ExpectedState::Absent, Action::Create),
            (ExpectedState::Present { version: 7 }, Action::Update),
        ] {
            let topic = command_with_fields(
                RequestCode::UpdateTopicConfigStateCas,
                &[("AccessKey", "operator"), ("topic", "TopicA")],
            )
            .set_body(
                serde_json::to_vec(&SupervisedTopicConfigCasRequestBody {
                    expected_state,
                    replacement: rocketmq_protocol::protocol::body::supervised_mutation::SupervisedTopicConfig {
                        read_queue_nums: 4,
                        write_queue_nums: 4,
                        perm: 6,
                        order: false,
                        message_type: "NORMAL".to_owned(),
                    },
                })
                .unwrap(),
            );
            let contexts = builder.build_from_remoting(&auth_context, &topic).unwrap();
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].resource_key().as_deref(), Some("Topic:TopicA"));
            assert_eq!(contexts[0].actions(), &[action]);
            assert!(!contexts[0].actions().contains(&Action::Delete));

            let group = command_with_fields(
                RequestCode::UpdateSubscriptionGroupConfigStateCas,
                &[("AccessKey", "operator"), ("group", "GroupA")],
            )
            .set_body(
                serde_json::to_vec(&SupervisedSubscriptionGroupConfigCasRequestBody {
                    expected_state,
                    replacement:
                        rocketmq_protocol::protocol::body::supervised_mutation::SupervisedSubscriptionGroupConfig {
                            consume_enable: true,
                            consume_from_min_enable: true,
                            consume_broadcast_enable: true,
                            consume_message_orderly: false,
                            retry_queue_nums: 1,
                            retry_max_times: 16,
                            broker_id: 0,
                            which_broker_when_consume_slowly: 1,
                            notify_consumer_ids_changed_enable: true,
                            group_sys_flag: 0,
                            consume_timeout_minute: 15,
                        },
                })
                .unwrap(),
            );
            let contexts = builder.build_from_remoting(&auth_context, &group).unwrap();
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].resource_key().as_deref(), Some("Group:GroupA"));
            assert_eq!(contexts[0].actions(), &[action]);
            assert!(!contexts[0].actions().contains(&Action::Delete));
        }

        let offset = command_with_fields(
            RequestCode::UpdateConsumerOffsetConditional,
            &[
                ("AccessKey", "operator"),
                ("consumerGroup", "GroupA"),
                ("topic", "TopicA"),
                ("queueId", "0"),
                ("expectedOffset", "9"),
                ("newOffset", "4"),
            ],
        );
        let contexts = builder.build_from_remoting(&auth_context, &offset).unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].resource_key().as_deref(), Some("Topic:TopicA"));
        assert_eq!(contexts[1].resource_key().as_deref(), Some("Group:GroupA"));
        assert!(contexts
            .iter()
            .all(|context| context.actions() == [Action::Sub, Action::Update]));

        let broker = command_with_fields(RequestCode::GetBrokerMutationConfig, &access);
        let contexts = builder.build_from_remoting(&auth_context, &broker).unwrap();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].resource_key().as_deref(), Some("Cluster:DefaultCluster"));
        assert_eq!(contexts[0].actions(), &[Action::Get]);

        let get_mode = command_with_fields(RequestCode::GetMessageRequestMode, &access).set_body(
            serde_json::to_vec(&GetMessageRequestModeRequestBody {
                topic: "TopicA".to_owned(),
                consumer_group: "GroupA".to_owned(),
            })
            .unwrap(),
        );
        let contexts = builder.build_from_remoting(&auth_context, &get_mode).unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].actions(), &[Action::Get]);
        assert_eq!(contexts[1].actions(), &[Action::Get]);

        let set_mode = command_with_fields(RequestCode::SetMessageRequestModeCas, &access).set_body(
            serde_json::to_vec(&SetMessageRequestModeCasRequestBody {
                topic: "TopicA".to_owned(),
                consumer_group: "GroupA".to_owned(),
                expected_state: ExpectedMessageRequestMode::Absent,
                replacement: rocketmq_protocol::protocol::body::supervised_mutation::SupervisedMessageRequestMode {
                    mode: "POP".to_owned(),
                    pop_share_queue_num: 4,
                },
            })
            .unwrap(),
        );
        let contexts = builder.build_from_remoting(&auth_context, &set_mode).unwrap();
        assert_eq!(contexts.len(), 2);
        assert!(contexts.iter().all(|context| context.actions() == [Action::Update]));
    }

    #[test]
    fn supervised_mutation_codes_reject_missing_or_malformed_authorization_inputs() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig {
            cluster_name: CheetahString::from_static_str("DefaultCluster"),
            ..AuthConfig::default()
        });
        let auth_context = RemotingAuthContext::embedded("test-session");
        let invalid = [
            command_with_fields(
                RequestCode::UpdateTopicConfigStateCas,
                &[("AccessKey", "operator"), ("topic", "TopicA")],
            ),
            command_with_fields(
                RequestCode::UpdateSubscriptionGroupConfigStateCas,
                &[("AccessKey", "operator"), ("group", "GroupA")],
            )
            .set_body(b"not-json".to_vec()),
            command_with_fields(
                RequestCode::UpdateConsumerOffsetConditional,
                &[
                    ("AccessKey", "operator"),
                    ("consumerGroup", "GroupA"),
                    ("topic", "TopicA"),
                    ("queueId", "0"),
                ],
            ),
            command_with_fields(RequestCode::GetMessageRequestMode, &[("AccessKey", "operator")]),
            command_with_fields(RequestCode::SetMessageRequestModeCas, &[("AccessKey", "operator")]).set_body(
                br#"{"topic":"TopicA","consumerGroup":"GroupA","expectedState":{"kind":"absent"},"replacement":{"mode":"INVALID","popShareQueueNum":0}}"#
                    .to_vec(),
            ),
        ];
        for command in invalid {
            assert!(
                builder.build_from_remoting(&auth_context, &command).is_err(),
                "malformed {:?} must fail before dispatch",
                RequestCode::from(command.code())
            );
        }
    }

    #[test]
    fn batch_delete_builds_one_deduplicated_context_per_resource() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig::default());
        let topic_body =
            rocketmq_protocol::protocol::body::delete_topic_list_request_body::DeleteTopicListRequestBody {
                topic_list: vec!["TopicA".into(), "TopicB".into(), "TopicA".into()],
            };
        let topic_command = RemotingCommand::create_remoting_command(RequestCode::DeleteTopicInBrokerList.to_i32())
            .set_body(serde_json::to_vec(&topic_body).unwrap());

        let topic_contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &topic_command)
            .unwrap();
        assert_eq!(2, topic_contexts.len());
        assert_eq!(Some("Topic:TopicA".to_string()), topic_contexts[0].resource_key());
        assert_eq!(Some("Topic:TopicB".to_string()), topic_contexts[1].resource_key());
        assert!(topic_contexts
            .iter()
            .all(|context| context.actions() == [Action::Delete]));

        let group_body = rocketmq_protocol::protocol::body::delete_subscription_group_list_request_body::DeleteSubscriptionGroupListRequestBody {
            group_name_list: vec!["GroupA".into(), "GroupA".into(), "GroupB".into()],
            clean_offset: true,
        };
        let group_command = RemotingCommand::create_remoting_command(RequestCode::DeleteSubscriptionGroupList.to_i32())
            .set_body(serde_json::to_vec(&group_body).unwrap());

        let group_contexts = builder
            .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &group_command)
            .unwrap();
        assert_eq!(2, group_contexts.len());
        assert_eq!(Some("Group:GroupA".to_string()), group_contexts[0].resource_key());
        assert_eq!(Some("Group:GroupB".to_string()), group_contexts[1].resource_key());
        assert!(group_contexts
            .iter()
            .all(|context| context.actions() == [Action::Delete]));
    }

    #[test]
    fn batch_delete_rejects_missing_or_malformed_body() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig::default());
        for command in [
            RemotingCommand::create_remoting_command(RequestCode::DeleteTopicInBrokerList.to_i32()),
            RemotingCommand::create_remoting_command(RequestCode::DeleteSubscriptionGroupList.to_i32())
                .set_body(b"not-json".to_vec()),
        ] {
            assert!(builder
                .build_from_remoting(&RemotingAuthContext::embedded("test-session"), &command)
                .is_err());
        }
    }

    #[test]
    fn missing_typed_ingress_metadata_is_rejected() {
        let builder = DefaultAuthorizationContextBuilder::new(AuthConfig::default());
        let command = command_with_fields(
            RequestCode::SendMessage,
            &[("AccessKey", "alice"), ("topic", "test-topic")],
        );

        let error = builder
            .build_from_remoting(&RemotingAuthContext::default(), &command)
            .expect_err("missing trusted session metadata must fail closed");
        assert!(matches!(error, AuthorizationError::InvalidContext(_)));

        let command_without_ext_fields = RemotingCommand::create_remoting_command(RequestCode::SendMessage.to_i32());
        let error = builder
            .build_from_remoting(&RemotingAuthContext::default(), &command_without_ext_fields)
            .expect_err("missing trusted metadata must fail closed before the no-context fast path");
        assert!(matches!(error, AuthorizationError::InvalidContext(_)));
    }
}
