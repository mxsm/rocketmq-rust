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

use std::any::Any;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_common::common::action::Action;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;

use crate::authentication::enums::subject_type::SubjectType;
use crate::authorization::builder::AuthorizationContextBuilder;
use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::model::resource::Resource;
use crate::authorization::provider::AuthorizationError;
use crate::authorization::provider::AuthorizationResult;
use crate::config::AuthConfig;

const ACCESS_KEY: &str = "AccessKey";
const TOPIC: &str = "topic";
const GROUP: &str = "group";
const CONSUMER_GROUP: &str = "consumerGroup";
const B: &str = "b";

#[derive(Clone, Debug)]
pub struct DefaultAuthorizationContextBuilder {
    #[allow(dead_code)]
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

    fn subject_key(&self, command: &RemotingCommand) -> Option<String> {
        command
            .ext_fields()
            .and_then(|fields| self.field_value(fields, ACCESS_KEY))
            .map(|username| format!("{}:{}", SubjectType::User.name(), username))
    }

    fn source_ip(&self, channel_context: &dyn Any) -> String {
        if let Some(ctx) = channel_context.downcast_ref::<ConnectionHandlerContext>() {
            return ctx.remote_address().ip().to_string();
        }

        if let Some(ctx) = channel_context.downcast_ref::<ConnectionHandlerContextWrapper>() {
            return ctx.remote_address().ip().to_string();
        }

        String::from("unknown")
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
}

impl AuthorizationContextBuilder for DefaultAuthorizationContextBuilder {
    fn build_from_remoting(
        &self,
        channel_context: &dyn Any,
        command: &RemotingCommand,
    ) -> AuthorizationResult<Vec<DefaultAuthorizationContext>> {
        let mut contexts = Vec::new();
        let fields = match command.ext_fields() {
            Some(fields) => fields,
            None => return Ok(contexts),
        };

        let subject_key = self.subject_key(command);
        let subject_key = subject_key.as_deref();
        let source_ip = self.source_ip(channel_context);
        let rpc_code = command.code().to_string();

        match RequestCode::from(command.code()) {
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
            _ => {}
        }

        Ok(contexts)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_remoting::code::request_code::RequestCode;

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

        let contexts = builder.build_from_remoting(&(), &command).unwrap();
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

        let contexts = builder.build_from_remoting(&(), &command).unwrap();
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
        let mut consumer = rocketmq_remoting::protocol::heartbeat::consumer_data::ConsumerData {
            group_name: CheetahString::from("group-a"),
            ..Default::default()
        };
        let subscription = rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData {
            topic: CheetahString::from("topic-a"),
            ..Default::default()
        };
        consumer.subscription_data_set.insert(subscription);
        heartbeat.consumer_data_set.insert(consumer);

        let command = RemotingCommand::create_remoting_command(RequestCode::HeartBeat.to_i32())
            .set_ext_fields(ext_fields)
            .set_body(serde_json::to_vec(&heartbeat).unwrap());

        let contexts = builder.build_from_remoting(&(), &command).unwrap();
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

        let contexts = builder.build_from_remoting(&(), &command).unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].resource_key(), Some("Group:group-a".to_string()));
        assert_eq!(contexts[1].resource_key(), Some("Topic:topic-a".to_string()));
    }
}
