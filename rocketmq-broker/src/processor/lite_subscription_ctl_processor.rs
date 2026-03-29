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
use rocketmq_common::common::lite::to_lmq_name;
use rocketmq_common::common::lite::LiteSubscriptionAction;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::lite_subscription_ctl_request_body::LiteSubscriptionCtlRequestBody;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;

pub(crate) struct LiteSubscriptionCtlProcessor<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> LiteSubscriptionCtlProcessor<MS> {
    pub(crate) fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS: MessageStore> RequestProcessor for LiteSubscriptionCtlProcessor<MS> {
    async fn process_request(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let body = match request.body() {
            Some(body) if !body.is_empty() => body,
            _ => {
                return Ok(Some(self.response_with_code(
                    request,
                    ResponseCode::IllegalOperation,
                    "Request body is null.",
                )));
            }
        };

        let request_body = match SerdeJsonUtils::from_json_bytes::<LiteSubscriptionCtlRequestBody>(body.as_ref()) {
            Ok(body) => body,
            Err(error) => {
                return Ok(Some(self.response_with_code(
                    request,
                    ResponseCode::IllegalOperation,
                    format!("Failed to decode LiteSubscriptionCtlRequestBody: {error}"),
                )));
            }
        };

        if request_body.subscription_set().is_empty() {
            return Ok(Some(self.response_with_code(
                request,
                ResponseCode::IllegalOperation,
                "LiteSubscriptionCtlRequestBody is empty.",
            )));
        }

        for entry in request_body.subscription_set() {
            if entry.client_id().is_empty() {
                warn!("clientId is blank, {}", entry);
                continue;
            }
            if entry.group().is_empty() {
                warn!("group is blank, {}", entry);
                continue;
            }
            if entry.topic().is_empty() {
                warn!("topic is blank, {}", entry);
                continue;
            }

            let registry = self.broker_runtime_inner.lite_subscription_registry();
            let lmq_name_set = self.to_lmq_name_set(entry.topic(), entry.lite_topic_set());

            match entry.action() {
                LiteSubscriptionAction::PartialAdd => {
                    if let Err((code, remark)) = self.validate_consumer_group(entry.group(), entry.topic()) {
                        return Ok(Some(self.response_with_code(request, code, remark)));
                    };
                    registry.update_client_channel(entry.client_id(), channel.clone());
                    registry.add_partial_subscription(entry.client_id(), entry.group(), entry.topic(), &lmq_name_set);
                }
                LiteSubscriptionAction::PartialRemove => {
                    registry.remove_partial_subscription(
                        entry.client_id(),
                        entry.group(),
                        entry.topic(),
                        &lmq_name_set,
                    );
                }
                LiteSubscriptionAction::CompleteAdd => {
                    if let Err((code, remark)) = self.validate_consumer_group(entry.group(), entry.topic()) {
                        return Ok(Some(self.response_with_code(request, code, remark)));
                    };
                    registry.update_client_channel(entry.client_id(), channel.clone());
                    registry.add_complete_subscription(
                        entry.client_id(),
                        entry.group(),
                        entry.topic(),
                        &lmq_name_set,
                        entry.version(),
                    );
                }
                LiteSubscriptionAction::CompleteRemove => {
                    registry.remove_complete_subscription(entry.client_id());
                }
            }
        }

        Ok(Some(
            RemotingCommand::create_response_command().set_opaque(request.opaque()),
        ))
    }
}

impl<MS: MessageStore> LiteSubscriptionCtlProcessor<MS> {
    fn validate_consumer_group(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
    ) -> Result<
        Arc<rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig>,
        (ResponseCode, CheetahString),
    > {
        let group_config = self
            .broker_runtime_inner
            .subscription_group_manager()
            .subscription_group_table()
            .get(group)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| {
                (
                    ResponseCode::SubscriptionGroupNotExist,
                    CheetahString::from_string(format!("Group [{}] not exist.", group)),
                )
            })?;

        if !group_config.consume_enable() {
            return Err((
                ResponseCode::IllegalOperation,
                CheetahString::from_static_str("Consumer group is not allowed to consume."),
            ));
        }

        match group_config.lite_bind_topic() {
            Some(bind_topic) if bind_topic == topic => Ok(group_config),
            _ => Err((
                ResponseCode::InvalidParameter,
                CheetahString::from_string(format!("Subscription [{}]-[{}] not match.", group, topic)),
            )),
        }
    }

    fn to_lmq_name_set(
        &self,
        topic: &CheetahString,
        lite_topic_set: &HashSet<CheetahString>,
    ) -> HashSet<CheetahString> {
        lite_topic_set
            .iter()
            .filter_map(|lite_topic| to_lmq_name(topic.as_str(), lite_topic.as_str()).map(CheetahString::from_string))
            .collect()
    }

    fn response_with_code(
        &self,
        request: &RemotingCommand,
        code: ResponseCode,
        remark: impl Into<CheetahString>,
    ) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code_remark(code, remark).set_opaque(request.opaque())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::time::SystemTime;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_common::common::attribute::subscription_group_attributes::LITE_BIND_TOPIC_ATTRIBUTE_NAME;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::lite::to_lmq_name;
    use rocketmq_common::common::lite::LiteSubscriptionAction;
    use rocketmq_common::common::lite::LiteSubscriptionDTO;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::body::lite_subscription_ctl_request_body::LiteSubscriptionCtlRequestBody;
    use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-lite-subscription-{label}-{millis}"))
    }

    async fn new_test_runtime(label: &str) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = std::sync::Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = std::sync::Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await);
        runtime
    }

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn complete_add_updates_registry_with_lmq_names() {
        let mut runtime = new_test_runtime("processor-success").await;
        let mut inner = runtime.inner_for_test().clone();
        let mut config =
            rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::new(
                CheetahString::from_static_str("lite-group"),
            );
        config.set_attributes(HashMap::from([(
            CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
            CheetahString::from_static_str("parent-topic"),
        )]));
        inner
            .subscription_group_manager_mut()
            .update_subscription_group_config(&mut config);

        let dto = LiteSubscriptionDTO::new()
            .with_action(LiteSubscriptionAction::CompleteAdd)
            .with_client_id(CheetahString::from_static_str("client-id"))
            .with_group(CheetahString::from_static_str("lite-group"))
            .with_topic(CheetahString::from_static_str("parent-topic"))
            .with_lite_topic_set(HashSet::from([CheetahString::from_static_str("child-a")]));
        let mut body = LiteSubscriptionCtlRequestBody::new();
        body.set_subscription_set(vec![dto]);
        let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")));

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut processor = LiteSubscriptionCtlProcessor::new(inner.clone());
        let response = processor
            .process_request(channel, ctx, &mut request)
            .await
            .expect("processor request should succeed")
            .expect("processor should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let subscription = inner
            .lite_subscription_registry()
            .lite_subscription(
                &CheetahString::from_static_str("client-id"),
                &CheetahString::from_static_str("lite-group"),
                &CheetahString::from_static_str("parent-topic"),
            )
            .expect("registry should contain lite subscription");
        assert!(subscription.lite_topic_set().contains(&CheetahString::from_string(
            to_lmq_name("parent-topic", "child-a").expect("convert lite topic")
        )));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
