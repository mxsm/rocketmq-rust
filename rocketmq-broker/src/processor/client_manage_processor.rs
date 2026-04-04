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
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::IS_SUB_CHANGE;
use rocketmq_common::common::mix_all::IS_SUPPORT_HEART_BEAT_V2;
use rocketmq_common::common::sys_flag::topic_sys_flag;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_filter::filter::FilterFactory;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::check_client_request_body::CheckClientRequestBody;
use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::client::client_channel_info::ClientChannelInfo;

pub struct ClientManageProcessor<MS: MessageStore> {
    consumer_group_heartbeat_table:
        Arc<parking_lot::RwLock<HashMap<CheetahString /* ConsumerGroup */, i32 /* HeartbeatFingerprint */>>>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> RequestProcessor for ClientManageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("ClientManageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::HeartBeat | RequestCode::UnregisterClient | RequestCode::CheckClientConfig => {
                self.process_request_inner(channel, ctx, request_code, request).await
            }
            _ => {
                warn!(
                    "ClientManageProcessor received unknown request code: {:?}",
                    request_code
                );
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("ClientManageProcessor request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }
}

impl<MS> ClientManageProcessor<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self {
            consumer_group_heartbeat_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            broker_runtime_inner,
        }
    }
}

impl<MS> ClientManageProcessor<MS>
where
    MS: MessageStore,
{
    async fn process_request_inner(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match request_code {
            RequestCode::HeartBeat => self.heart_beat(channel, ctx, request).await,
            RequestCode::UnregisterClient => self.unregister_client(channel, ctx, request),
            RequestCode::CheckClientConfig => self.check_client_config(request),
            _ => Ok(Some(
                RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("ClientManageProcessor request code {} not supported", request.code()),
                )
                .set_opaque(request.opaque()),
            )),
        }
    }

    fn check_client_config(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();
        let Some(body) = request.body() else {
            return Ok(Some(response));
        };

        let request_body = SerdeJsonUtils::from_json_bytes::<CheckClientRequestBody>(body.as_ref())?;
        let subscription_data = request_body.get_subscription_data();
        if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
            return Ok(Some(response));
        }

        if !self.broker_runtime_inner.broker_config().enable_property_filter {
            return Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                "The broker does not support consumer to filter message by {}",
                subscription_data.expression_type
            ))));
        }

        match FilterFactory::instance().get(subscription_data.expression_type.as_str()) {
            Some(filter) => match filter.compile(subscription_data.sub_string.as_str()) {
                Ok(_) => Ok(Some(response)),
                Err(error) => Ok(Some(
                    response
                        .set_code(ResponseCode::SubscriptionParseFailed)
                        .set_remark(error.to_string()),
                )),
            },
            None => Ok(Some(
                response
                    .set_code(ResponseCode::SubscriptionParseFailed)
                    .set_remark(format!("unsupported filter type {}", subscription_data.expression_type)),
            )),
        }
    }

    fn unregister_client(
        &self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<UnregisterClientRequestHeader>()?;

        let client_channel_info = ClientChannelInfo::new(
            channel,
            request_header.client_id.clone(),
            request.language(),
            request.version(),
        );

        if let Some(ref group) = request_header.producer_group {
            self.broker_runtime_inner
                .producer_manager()
                .unregister_producer(group, &client_channel_info, &ctx);
        }

        if let Some(ref group) = request_header.consumer_group {
            let subscription_group_config = self
                .broker_runtime_inner
                .subscription_group_manager()
                .find_subscription_group_config(group);
            let is_notify_consumer_ids_changed_enable =
                if let Some(ref subscription_group_config) = subscription_group_config {
                    subscription_group_config.notify_consumer_ids_changed_enable()
                } else {
                    true
                };
            self.broker_runtime_inner.consumer_manager().unregister_consumer(
                group,
                &client_channel_info,
                is_notify_consumer_ids_changed_enable,
            );
        }

        Ok(Some(RemotingCommand::create_response_command()))
    }

    async fn heart_beat(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let heartbeat_data =
            SerdeJsonUtils::from_json_bytes::<HeartbeatData>(request.body().as_ref().map(|v| v.as_ref()).unwrap())
                .unwrap();
        let client_channel_info = ClientChannelInfo::new(
            channel.clone(),
            heartbeat_data.client_id.clone(),
            request.language(),
            request.version(),
        );
        if heartbeat_data.heartbeat_fingerprint != 0 {
            return Ok(self
                .heart_beat_v2(&channel, &ctx, heartbeat_data, client_channel_info)
                .await);
        }

        //do consumer data handle
        for consumer_data in heartbeat_data.consumer_data_set.iter() {
            if self.broker_runtime_inner.broker_config().reject_pull_consumer_enable
                && ConsumeType::ConsumeActively == consumer_data.consume_type
            {
                continue;
            }
            self.consumer_group_heartbeat_table
                .write()
                .insert(consumer_data.group_name.clone(), heartbeat_data.heartbeat_fingerprint);
            let mut has_order_topic_sub = false;
            for subscription_data in consumer_data.subscription_data_set.iter() {
                if self
                    .broker_runtime_inner
                    .topic_config_manager()
                    .is_order_topic(subscription_data.topic.as_str())
                {
                    has_order_topic_sub = true;
                    break;
                }
            }
            let subscription_group_config = self
                .broker_runtime_inner
                .subscription_group_manager()
                .find_subscription_group_config(consumer_data.group_name.as_ref());
            if subscription_group_config.is_none() {
                continue;
            }
            let subscription_group_config = subscription_group_config.unwrap();
            let is_notify_consumer_ids_changed_enable = subscription_group_config.notify_consumer_ids_changed_enable();
            let topic_sys_flag = if consumer_data.unit_mode {
                topic_sys_flag::build_sys_flag(false, true)
            } else {
                0
            };
            let new_topic = CheetahString::from_string(mix_all::get_retry_topic(consumer_data.group_name.as_str()));
            self.broker_runtime_inner
                .topic_config_manager_mut()
                .create_topic_in_send_message_back_method(
                    &new_topic,
                    subscription_group_config.retry_queue_nums(),
                    PermName::PERM_WRITE | PermName::PERM_READ,
                    has_order_topic_sub,
                    topic_sys_flag,
                )
                .await;
            let changed = self.broker_runtime_inner.consumer_manager().register_consumer(
                consumer_data.group_name.as_ref(),
                client_channel_info.clone(),
                consumer_data.consume_type,
                consumer_data.message_model,
                consumer_data.consume_from_where,
                consumer_data.subscription_data_set.clone(),
                is_notify_consumer_ids_changed_enable,
            );
            if changed {
                info!(
                    "ClientManageProcessor: registerConsumer info changed, SDK address={}, consumerData={:?}",
                    channel.remote_address(),
                    consumer_data
                )
            }
        }
        //do producer data handle
        for producer_data in heartbeat_data.producer_data_set.iter() {
            self.broker_runtime_inner
                .producer_manager()
                .register_producer(&producer_data.group_name, &client_channel_info);
        }
        let mut response_command = RemotingCommand::create_response_command();
        response_command.ensure_ext_fields_initialized();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), true.to_string());
        Ok(Some(response_command))
    }

    async fn heart_beat_v2(
        &mut self,
        channel: &Channel,
        _ctx: &ConnectionHandlerContext,
        heartbeat_data: HeartbeatData,
        client_channel_info: ClientChannelInfo,
    ) -> Option<RemotingCommand> {
        let mut is_sub_change = false;
        for consumer_data in heartbeat_data.consumer_data_set.iter() {
            if self.broker_runtime_inner.broker_config().reject_pull_consumer_enable
                && ConsumeType::ConsumeActively == consumer_data.consume_type
            {
                continue;
            }
            if self
                .consumer_group_heartbeat_table
                .read()
                .get(&consumer_data.group_name)
                .is_some_and(|fingerprint| *fingerprint != heartbeat_data.heartbeat_fingerprint)
            {
                is_sub_change = true;
            }
            self.consumer_group_heartbeat_table
                .write()
                .insert(consumer_data.group_name.clone(), heartbeat_data.heartbeat_fingerprint);
            let mut has_order_topic_sub = false;
            for subscription_data in consumer_data.subscription_data_set.iter() {
                if self
                    .broker_runtime_inner
                    .topic_config_manager()
                    .is_order_topic(subscription_data.topic.as_str())
                {
                    has_order_topic_sub = true;
                    break;
                }
            }

            let Some(subscription_group_config) = self
                .broker_runtime_inner
                .subscription_group_manager()
                .find_subscription_group_config(consumer_data.group_name.as_ref())
            else {
                continue;
            };
            let is_notify_consumer_ids_changed_enable = subscription_group_config.notify_consumer_ids_changed_enable();
            let topic_sys_flag = if consumer_data.unit_mode {
                topic_sys_flag::build_sys_flag(false, true)
            } else {
                0
            };
            let new_topic = CheetahString::from_string(mix_all::get_retry_topic(consumer_data.group_name.as_str()));
            self.broker_runtime_inner
                .topic_config_manager_mut()
                .create_topic_in_send_message_back_method(
                    &new_topic,
                    subscription_group_config.retry_queue_nums(),
                    PermName::PERM_WRITE | PermName::PERM_READ,
                    has_order_topic_sub,
                    topic_sys_flag,
                )
                .await;

            let changed = if heartbeat_data.is_without_sub {
                self.broker_runtime_inner
                    .consumer_manager()
                    .register_consumer_without_sub(
                        consumer_data.group_name.as_ref(),
                        client_channel_info.clone(),
                        consumer_data.consume_type,
                        consumer_data.message_model,
                        consumer_data.consume_from_where,
                        is_notify_consumer_ids_changed_enable,
                    )
            } else {
                self.broker_runtime_inner.consumer_manager().register_consumer(
                    consumer_data.group_name.as_ref(),
                    client_channel_info.clone(),
                    consumer_data.consume_type,
                    consumer_data.message_model,
                    consumer_data.consume_from_where,
                    consumer_data.subscription_data_set.clone(),
                    is_notify_consumer_ids_changed_enable,
                )
            };

            if changed {
                info!(
                    "heartBeatV2 ClientManageProcessor: registerConsumer info changed, SDK address={}, \
                     consumerData={:?}",
                    channel.remote_address(),
                    consumer_data
                );
            }
        }

        //handle producer data
        for producer_data in heartbeat_data.producer_data_set.iter() {
            self.broker_runtime_inner
                .producer_manager()
                .register_producer(&producer_data.group_name, &client_channel_info);
        }
        let mut response_command = RemotingCommand::create_response_command();
        response_command.ensure_ext_fields_initialized();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), is_sub_change.to_string());
        Some(response_command)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::path::PathBuf;

    use bytes::Bytes;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_common::common::filter::expression_type::ExpressionType;
    use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
    use rocketmq_remoting::protocol::heartbeat::consumer_data::ConsumerData;
    use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use tokio::net::TcpStream;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::code::response_code::ResponseCode as RemotingResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;

    fn temp_test_root(label: &str) -> PathBuf {
        let unique = format!(
            "rocketmq-broker-client-manage-{label}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    async fn new_test_runtime(label: &str, enable_property_filter: bool) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = std::sync::Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            enable_property_filter,
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
        let tcp_stream = TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    fn check_request(subscription_data: SubscriptionData) -> RemotingCommand {
        let body = CheckClientRequestBody::new("client-id".to_string(), "group-a".to_string(), subscription_data);
        RemotingCommand::create_request_command(RequestCode::CheckClientConfig, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")))
    }

    #[tokio::test]
    async fn check_client_config_rejects_property_filter_when_disabled() {
        let mut runtime = new_test_runtime("check-client-filter-disabled", false).await;
        let inner = runtime.inner_for_test().clone();
        let processor = ClientManageProcessor::new(inner);
        let mut request = check_request(SubscriptionData {
            topic: "topic-a".into(),
            sub_string: "a > 1".into(),
            expression_type: ExpressionType::SQL92.into(),
            ..Default::default()
        });

        let response = processor
            .check_client_config(&mut request)
            .expect("check client config should succeed")
            .expect("processor should return response");

        assert_eq!(
            RemotingResponseCode::from(response.code()),
            RemotingResponseCode::SystemError
        );
        assert!(response
            .remark()
            .expect("remark should be set")
            .contains("does not support consumer to filter message"));
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn check_client_config_accepts_valid_property_filter_when_enabled() {
        let mut runtime = new_test_runtime("check-client-filter-enabled-valid", true).await;
        let inner = runtime.inner_for_test().clone();
        let processor = ClientManageProcessor::new(inner);
        let mut request = check_request(SubscriptionData {
            topic: "topic-a".into(),
            sub_string: "region IN ('hz', 'sh') AND name CONTAINS 'rocket' AND score BETWEEN 0 AND 100".into(),
            expression_type: ExpressionType::SQL92.into(),
            ..Default::default()
        });

        let response = processor
            .check_client_config(&mut request)
            .expect("check client config should succeed")
            .expect("processor should return response");

        assert_eq!(
            RemotingResponseCode::from(response.code()),
            RemotingResponseCode::Success
        );
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn check_client_config_rejects_invalid_property_filter_when_enabled() {
        let mut runtime = new_test_runtime("check-client-filter-enabled-invalid", true).await;
        let inner = runtime.inner_for_test().clone();
        let processor = ClientManageProcessor::new(inner);
        let mut request = check_request(SubscriptionData {
            topic: "topic-a".into(),
            sub_string: "a >".into(),
            expression_type: ExpressionType::SQL92.into(),
            ..Default::default()
        });

        let response = processor
            .check_client_config(&mut request)
            .expect("check client config should succeed")
            .expect("processor should return response");

        assert_eq!(
            RemotingResponseCode::from(response.code()),
            RemotingResponseCode::SubscriptionParseFailed
        );
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn heart_beat_v2_without_sub_registers_consumer_and_marks_sub_change() {
        let mut runtime = new_test_runtime("heartbeat-v2-without-sub", false).await;
        let inner = runtime.inner_for_test().clone();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let client_channel_info = ClientChannelInfo::new(channel.clone(), "client-id".into(), Default::default(), 0);
        let mut processor = ClientManageProcessor::new(inner.clone());
        processor
            .consumer_group_heartbeat_table
            .write()
            .insert("group-a".into(), 1);

        let heartbeat_data = HeartbeatData {
            client_id: "client-id".into(),
            heartbeat_fingerprint: 2,
            is_without_sub: true,
            consumer_data_set: HashSet::from([ConsumerData {
                group_name: "group-a".into(),
                consume_type: ConsumeType::ConsumePassively,
                message_model: MessageModel::Clustering,
                consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
                subscription_data_set: HashSet::new(),
                unit_mode: false,
            }]),
            producer_data_set: HashSet::new(),
        };

        let response = processor
            .heart_beat_v2(&channel, &ctx, heartbeat_data, client_channel_info)
            .await
            .expect("processor should return response");

        assert_eq!(
            RemotingResponseCode::from(response.code()),
            RemotingResponseCode::Success
        );
        let ext_fields = response.ext_fields().expect("ext fields should exist");
        assert_eq!(
            ext_fields
                .get(&CheetahString::from_static_str(IS_SUB_CHANGE))
                .map(|value| value.as_str()),
            Some("true")
        );

        let consumer_group_info = inner
            .consumer_manager()
            .get_consumer_group_info(&CheetahString::from_static_str("group-a"))
            .expect("consumer should be registered");
        assert!(consumer_group_info.get_subscription_table().is_empty());
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
