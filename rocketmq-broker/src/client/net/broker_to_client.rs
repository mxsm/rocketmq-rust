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
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::mq_version::RocketMqVersion;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::body::response::get_consumer_status_body::GetConsumerStatusBody;
use rocketmq_protocol::protocol::body::response::reset_offset_body::ResetOffsetBody;
use rocketmq_protocol::protocol::body::response::reset_offset_body::ResetOffsetBodyForC;
use rocketmq_protocol::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_status_request_header::GetConsumerStatusRequestHeader;
use rocketmq_protocol::protocol::header::notify_consumer_ids_changed_request_header::NotifyConsumerIdsChangedRequestHeader;
use rocketmq_protocol::protocol::header::notify_unsubscribe_lite_request_header::NotifyUnsubscribeLiteRequestHeader;
use rocketmq_protocol::protocol::header::reset_offset_request_header::ResetOffsetRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_store::BrokerAdminStore;
use rocketmq_transport::api::ServerPushCommand;
use rocketmq_transport::api::ServerPushSender;
use rocketmq_transport::api::ServerRequestCommand;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker::broker_admin_runtime::BrokerAdminRuntime;

/// Minimum client version required for reset offset and get consumer status operations.
/// Java uses V3_0_7_SNAPSHOT.ordinal()
const MIN_CLIENT_VERSION: i32 = RocketMqVersion::V3_0_7_SNAPSHOT as i32;

#[derive(Clone, Copy)]
pub struct Broker2Client {
    command_factory: RemotingCommandFactory,
}

impl Default for Broker2Client {
    fn default() -> Self {
        Self::new(application_remoting_command_factory())
    }
}

impl Broker2Client {
    pub const fn new(command_factory: RemotingCommandFactory) -> Self {
        Self { command_factory }
    }

    pub(crate) const fn command_factory(&self) -> &RemotingCommandFactory {
        &self.command_factory
    }

    /// Check producer transaction state.
    /// This is a oneway request - exceptions are logged but not propagated.
    ///
    /// # Arguments
    /// * `group` - Producer group name
    /// * `sender` - The producer session's typed push capability
    /// * `request_header` - Transaction state check request header
    /// * `message_ext` - The message to check
    pub async fn check_producer_transaction_state(
        &self,
        group: &CheetahString,
        sender: &ServerPushSender,
        request_header: CheckTransactionStateRequestHeader,
        message_ext: MessageExt,
    ) {
        let msg_id = message_ext.msg_id().clone();
        let body = match MessageDecoder::encode(&message_ext, false) {
            Ok(body) => body,
            Err(e) => {
                error!(
                    "Check transaction failed because encode message error. group={}, msgId={}, error={:?}",
                    group, msg_id, e
                );
                return;
            }
        };
        // Java uses timeout=10ms for oneway
        if let Err(e) = sender
            .send(
                ServerPushCommand::CheckTransactionState {
                    header: request_header,
                    body,
                },
                Duration::from_millis(10),
            )
            .await
        {
            error!(
                "Check transaction failed because invoke producer exception. group={}, msgId={}, error={:?}",
                group, msg_id, e
            );
        }
    }

    /// Notify consumer that consumer IDs have changed in the group.
    /// This triggers consumer rebalance.
    ///
    /// # Arguments
    /// * `sender` - The consumer session's typed push capability
    /// * `consumer_group` - The consumer group name
    pub async fn notify_consumer_ids_changed(&self, sender: &ServerPushSender, consumer_group: &CheetahString) {
        if consumer_group.is_empty() {
            error!("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        let request_header = NotifyConsumerIdsChangedRequestHeader {
            consumer_group: consumer_group.clone(),
            rpc_request_header: None,
        };
        // Java uses timeout=10ms for oneway
        if let Err(e) = sender
            .send(
                ServerPushCommand::NotifyConsumerIdsChanged {
                    header: request_header,
                    opaque: None,
                },
                Duration::from_millis(10),
            )
            .await
        {
            warn!(
                "notifyConsumerIdsChanged exception. group={}, error={:?}",
                consumer_group, e
            );
        }
    }

    /// Notify Lite clients that a Lite subscription has been removed.
    ///
    /// Java sends this as a one-way broker-to-client callback with a short timeout.
    pub async fn notify_unsubscribe_lite(
        &self,
        sender: &ServerPushSender,
        request_header: NotifyUnsubscribeLiteRequestHeader,
    ) {
        let lite_topic = request_header.lite_topic.clone();
        let consumer_group = request_header.consumer_group.clone();
        let client_id = request_header.client_id.clone();
        if let Err(e) = sender
            .send(
                ServerPushCommand::NotifyUnsubscribeLite {
                    header: request_header,
                    opaque: None,
                },
                Duration::from_millis(100),
            )
            .await
        {
            error!(
                "notifyUnsubscribeLite failed. liteTopic={}, group={}, clientId={}, error={:?}",
                lite_topic, consumer_group, client_id, e
            );
        }
    }

    /// Reset consumer offset for a topic.
    ///
    /// # Arguments
    /// * `broker_inner` - Reference to the broker runtime inner
    /// * `topic` - Topic name
    /// * `group` - Consumer group name
    /// * `timestamp` - Target timestamp (-1 means max offset)
    /// * `is_force` - Whether to force reset even if new offset > current offset
    ///
    /// # Returns
    /// Response command with result
    pub async fn reset_offset<MS: BrokerAdminStore>(
        &self,
        broker_inner: &BrokerAdminRuntime<MS>,
        topic: &CheetahString,
        group: &CheetahString,
        timestamp: i64,
        is_force: bool,
    ) -> RemotingCommand {
        self.reset_offset_inner(broker_inner, topic, group, timestamp, is_force, false)
            .await
    }

    /// Reset consumer offset for a topic (C++ client version).
    ///
    /// # Arguments
    /// * `broker_inner` - Reference to the broker runtime inner
    /// * `topic` - Topic name
    /// * `group` - Consumer group name
    /// * `timestamp` - Target timestamp (-1 means max offset)
    /// * `is_force` - Whether to force reset even if new offset > current offset
    ///
    /// # Returns
    /// Response command with result
    pub async fn reset_offset_for_c<MS: BrokerAdminStore>(
        &self,
        broker_inner: &BrokerAdminRuntime<MS>,
        topic: &CheetahString,
        group: &CheetahString,
        timestamp: i64,
        is_force: bool,
    ) -> RemotingCommand {
        self.reset_offset_inner(broker_inner, topic, group, timestamp, is_force, true)
            .await
    }

    /// Internal reset offset implementation.
    async fn reset_offset_inner<MS: BrokerAdminStore>(
        &self,
        broker_inner: &BrokerAdminRuntime<MS>,
        topic: &CheetahString,
        group: &CheetahString,
        timestamp: i64,
        is_force: bool,
        is_c: bool,
    ) -> RemotingCommand {
        let mut response = self.command_factory.create_java_default_error_response_command();

        // Check if topic exists
        let topic_config = broker_inner.topic_config_manager().select_topic_config(topic);
        if topic_config.is_none() {
            error!(
                "[reset-offset] reset offset failed, no topic in this broker. topic={}",
                topic
            );
            response.set_code_ref(ResponseCode::TopicNotExist);
            response.set_remark_mut(format!(
                "[reset-offset] reset offset failed, no topic in this broker. topic={}",
                topic
            ));
            return response;
        }
        let topic_config = topic_config.unwrap();

        let mut offset_table: HashMap<MessageQueue, i64> = HashMap::new();
        let broker_name = broker_inner.broker_config().broker_name().clone();
        let write_queue_nums = topic_config.write_queue_nums;

        for i in 0..write_queue_nums {
            let queue_id = i as i32;
            let consumer_offset = broker_inner
                .consumer_offset_manager()
                .query_offset(group, topic, queue_id);
            if consumer_offset == -1 {
                response.set_code_ref(ResponseCode::SystemError);
                response.set_remark_mut(format!("The consumer group <{}> not exist", group));
                return response;
            }

            let message_store = broker_inner.message_store();
            let timestamp_offset = if timestamp == -1 {
                // Get max offset
                message_store
                    .map(|store| store.get_max_offset_in_queue(topic, queue_id))
                    .unwrap_or(0)
            } else {
                match message_store {
                    Some(store) => match store
                        .get_offset_in_queue_by_time_async(topic, queue_id, timestamp)
                        .await
                    {
                        Ok(offset) => offset,
                        Err(error) => {
                            warn!(
                                "reset offset by timestamp failed. topic={}, queueId={}, timestamp={}, error={}",
                                topic, queue_id, timestamp, error
                            );
                            0
                        }
                    },
                    None => 0,
                }
            };

            let timestamp_offset = if timestamp_offset < 0 {
                warn!(
                    "reset offset is invalid. topic={}, queueId={}, timeStampOffset={}",
                    topic, i, timestamp_offset
                );
                0
            } else {
                timestamp_offset
            };

            let mq = MessageQueue::from_parts(topic.clone(), broker_name.clone(), queue_id);
            if is_force || timestamp_offset < consumer_offset {
                offset_table.insert(mq, timestamp_offset);
            } else {
                offset_table.insert(mq, consumer_offset);
            }
        }

        // Build request to send to consumers
        let request_header = ResetOffsetRequestHeader {
            topic: topic.clone(),
            group: group.clone(),
            queue_id: -1,
            offset: None,
            timestamp,
            is_force,
            topic_request_header: None,
        };
        // Use different body format for C++ clients
        let request_body = if is_c {
            let body = ResetOffsetBodyForC::from_offset_table(&offset_table);
            body.encode()
        } else {
            let body = ResetOffsetBody {
                offset_table: offset_table.clone(),
            };
            body.encode()
        };

        // Notify all consumers in the group
        let session_snapshot = broker_inner
            .consumer_manager()
            .session_registry()
            .transport_snapshot(group);
        if !session_snapshot.is_empty() {
            for (client, transport) in session_snapshot {
                let version = client.version();
                if version >= MIN_CLIENT_VERSION {
                    // Java uses timeout=5000ms for oneway
                    if let Err(e) = transport
                        .push_sender()
                        .send(
                            ServerPushCommand::ResetConsumerClientOffset {
                                header: request_header.clone(),
                                body: request_body.clone().into(),
                            },
                            Duration::from_millis(5_000),
                        )
                        .await
                    {
                        error!(
                            "[reset-offset] reset offset exception. topic={}, group={}, error={:?}",
                            topic, group, e
                        );
                    } else {
                        info!(
                            "[reset-offset] reset offset success. topic={}, group={}, clientId={}",
                            topic,
                            group,
                            client.client_id()
                        );
                    }
                } else {
                    let version_desc = RocketMqVersion::from_ordinal(version as u32).name();
                    response.set_code_ref(ResponseCode::SystemError);
                    response.set_remark_mut(format!(
                        "the client does not support this feature. version={}",
                        version_desc
                    ));
                    warn!(
                        "[reset-offset] the client does not support this feature. session={:?}, version={}",
                        client.session_id(),
                        version_desc
                    );
                    return response;
                }
            }
        } else {
            let error_info = format!(
                "Consumer not online, so can not reset offset, Group: {} Topic: {} Timestamp: {}",
                group, topic, timestamp
            );
            error!("{}", error_info);
            response.set_code_ref(ResponseCode::ConsumerNotOnline);
            response.set_remark_mut(error_info);
            return response;
        }

        let res_body = ResetOffsetBody { offset_table };
        self.command_factory
            .create_success_response_command()
            .set_body(res_body.encode())
    }

    /// Get consumer status from connected clients.
    ///
    /// # Arguments
    /// * `broker_inner` - Reference to the broker runtime inner
    /// * `topic` - Topic name
    /// * `group` - Consumer group name
    /// * `origin_client_id` - Optional specific client ID to query
    ///
    /// # Returns
    /// Response command with consumer status
    pub async fn get_consume_status<MS: BrokerAdminStore>(
        &self,
        broker_inner: &BrokerAdminRuntime<MS>,
        topic: &CheetahString,
        group: &CheetahString,
        origin_client_id: Option<&CheetahString>,
    ) -> RemotingCommand {
        let mut result = self.command_factory.create_java_default_error_response_command();

        let request_header = GetConsumerStatusRequestHeader::new(topic.clone(), group.clone());

        let mut consumer_status_table: HashMap<CheetahString, HashMap<MessageQueue, i64>> = HashMap::new();

        let channel_info_snapshot = broker_inner
            .consumer_manager()
            .session_registry()
            .transport_snapshot(group);

        if channel_info_snapshot.is_empty() {
            result.set_code_ref(ResponseCode::SystemError);
            result.set_remark_mut(format!("No Any Consumer online in the consumer group: [{}]", group));
            return result;
        }

        for (channel_info, transport) in channel_info_snapshot {
            let version = channel_info.version();
            let client_id = channel_info.client_id().clone();

            if version < MIN_CLIENT_VERSION {
                let version_desc = RocketMqVersion::from_ordinal(version as u32).name();
                result.set_code_ref(ResponseCode::SystemError);
                result.set_remark_mut(format!(
                    "the client does not support this feature. version={}",
                    version_desc
                ));
                warn!(
                    "[get-consumer-status] the client does not support this feature. session={:?}, version={}",
                    channel_info.session_id(),
                    version_desc
                );
                return result;
            }

            // Check if we should query this client
            let should_query = origin_client_id
                .map(|id| id.is_empty() || id == &client_id)
                .unwrap_or(true);

            if should_query {
                // Java uses timeout=5000ms
                match transport
                    .request_sender()
                    .request(
                        ServerRequestCommand::GetConsumerStatusFromClient {
                            header: request_header.clone(),
                        },
                        Duration::from_millis(5_000),
                    )
                    .await
                {
                    Ok(response) => {
                        if response.code() == ResponseCode::Success as i32 {
                            if let Some(body_bytes) = response.body() {
                                if let Some(body) = GetConsumerStatusBody::decode(body_bytes.as_ref()) {
                                    consumer_status_table.insert(client_id.clone(), body.message_queue_table);
                                    info!(
                                        "[get-consumer-status] get consumer status success. topic={}, group={}, \
                                         channelRemoteAddr={}",
                                        topic, group, client_id
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "[get-consumer-status] get consumer status exception. topic={}, group={}, error={:?}",
                            topic, group, e
                        );
                    }
                }

                // If specific client was requested and found, stop iterating
                if let Some(id) = origin_client_id {
                    if !id.is_empty() && id == &client_id {
                        break;
                    }
                }
            }
        }

        let res_body = GetConsumerStatusBody {
            message_queue_table: HashMap::new(),
            consumer_table: consumer_status_table,
        };
        self.command_factory
            .create_success_response_command()
            .set_body(res_body.encode())
    }
}
