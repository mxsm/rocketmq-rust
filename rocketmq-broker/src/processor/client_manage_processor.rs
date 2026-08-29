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
use std::sync::Arc;

use crate::config::broker_config::BrokerConfig;
use cheetah_string::CheetahString;
use rocketmq_filter::filter::FilterFactory;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::IS_SUB_CHANGE;
use rocketmq_model::common::mix_all::IS_SUPPORT_HEART_BEAT_V2;
use rocketmq_model::common::sys_flag::topic_sys_flag;
use rocketmq_model::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::check_client_request_body::CheckClientRequestBody;
use rocketmq_protocol::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_store::BrokerStorePort;
use rocketmq_transport::api::v1::request_code_not_supported_with_factory_remark_and_opaque;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::SessionId;
use rocketmq_transport::api::v2::SessionView;
use tracing::info;
use tracing::warn;

use crate::client::client_channel_info::ClientChannelInfo;
use crate::client::client_channel_info::ClientSessionInfo;
use crate::client::manager::consumer_manager::ConsumerClientRegistration;
use crate::client::manager::consumer_manager::ConsumerSessionRegistration;
use crate::client::manager::producer_manager::ProducerClientRegistration;
use crate::client::session_transition_locks::ClientSessionTransitionGuard;
use crate::client::session_transition_locks::ClientSessionTransitionLocks;
use crate::processor::response_plan::immediate_outcome_from_command_result;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::transaction::queue::transaction_topic_registration::TransactionTopicRegistration;

pub struct ClientManageProcessor<MS: BrokerStorePort> {
    command_factory: RemotingCommandFactory,
    consumer_group_heartbeat_table:
        Arc<parking_lot::RwLock<HashMap<CheetahString /* ConsumerGroup */, i32 /* HeartbeatFingerprint */>>>,
    broker_config: Arc<BrokerConfig>,
    topic_config_manager: Arc<TopicConfigManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    producer_registration: ProducerClientRegistration,
    consumer_registration: ConsumerClientRegistration,
    session_transition_locks: Arc<ClientSessionTransitionLocks>,
    retry_topic_registration: Arc<TransactionTopicRegistration<MS>>,
}

pub(crate) struct ClientManageProcessorContext<MS: BrokerStorePort> {
    pub(crate) command_factory: RemotingCommandFactory,
    pub(crate) broker_config: Arc<BrokerConfig>,
    pub(crate) topic_config_manager: Arc<TopicConfigManager>,
    pub(crate) subscription_group_lookup: SubscriptionGroupConfigLookup,
    pub(crate) producer_registration: ProducerClientRegistration,
    pub(crate) consumer_registration: ConsumerClientRegistration,
    pub(crate) retry_topic_registration: Arc<TransactionTopicRegistration<MS>>,
}

/// Compatibility adapter that contains the only raw legacy Channel ingress for
/// client heartbeat and unregister requests.
pub(crate) struct LegacyClientManageProcessor<MS: BrokerStorePort> {
    processor: ClientManageProcessor<MS>,
}

#[derive(Clone)]
enum RegisteredClient {
    Legacy(Box<ClientChannelInfo>),
    Session(ClientSessionInfo),
}

impl<MS> RequestProcessorV2 for ClientManageProcessor<MS>
where
    MS: BrokerStorePort + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_v2_shared(request).await
    }
}

impl<MS: BrokerStorePort> ClientManageProcessor<MS> {
    pub(crate) async fn process_v2_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original_opaque = request.original_identity().original_opaque();
        let session_id = request.session().id();
        let remote_address = trusted_remote_address(request)?;
        let result = self
            .process_session_command(session_id, remote_address, request.command_mut())
            .await;
        immediate_outcome_from_command_result(
            &self.command_factory,
            result,
            original_opaque,
            "client manager returned no response",
        )
    }
}

impl<MS> ClientManageProcessor<MS>
where
    MS: BrokerStorePort,
{
    pub(crate) fn new(context: ClientManageProcessorContext<MS>) -> Self {
        let producer_session_transition_locks = context.producer_registration.session_transition_locks();
        let session_transition_locks = context.consumer_registration.session_transition_locks();
        assert!(
            Arc::ptr_eq(&producer_session_transition_locks, &session_transition_locks),
            "producer and consumer session registries must share one heartbeat transition lock"
        );
        Self {
            command_factory: context.command_factory,
            consumer_group_heartbeat_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            broker_config: context.broker_config,
            topic_config_manager: context.topic_config_manager,
            subscription_group_lookup: context.subscription_group_lookup,
            producer_registration: context.producer_registration,
            consumer_registration: context.consumer_registration,
            session_transition_locks,
            retry_topic_registration: context.retry_topic_registration,
        }
    }

    pub(crate) fn legacy_adapter(&self) -> LegacyClientManageProcessor<MS> {
        LegacyClientManageProcessor {
            processor: self.clone(),
        }
    }
}

impl<MS> LegacyClientManageProcessor<MS>
where
    MS: BrokerStorePort,
{
    pub(crate) async fn process_legacy(
        &mut self,
        channel: Channel,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("ClientManageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::HeartBeat => {
                let heartbeat = decode_heartbeat(request)?;
                let client = RegisteredClient::Legacy(Box::new(ClientChannelInfo::new(
                    channel.clone(),
                    heartbeat.client_id.clone(),
                    request.language(),
                    request.version(),
                )));
                self.processor
                    .heart_beat(channel.remote_address().to_string(), heartbeat, client)
                    .await
            }
            RequestCode::UnregisterClient => {
                let header = request.decode_command_custom_header::<UnregisterClientRequestHeader>()?;
                let client = RegisteredClient::Legacy(Box::new(ClientChannelInfo::new(
                    channel,
                    header.client_id.clone(),
                    request.language(),
                    request.version(),
                )));
                self.processor.unregister_client(header, client)
            }
            RequestCode::CheckClientConfig => self.processor.check_client_config(request),
            _ => self.processor.unsupported(request, request_code),
        }
    }
}

impl<MS> ClientManageProcessor<MS>
where
    MS: BrokerStorePort,
{
    async fn process_session_command(
        &self,
        session_id: SessionId,
        remote_address: String,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("ClientManageProcessor received V2 request code: {:?}", request_code);
        match request_code {
            RequestCode::HeartBeat => {
                let heartbeat = decode_heartbeat(request)?;
                let client = RegisteredClient::Session(ClientSessionInfo::new(
                    session_id,
                    heartbeat.client_id.clone(),
                    Some(remote_address.clone().into()),
                    request.language(),
                    request.version(),
                ));
                self.heart_beat(remote_address, heartbeat, client).await
            }
            RequestCode::UnregisterClient => {
                let header = request.decode_command_custom_header::<UnregisterClientRequestHeader>()?;
                let client = RegisteredClient::Session(ClientSessionInfo::new(
                    session_id,
                    header.client_id.clone(),
                    Some(remote_address.into()),
                    request.language(),
                    request.version(),
                ));
                self.unregister_client(header, client)
            }
            RequestCode::CheckClientConfig => self.check_client_config(request),
            _ => self.unsupported(request, request_code),
        }
    }

    fn unsupported(
        &self,
        request: &RemotingCommand,
        request_code: RequestCode,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        warn!(
            "ClientManageProcessor received unknown request code: {:?}",
            request_code
        );
        Ok(Some(request_code_not_supported_with_factory_remark_and_opaque(
            &self.command_factory,
            request.code(),
            format!("ClientManageProcessor request code {} not supported", request.code()),
            request.opaque(),
        )))
    }

    fn check_client_config(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = self.command_factory.create_success_response_command();
        let Some(body) = request.body() else {
            return Ok(Some(response));
        };

        let request_body = SerdeJsonUtils::from_json_bytes::<CheckClientRequestBody>(body.as_ref())?;
        let subscription_data = request_body.get_subscription_data();
        if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
            return Ok(Some(response));
        }

        if !self.broker_config.enable_property_filter {
            return Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(format!(
                "The broker does not support consumer to filter message by {}",
                subscription_data.expression_type
            ))));
        }

        match FilterFactory::instance().get(subscription_data.expression_type.as_str()) {
            Some(filter) => match filter.try_compile(subscription_data.sub_string.as_str()) {
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
        request_header: UnregisterClientRequestHeader,
        client: RegisteredClient,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if let Some(ref group) = request_header.producer_group {
            match &client {
                RegisteredClient::Legacy(client) => self.producer_registration.unregister_producer(group, client),
                RegisteredClient::Session(client) => self
                    .producer_registration
                    .unregister_producer_session(group, client.session_id()),
            }
        }

        if let Some(ref group) = request_header.consumer_group {
            let subscription_group_config = self.subscription_group_lookup.find_subscription_group_config(group);
            let is_notify_consumer_ids_changed_enable =
                if let Some(ref subscription_group_config) = subscription_group_config {
                    subscription_group_config.notify_consumer_ids_changed_enable()
                } else {
                    true
                };
            match &client {
                RegisteredClient::Legacy(client) => {
                    self.consumer_registration
                        .unregister_consumer(group, client, is_notify_consumer_ids_changed_enable)
                }
                RegisteredClient::Session(client) => self.consumer_registration.unregister_consumer_session(
                    group,
                    client.session_id(),
                    is_notify_consumer_ids_changed_enable,
                ),
            }
        }

        Ok(Some(self.command_factory.create_success_response_command()))
    }

    async fn heart_beat(
        &self,
        remote_address: String,
        heartbeat_data: HeartbeatData,
        client: RegisteredClient,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if heartbeat_data.heartbeat_fingerprint != 0 {
            return self.heart_beat_v2(&remote_address, heartbeat_data, client).await;
        }
        if let RegisteredClient::Session(session) = &client {
            self.ensure_known_cross_role_session_identity(session)?;
        }

        //do consumer data handle
        let mut consumer_session_registrations = Vec::new();
        for consumer_data in heartbeat_data.consumer_data_set.iter() {
            if self.broker_config.reject_pull_consumer_enable
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
                    .topic_config_manager
                    .is_order_topic(subscription_data.topic.as_str())
                {
                    has_order_topic_sub = true;
                    break;
                }
            }
            let subscription_group_config = self
                .subscription_group_lookup
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
            let _ = self
                .retry_topic_registration
                .select_or_create_send_back_topic_with(
                    &new_topic,
                    subscription_group_config.retry_queue_nums(),
                    has_order_topic_sub,
                    topic_sys_flag,
                )
                .await;
            let changed = match client.clone() {
                RegisteredClient::Legacy(client) => self.consumer_registration.register_consumer(
                    consumer_data.group_name.as_ref(),
                    *client,
                    consumer_data.consume_type,
                    consumer_data.message_model,
                    consumer_data.consume_from_where,
                    consumer_data.subscription_data_set.clone(),
                    is_notify_consumer_ids_changed_enable,
                ),
                RegisteredClient::Session(_) => {
                    consumer_session_registrations.push(ConsumerSessionRegistration {
                        group: consumer_data.group_name.clone(),
                        consume_type: consumer_data.consume_type,
                        message_model: consumer_data.message_model,
                        consume_from_where: consumer_data.consume_from_where,
                        subscriptions: consumer_data.subscription_data_set.clone(),
                        notify_consumer_ids_changed: is_notify_consumer_ids_changed_enable,
                        update_subscription: true,
                    });
                    false
                }
            };
            if changed {
                info!(
                    "ClientManageProcessor: registerConsumer info changed, SDK address={}, consumerData={:?}",
                    remote_address, consumer_data
                )
            }
        }
        //do producer data handle
        match client {
            RegisteredClient::Legacy(client) => {
                for producer_data in &heartbeat_data.producer_data_set {
                    self.producer_registration
                        .register_producer(&producer_data.group_name, &client);
                }
            }
            RegisteredClient::Session(session) => {
                let groups = heartbeat_data
                    .producer_data_set
                    .iter()
                    .map(|producer| producer.group_name.clone())
                    .collect();
                let transition = self
                    .session_transition_locks
                    .lock(session.client_id(), session.session_id());
                self.ensure_cross_role_session_identity(&transition, &session)?;
                let consumer_batch = self.consumer_registration.prepare_consumer_sessions(
                    &transition,
                    session.clone(),
                    consumer_session_registrations,
                );
                let producer_batch = self
                    .producer_registration
                    .prepare_producer_sessions(&transition, groups, session);
                drop(transition);
                let changed_groups = self.consumer_registration.complete_consumer_sessions(consumer_batch);
                self.producer_registration.complete_producer_sessions(producer_batch);
                if !changed_groups.is_empty() {
                    info!(
                        "ClientManageProcessor: consumer session registrations changed, SDK address={}, groups={:?}",
                        remote_address, changed_groups
                    );
                }
            }
        }
        let mut response_command = self.command_factory.create_success_response_command();
        response_command.ensure_ext_fields_initialized();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), true.to_string());
        Ok(Some(response_command))
    }

    async fn heart_beat_v2(
        &self,
        remote_address: &str,
        heartbeat_data: HeartbeatData,
        client: RegisteredClient,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if let RegisteredClient::Session(session) = &client {
            self.ensure_known_cross_role_session_identity(session)?;
        }
        let mut is_sub_change = false;
        let mut consumer_session_registrations = Vec::new();
        for consumer_data in heartbeat_data.consumer_data_set.iter() {
            if self.broker_config.reject_pull_consumer_enable
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
                    .topic_config_manager
                    .is_order_topic(subscription_data.topic.as_str())
                {
                    has_order_topic_sub = true;
                    break;
                }
            }

            let Some(subscription_group_config) = self
                .subscription_group_lookup
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
            let _ = self
                .retry_topic_registration
                .select_or_create_send_back_topic_with(
                    &new_topic,
                    subscription_group_config.retry_queue_nums(),
                    has_order_topic_sub,
                    topic_sys_flag,
                )
                .await;

            let changed = if heartbeat_data.is_without_sub {
                match client.clone() {
                    RegisteredClient::Legacy(client) => self.consumer_registration.register_consumer_without_sub(
                        consumer_data.group_name.as_ref(),
                        *client,
                        consumer_data.consume_type,
                        consumer_data.message_model,
                        consumer_data.consume_from_where,
                        is_notify_consumer_ids_changed_enable,
                    ),
                    RegisteredClient::Session(_) => {
                        consumer_session_registrations.push(ConsumerSessionRegistration {
                            group: consumer_data.group_name.clone(),
                            consume_type: consumer_data.consume_type,
                            message_model: consumer_data.message_model,
                            consume_from_where: consumer_data.consume_from_where,
                            subscriptions: HashSet::new(),
                            notify_consumer_ids_changed: is_notify_consumer_ids_changed_enable,
                            update_subscription: false,
                        });
                        false
                    }
                }
            } else {
                match client.clone() {
                    RegisteredClient::Legacy(client) => self.consumer_registration.register_consumer(
                        consumer_data.group_name.as_ref(),
                        *client,
                        consumer_data.consume_type,
                        consumer_data.message_model,
                        consumer_data.consume_from_where,
                        consumer_data.subscription_data_set.clone(),
                        is_notify_consumer_ids_changed_enable,
                    ),
                    RegisteredClient::Session(_) => {
                        consumer_session_registrations.push(ConsumerSessionRegistration {
                            group: consumer_data.group_name.clone(),
                            consume_type: consumer_data.consume_type,
                            message_model: consumer_data.message_model,
                            consume_from_where: consumer_data.consume_from_where,
                            subscriptions: consumer_data.subscription_data_set.clone(),
                            notify_consumer_ids_changed: is_notify_consumer_ids_changed_enable,
                            update_subscription: true,
                        });
                        false
                    }
                }
            };

            if changed {
                info!(
                    "heartBeatV2 ClientManageProcessor: registerConsumer info changed, SDK address={}, \
                     consumerData={:?}",
                    remote_address, consumer_data
                );
            }
        }

        //handle producer data
        match client {
            RegisteredClient::Legacy(client) => {
                for producer_data in &heartbeat_data.producer_data_set {
                    self.producer_registration
                        .register_producer(&producer_data.group_name, &client);
                }
            }
            RegisteredClient::Session(session) => {
                let groups = heartbeat_data
                    .producer_data_set
                    .iter()
                    .map(|producer| producer.group_name.clone())
                    .collect();
                let transition = self
                    .session_transition_locks
                    .lock(session.client_id(), session.session_id());
                self.ensure_cross_role_session_identity(&transition, &session)?;
                let consumer_batch = self.consumer_registration.prepare_consumer_sessions(
                    &transition,
                    session.clone(),
                    consumer_session_registrations,
                );
                let producer_batch = self
                    .producer_registration
                    .prepare_producer_sessions(&transition, groups, session);
                drop(transition);
                let changed_groups = self.consumer_registration.complete_consumer_sessions(consumer_batch);
                self.producer_registration.complete_producer_sessions(producer_batch);
                if !changed_groups.is_empty() {
                    is_sub_change = true;
                    info!(
                        "heartBeatV2 ClientManageProcessor: consumer session registrations changed, SDK address={}, \
                         groups={:?}",
                        remote_address, changed_groups
                    );
                }
            }
        }
        let mut response_command = self.command_factory.create_success_response_command();
        response_command.ensure_ext_fields_initialized();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), is_sub_change.to_string());
        Ok(Some(response_command))
    }

    fn ensure_cross_role_session_identity(
        &self,
        transition: &ClientSessionTransitionGuard<'_>,
        session: &ClientSessionInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        assert!(
            self.session_transition_locks
                .covers(transition, session.client_id(), session.session_id()),
            "session identity preflight requires the matching transition guard"
        );
        self.ensure_known_cross_role_session_identity(session)
    }

    fn ensure_known_cross_role_session_identity(
        &self,
        session: &ClientSessionInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        let conflicts = [
            self.consumer_registration.client_id_for_session(session.session_id()),
            self.producer_registration.client_id_for_session(session.session_id()),
        ]
        .into_iter()
        .flatten()
        .any(|client_id| client_id.as_str() != session.client_id().as_str());
        if conflicts {
            return Err(rocketmq_error::RocketMQError::request_body_invalid(
                "HEART_BEAT",
                "a live SessionId cannot change client identity",
            ));
        }
        Ok(())
    }
}

impl<MS: BrokerStorePort> Clone for ClientManageProcessor<MS> {
    fn clone(&self) -> Self {
        Self {
            command_factory: self.command_factory,
            consumer_group_heartbeat_table: self.consumer_group_heartbeat_table.clone(),
            broker_config: Arc::clone(&self.broker_config),
            topic_config_manager: Arc::clone(&self.topic_config_manager),
            subscription_group_lookup: self.subscription_group_lookup.clone(),
            producer_registration: self.producer_registration.clone(),
            consumer_registration: self.consumer_registration.clone(),
            session_transition_locks: Arc::clone(&self.session_transition_locks),
            retry_topic_registration: Arc::clone(&self.retry_topic_registration),
        }
    }
}

fn decode_heartbeat(request: &RemotingCommand) -> rocketmq_error::RocketMQResult<HeartbeatData> {
    let body = request
        .body()
        .ok_or_else(|| rocketmq_error::RocketMQError::request_body_invalid("HEART_BEAT", "request body is empty"))?;
    SerdeJsonUtils::from_json_bytes(body.as_ref())
}

fn trusted_remote_address(request: &RemotingRequest) -> rocketmq_error::RocketMQResult<String> {
    let origin = match request.origin() {
        RequestOrigin::Network { peer } => TrustedOriginFact::Network(peer.address()),
        RequestOrigin::Embedded { .. } => TrustedOriginFact::Embedded,
        _ => TrustedOriginFact::Unsupported,
    };
    let session = match request.session() {
        SessionView::Network { remote_addr, .. } => TrustedSessionFact::Network(*remote_addr),
        SessionView::Embedded { .. } => TrustedSessionFact::Embedded,
        _ => TrustedSessionFact::Unsupported,
    };
    trusted_remote_address_from_facts(origin, session)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TrustedOriginFact {
    Network(std::net::SocketAddr),
    Embedded,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TrustedSessionFact {
    Network(std::net::SocketAddr),
    Embedded,
    Unsupported,
}

fn trusted_remote_address_from_facts(
    origin: TrustedOriginFact,
    session: TrustedSessionFact,
) -> rocketmq_error::RocketMQResult<String> {
    match (origin, session) {
        (TrustedOriginFact::Network(peer), TrustedSessionFact::Network(remote_addr)) if peer == remote_addr => {
            Ok(remote_addr.to_string())
        }
        (TrustedOriginFact::Embedded, TrustedSessionFact::Embedded) => Ok("embedded".to_string()),
        _ => Err(rocketmq_error::RocketMQError::invariant_violated(
            "client manager request origin does not match its session view",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::PathBuf;

    use crate::config::broker_config::BrokerConfig;
    use bytes::Bytes;
    use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_model::common::filter::expression_type::ExpressionType;
    use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
    use rocketmq_protocol::protocol::heartbeat::consumer_data::ConsumerData;
    use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_protocol::protocol::heartbeat::producer_data::ProducerData;
    use rocketmq_security_api::AuthenticatedRequestContext;
    use rocketmq_security_api::Decision;
    use rocketmq_security_api::Principal;
    use rocketmq_security_api::RequestPolicy;
    use rocketmq_store::MessageStoreConfig;
    use tokio::net::TcpStream;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;
    use crate::long_polling::pull_deferred::PullSessionClientLookup;
    use rocketmq_protocol::code::response_code::ResponseCode as RemotingResponseCode;
    use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_transport::api::v1::AdmissionController;
    use rocketmq_transport::api::v1::AdmissionLimits;
    use rocketmq_transport::api::v1::TransportSecurity;
    use rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2;
    use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;
    use rocketmq_transport::test_support::session_id_for_test;
    use rocketmq_transport::test_support::Connection;
    use rocketmq_transport::test_support::EmbeddedRequestHarnessV2;

    struct AllowEmbeddedPolicy;

    impl RequestPolicy for AllowEmbeddedPolicy {
        fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
            Decision::Allow
        }
    }

    async fn dispatch_v2<P>(processor: P, command: RemotingCommand) -> EmbeddedDispatchOutcome
    where
        P: RequestProcessorV2 + Clone + Sync + 'static,
    {
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
            processor,
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        let harness = EmbeddedRequestHarnessV2::new(
            dispatcher,
            crate::test_task_group("client-manage-v2"),
            Principal::new("client-manage-v2-test"),
        );
        harness
            .dispatch(None, command)
            .await
            .expect("client manager V2 dispatch should complete")
    }

    struct ObservedClientManageProcessor<MS: BrokerStorePort> {
        inner: ClientManageProcessor<MS>,
        observed: Arc<std::sync::Mutex<Option<(bool, bool, i32)>>>,
    }

    impl<MS: BrokerStorePort> Clone for ObservedClientManageProcessor<MS> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                observed: Arc::clone(&self.observed),
            }
        }
    }

    impl<MS: BrokerStorePort + 'static> RequestProcessorV2 for ObservedClientManageProcessor<MS> {
        async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            self.observed.lock().expect("observation lock").replace((
                matches!(request.origin(), RequestOrigin::Embedded { .. }),
                matches!(request.session(), SessionView::Embedded { .. }),
                request.original_identity().original_opaque(),
            ));
            RequestProcessorV2::process(&mut self.inner, request).await
        }
    }

    #[test]
    fn production_processor_has_no_complete_runtime_owner() {
        let source = include_str!("client_manage_processor.rs");
        let production_source = source.split("#[cfg(test)]").next().expect("production source");

        assert!(!production_source.contains("ArcMut"));
        assert!(!production_source.contains("BrokerRuntimeState"));
    }

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
        assert!(runtime.initialize().await.is_ok());
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
        rocketmq_transport::test_support::TestChannelBuilder::new(connection, crate::test_task_group("channel"))
            .addresses(local_addr, local_addr)
            .build()
            .expect("build test channel")
    }

    fn check_request(subscription_data: SubscriptionData) -> RemotingCommand {
        let body = CheckClientRequestBody::new("client-id".to_string(), "group-a".to_string(), subscription_data);
        RemotingCommand::create_request_command(RequestCode::CheckClientConfig, EmptyHeader {})
            .set_body(Bytes::from(serde_json::to_vec(&body).expect("serialize request body")))
    }

    fn session_heartbeat_request(
        client_id: &str,
        consumer_groups: &[CheetahString],
        producer_groups: &[CheetahString],
    ) -> RemotingCommand {
        let heartbeat = HeartbeatData {
            client_id: client_id.into(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
            consumer_data_set: consumer_groups
                .iter()
                .cloned()
                .map(|group_name| ConsumerData {
                    group_name,
                    consume_type: ConsumeType::ConsumePassively,
                    message_model: MessageModel::Clustering,
                    consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
                    subscription_data_set: HashSet::new(),
                    unit_mode: false,
                })
                .collect(),
            producer_data_set: producer_groups
                .iter()
                .cloned()
                .map(|group_name| ProducerData { group_name })
                .collect(),
        };
        RemotingCommand::create_request_command(RequestCode::HeartBeat, EmptyHeader {}).set_body(Bytes::from(
            serde_json::to_vec(&heartbeat).expect("serialize session heartbeat"),
        ))
    }

    #[tokio::test]
    async fn client_manage_v2_uses_typed_embedded_origin_and_original_opaque() {
        let mut runtime = new_test_runtime("client-manage-v2", false).await;
        let observed = Arc::new(std::sync::Mutex::new(None));
        let processor = ObservedClientManageProcessor {
            inner: runtime.runtime_state_mut().build_client_manage_processor(),
            observed: Arc::clone(&observed),
        };
        let request = check_request(SubscriptionData {
            topic: "topic-a".into(),
            sub_string: "*".into(),
            expression_type: ExpressionType::TAG.into(),
            ..Default::default()
        })
        .set_opaque(9_845);

        let EmbeddedDispatchOutcome::Reply(plan) = dispatch_v2(processor, request).await else {
            panic!("client manager V2 must return an inline response plan");
        };

        assert_eq!(
            RemotingResponseCode::from(plan.response_code()),
            RemotingResponseCode::Success
        );
        assert_eq!(*observed.lock().expect("observation lock"), Some((true, true, 9_845)));
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[test]
    fn client_manage_rejects_origin_session_mismatch() {
        let remote_addr = "127.0.0.1:10911".parse().expect("remote address");
        let other_addr = "127.0.0.1:10912".parse().expect("other address");

        assert!(trusted_remote_address_from_facts(
            TrustedOriginFact::Network(remote_addr),
            TrustedSessionFact::Embedded,
        )
        .is_err());
        assert!(trusted_remote_address_from_facts(
            TrustedOriginFact::Embedded,
            TrustedSessionFact::Network(remote_addr),
        )
        .is_err());
        assert!(trusted_remote_address_from_facts(
            TrustedOriginFact::Network(remote_addr),
            TrustedSessionFact::Network(other_addr),
        )
        .is_err());
    }

    #[tokio::test]
    async fn check_client_config_rejects_property_filter_when_disabled() {
        let mut runtime = new_test_runtime("check-client-filter-disabled", false).await;
        let processor = runtime.runtime_state_mut().build_client_manage_processor();
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
        let processor = runtime.runtime_state_mut().build_client_manage_processor();
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
        let processor = runtime.runtime_state_mut().build_client_manage_processor();
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
        let remark = response.remark().expect("parse failure should have a redacted remark");
        assert!(remark.contains("UnexpectedToken"));
        assert!(!remark.contains("a >"));
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn heart_beat_v2_without_sub_registers_consumer_and_marks_sub_change() {
        let mut runtime = new_test_runtime("heartbeat-v2-without-sub", false).await;
        let (consumer_manager, processor) = {
            let inner = runtime.runtime_state_mut();
            (
                inner.consumer_manager().clone_shared_state(),
                inner.build_client_manage_processor(),
            )
        };
        let channel = create_test_channel().await;
        let client_channel_info = ClientChannelInfo::new(channel.clone(), "client-id".into(), Default::default(), 0);
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
            .heart_beat_v2(
                &channel.remote_address().to_string(),
                heartbeat_data,
                RegisteredClient::Legacy(Box::new(client_channel_info)),
            )
            .await
            .expect("heartbeat should succeed")
            .expect("processor should return response");

        assert_eq!(
            RemotingResponseCode::from(response.code()),
            RemotingResponseCode::Success
        );
        let ext_fields = response.ext_fields().expect("ext fields should exist");
        assert_eq!(
            ext_fields
                .get(&CheetahString::from_static_str(IS_SUPPORT_HEART_BEAT_V2))
                .map(|value| value.as_str()),
            Some("true")
        );
        assert_eq!(
            ext_fields
                .get(&CheetahString::from_static_str(IS_SUB_CHANGE))
                .map(|value| value.as_str()),
            Some("true")
        );

        let consumer_group_info = consumer_manager
            .get_consumer_group_info(&CheetahString::from_static_str("group-a"))
            .expect("consumer should be registered");
        assert!(consumer_group_info.subscriptions_is_empty());
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn heart_beat_v1_and_unregister_share_live_client_registries() {
        let mut runtime = new_test_runtime("heartbeat-v1-unregister", false).await;
        let (producer_manager, consumer_manager, processor) = {
            let inner = runtime.runtime_state_mut();
            (
                inner.producer_manager().clone_shared_state(),
                inner.consumer_manager().clone_shared_state(),
                inner.build_client_manage_processor(),
            )
        };
        let channel = create_test_channel().await;
        let producer_group = CheetahString::from_static_str("producer-group");
        let consumer_group = CheetahString::from_static_str("group-a");
        let heartbeat_data = HeartbeatData {
            client_id: "client-id".into(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
            consumer_data_set: HashSet::from([ConsumerData {
                group_name: consumer_group.clone(),
                consume_type: ConsumeType::ConsumePassively,
                message_model: MessageModel::Clustering,
                consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
                subscription_data_set: HashSet::new(),
                unit_mode: false,
            }]),
            producer_data_set: HashSet::from([ProducerData {
                group_name: producer_group.clone(),
            }]),
        };
        let mut heartbeat_request = RemotingCommand::create_request_command(RequestCode::HeartBeat, EmptyHeader {})
            .set_body(Bytes::from(
                serde_json::to_vec(&heartbeat_data).expect("serialize heartbeat"),
            ));
        let mut legacy_processor = processor.legacy_adapter();
        let heartbeat_response = legacy_processor
            .process_legacy(channel.clone(), &mut heartbeat_request)
            .await
            .expect("heartbeat should succeed")
            .expect("heartbeat should return response");

        assert_eq!(
            RemotingResponseCode::from(heartbeat_response.code()),
            RemotingResponseCode::Success
        );
        assert!(producer_manager.group_online(producer_group.as_str()));
        assert!(consumer_manager.get_consumer_group_info(&consumer_group).is_some());

        let mut unregister_request = RemotingCommand::create_request_command(
            RequestCode::UnregisterClient,
            UnregisterClientRequestHeader {
                client_id: "client-id".into(),
                producer_group: Some(producer_group.clone()),
                consumer_group: Some(consumer_group.clone()),
                rpc_request_header: None,
            },
        );
        unregister_request.make_custom_header_to_net();
        let unregister_response = legacy_processor
            .process_legacy(channel, &mut unregister_request)
            .await
            .expect("unregister should succeed")
            .expect("unregister should return response");

        assert_eq!(
            RemotingResponseCode::from(unregister_response.code()),
            RemotingResponseCode::Success
        );
        assert!(!producer_manager.group_online(producer_group.as_str()));
        assert!(consumer_manager.get_consumer_group_info(&consumer_group).is_none());
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn session_command_core_uses_only_the_stable_session_identity_index() {
        let mut runtime = new_test_runtime("session-identity-index", false).await;
        let (producer_manager, consumer_manager, processor) = {
            let inner = runtime.runtime_state_mut();
            (
                inner.producer_manager().clone_shared_state(),
                inner.consumer_manager().clone_shared_state(),
                inner.build_client_manage_processor(),
            )
        };
        let session_id = session_id_for_test(9_846);
        let producer_group = CheetahString::from_static_str("session-producer-group");
        let consumer_group = CheetahString::from_static_str("session-consumer-group");
        let heartbeat_data = HeartbeatData {
            client_id: "session-client".into(),
            heartbeat_fingerprint: 0,
            is_without_sub: false,
            consumer_data_set: HashSet::from([ConsumerData {
                group_name: consumer_group.clone(),
                consume_type: ConsumeType::ConsumePassively,
                message_model: MessageModel::Clustering,
                consume_from_where: ConsumeFromWhere::ConsumeFromLastOffset,
                subscription_data_set: HashSet::new(),
                unit_mode: false,
            }]),
            producer_data_set: HashSet::from([ProducerData {
                group_name: producer_group.clone(),
            }]),
        };
        let mut heartbeat_request = RemotingCommand::create_request_command(RequestCode::HeartBeat, EmptyHeader {})
            .set_body(Bytes::from(
                serde_json::to_vec(&heartbeat_data).expect("serialize heartbeat"),
            ));

        let heartbeat_response = processor
            .process_session_command(session_id, "127.0.0.1:9846".to_owned(), &mut heartbeat_request)
            .await
            .expect("session heartbeat should succeed")
            .expect("session heartbeat should return a response");
        assert_eq!(
            RemotingResponseCode::from(heartbeat_response.code()),
            RemotingResponseCode::Success
        );
        let group_info = consumer_manager
            .get_consumer_group_info(&consumer_group)
            .expect("session consumer should be registered");
        assert!(group_info.get_all_channels().is_empty());
        assert_eq!(
            crate::long_polling::pull_deferred::PullSessionClientLookup::client_id(
                &consumer_manager.session_registry(),
                session_id,
                &consumer_group,
            ),
            Some(CheetahString::from_static_str("session-client"))
        );
        assert!(producer_manager.group_online(producer_group.as_str()));

        let mut unregister_request = RemotingCommand::create_request_command(
            RequestCode::UnregisterClient,
            UnregisterClientRequestHeader {
                client_id: "session-client".into(),
                producer_group: Some(producer_group.clone()),
                consumer_group: Some(consumer_group.clone()),
                rpc_request_header: None,
            },
        );
        unregister_request.make_custom_header_to_net();
        processor
            .process_session_command(session_id, "127.0.0.1:9846".to_owned(), &mut unregister_request)
            .await
            .expect("session unregister should succeed")
            .expect("session unregister should return a response");

        assert!(!producer_manager.group_online(producer_group.as_str()));
        assert_eq!(
            crate::long_polling::pull_deferred::PullSessionClientLookup::client_id(
                &consumer_manager.session_registry(),
                session_id,
                &consumer_group,
            ),
            None
        );
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn session_reconnect_cleans_the_role_omitted_by_the_new_heartbeat() {
        let mut runtime = new_test_runtime("session-role-reconnect", false).await;
        let (producer_manager, consumer_manager, processor) = {
            let inner = runtime.runtime_state_mut();
            (
                inner.producer_manager().clone_shared_state(),
                inner.consumer_manager().clone_shared_state(),
                inner.build_client_manage_processor(),
            )
        };

        let producer_group = CheetahString::from_static_str("role-reconnect-producer");
        let consumer_group = CheetahString::from_static_str("role-reconnect-consumer");
        let old_session = session_id_for_test(9_851);
        let producer_only_session = session_id_for_test(9_852);
        let mut old_request = session_heartbeat_request(
            "role-reconnect-client",
            std::slice::from_ref(&consumer_group),
            std::slice::from_ref(&producer_group),
        );
        processor
            .process_session_command(old_session, "127.0.0.1:9851".to_owned(), &mut old_request)
            .await
            .expect("initial mixed-role heartbeat")
            .expect("initial heartbeat response");

        let mut producer_only_request =
            session_heartbeat_request("role-reconnect-client", &[], std::slice::from_ref(&producer_group));
        processor
            .process_session_command(
                producer_only_session,
                "127.0.0.1:9852".to_owned(),
                &mut producer_only_request,
            )
            .await
            .expect("producer-only reconnect")
            .expect("producer-only response");

        assert!(producer_manager.group_online(producer_group.as_str()));
        assert!(consumer_manager.get_consumer_group_info(&consumer_group).is_none());
        assert!(!producer_manager
            .connection_housekeeping()
            .do_session_close_event(old_session));
        assert!(!consumer_manager
            .connection_housekeeping()
            .do_session_close_event(old_session));
        assert!(producer_manager
            .connection_housekeeping()
            .do_session_close_event(producer_only_session));
        assert!(!producer_manager.group_online(producer_group.as_str()));

        let second_producer_group = CheetahString::from_static_str("role-reconnect-producer-2");
        let second_consumer_group = CheetahString::from_static_str("role-reconnect-consumer-2");
        let second_old_session = session_id_for_test(9_853);
        let consumer_only_session = session_id_for_test(9_854);
        let mut second_old_request = session_heartbeat_request(
            "role-reconnect-client-2",
            std::slice::from_ref(&second_consumer_group),
            std::slice::from_ref(&second_producer_group),
        );
        processor
            .process_session_command(second_old_session, "127.0.0.1:9853".to_owned(), &mut second_old_request)
            .await
            .expect("second initial mixed-role heartbeat")
            .expect("second initial heartbeat response");

        let mut consumer_only_request = session_heartbeat_request(
            "role-reconnect-client-2",
            std::slice::from_ref(&second_consumer_group),
            &[],
        );
        processor
            .process_session_command(
                consumer_only_session,
                "127.0.0.1:9854".to_owned(),
                &mut consumer_only_request,
            )
            .await
            .expect("consumer-only reconnect")
            .expect("consumer-only response");

        assert!(!producer_manager.group_online(second_producer_group.as_str()));
        assert_eq!(
            consumer_manager
                .session_registry()
                .client_id(consumer_only_session, &second_consumer_group)
                .as_deref(),
            Some("role-reconnect-client-2")
        );
        assert!(!producer_manager
            .connection_housekeeping()
            .do_session_close_event(second_old_session));
        assert!(!consumer_manager
            .connection_housekeeping()
            .do_session_close_event(second_old_session));
        assert!(consumer_manager
            .connection_housekeeping()
            .do_session_close_event(consumer_only_session));
        assert!(consumer_manager
            .get_consumer_group_info(&second_consumer_group)
            .is_none());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn session_identity_conflict_is_rejected_before_either_role_commits() {
        let mut runtime = new_test_runtime("session-cross-role-identity", false).await;
        let (producer_manager, consumer_manager, processor) = {
            let inner = runtime.runtime_state_mut();
            (
                inner.producer_manager().clone_shared_state(),
                inner.consumer_manager().clone_shared_state(),
                inner.build_client_manage_processor(),
            )
        };
        let consumer_first_session = session_id_for_test(9_861);
        let consumer_group = CheetahString::from_static_str("identity-consumer-first");
        let rejected_producer_group = CheetahString::from_static_str("identity-producer-rejected");
        let mut consumer_first =
            session_heartbeat_request("identity-client-a", std::slice::from_ref(&consumer_group), &[]);
        processor
            .process_session_command(consumer_first_session, "127.0.0.1:9861".to_owned(), &mut consumer_first)
            .await
            .expect("consumer-first heartbeat")
            .expect("consumer-first response");
        let mut conflicting_producer =
            session_heartbeat_request("identity-client-b", &[], std::slice::from_ref(&rejected_producer_group));
        assert!(processor
            .process_session_command(
                consumer_first_session,
                "127.0.0.1:9862".to_owned(),
                &mut conflicting_producer,
            )
            .await
            .is_err());
        assert!(!producer_manager.group_online(rejected_producer_group.as_str()));
        assert_eq!(
            consumer_manager
                .session_registry()
                .client_id(consumer_first_session, &consumer_group)
                .as_deref(),
            Some("identity-client-a")
        );

        let producer_first_session = session_id_for_test(9_862);
        let producer_group = CheetahString::from_static_str("identity-producer-first");
        let rejected_consumer_group = CheetahString::from_static_str("identity-consumer-rejected");
        let mut producer_first =
            session_heartbeat_request("identity-client-c", &[], std::slice::from_ref(&producer_group));
        processor
            .process_session_command(producer_first_session, "127.0.0.1:9863".to_owned(), &mut producer_first)
            .await
            .expect("producer-first heartbeat")
            .expect("producer-first response");
        let mut conflicting_consumer =
            session_heartbeat_request("identity-client-d", std::slice::from_ref(&rejected_consumer_group), &[]);
        assert!(processor
            .process_session_command(
                producer_first_session,
                "127.0.0.1:9864".to_owned(),
                &mut conflicting_consumer,
            )
            .await
            .is_err());
        assert!(consumer_manager
            .get_consumer_group_info(&rejected_consumer_group)
            .is_none());
        assert!(producer_manager.group_online(producer_group.as_str()));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn concurrent_mixed_role_heartbeats_choose_one_cross_role_session() {
        let mut runtime = new_test_runtime("session-cross-role-race", false).await;
        let (consumer_manager, processor) = {
            let inner = runtime.runtime_state_mut();
            (
                inner.consumer_manager().clone_shared_state(),
                inner.build_client_manage_processor(),
            )
        };
        let producer_registration = processor.producer_registration.clone();
        let consumer_registration = processor.consumer_registration.clone();
        let first_session = session_id_for_test(9_871);
        let second_session = session_id_for_test(9_872);
        let producer_group = CheetahString::from_static_str("cross-role-race-producer");
        let consumer_group = CheetahString::from_static_str("cross-role-race-consumer");
        let first_processor = processor.clone();
        let second_processor = processor;
        let mut first_request = session_heartbeat_request(
            "cross-role-race-client",
            std::slice::from_ref(&consumer_group),
            std::slice::from_ref(&producer_group),
        );
        let mut second_request = session_heartbeat_request(
            "cross-role-race-client",
            std::slice::from_ref(&consumer_group),
            std::slice::from_ref(&producer_group),
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let first_barrier = Arc::clone(&barrier);
        let second_barrier = Arc::clone(&barrier);
        let (first_result, second_result) = tokio::join!(
            async move {
                first_barrier.wait().await;
                first_processor
                    .process_session_command(first_session, "127.0.0.1:9871".to_owned(), &mut first_request)
                    .await
            },
            async move {
                second_barrier.wait().await;
                second_processor
                    .process_session_command(second_session, "127.0.0.1:9872".to_owned(), &mut second_request)
                    .await
            }
        );
        assert!(first_result.is_ok());
        assert!(second_result.is_ok());

        let first_is_producer = producer_registration.client_id_for_session(first_session).is_some();
        let first_is_consumer = consumer_registration.client_id_for_session(first_session).is_some();
        let second_is_producer = producer_registration.client_id_for_session(second_session).is_some();
        let second_is_consumer = consumer_registration.client_id_for_session(second_session).is_some();
        assert_eq!(first_is_producer, first_is_consumer);
        assert_eq!(second_is_producer, second_is_consumer);
        assert_ne!(first_is_producer, second_is_producer);
        assert_eq!(
            consumer_manager
                .get_consumer_group_info(&consumer_group)
                .expect("winning consumer group")
                .session_info_snapshot()
                .len(),
            1
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn explicit_client_capabilities_share_live_state_and_preserve_retry_topic_options() {
        let mut runtime = new_test_runtime("explicit-client-capabilities", false).await;
        let (producer_manager, topic_config_manager, processor) = {
            let inner = runtime.runtime_state_mut();
            (
                inner.producer_manager().clone_shared_state(),
                inner.topic_config_manager_handle(),
                inner.build_client_manage_processor(),
            )
        };
        let channel = create_test_channel().await;
        let client_channel_info = ClientChannelInfo::new(channel, "client-id".into(), Default::default(), 0);
        let producer_group = CheetahString::from_static_str("producer-group");

        processor
            .producer_registration
            .register_producer(&producer_group, &client_channel_info);
        assert!(producer_manager.group_online(producer_group.as_str()));

        let retry_topic = CheetahString::from_static_str("%RETRY%explicit-client-capabilities");
        let topic_sys_flag = topic_sys_flag::build_sys_flag(false, true);
        let topic_config = processor
            .retry_topic_registration
            .select_or_create_send_back_topic_with(&retry_topic, 3, true, topic_sys_flag)
            .await
            .expect("retry topic should be created");

        assert_eq!(topic_config.read_queue_nums, 3);
        assert_eq!(topic_config.write_queue_nums, 3);
        assert_eq!(topic_config.topic_sys_flag, topic_sys_flag);
        assert!(topic_config.order);
        assert_eq!(
            topic_config_manager
                .select_topic_config(&retry_topic)
                .expect("live topic manager should contain retry topic")
                .as_ref(),
            topic_config.as_ref()
        );
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
