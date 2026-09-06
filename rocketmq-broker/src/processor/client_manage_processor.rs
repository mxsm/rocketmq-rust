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
use rocketmq_error::PublicErrorView;
use rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED;
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
use rocketmq_transport::api::error_response;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::SessionCloseReason;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionView;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::client::client_session_info::ClientSessionInfo;
use crate::client::client_session_info::ClientSessionRetirement;
use crate::client::manager::consumer_manager::ConsumerClientRegistration;
use crate::client::manager::consumer_manager::ConsumerSessionRegistration;
use crate::client::manager::producer_manager::ProducerClientRegistration;
use crate::client::session_transition_locks::ClientSessionTransitionGuard;
use crate::client::session_transition_locks::ClientSessionTransitionLocks;
use crate::processor::response_assembly::immediate_outcome_from_command_result;
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

#[derive(Clone)]
enum RegisteredClient {
    Session(ClientSessionInfo),
}

impl<MS> RequestProcessor for ClientManageProcessor<MS>
where
    MS: BrokerStorePort + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

impl<MS: BrokerStorePort> ClientManageProcessor<MS> {
    pub(crate) async fn process_shared(
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
        info!("ClientManageProcessor received request code: {:?}", request_code);
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
        Ok(Some(error_response(
            PublicErrorView::descriptor_only(&PROTOCOL_REQUEST_UNSUPPORTED),
            RemotingErrorTarget::Reply {
                factory: &self.command_factory,
                opaque: request.opaque(),
            },
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
        let RegisteredClient::Session(client) = client;
        if let Some(ref group) = request_header.producer_group {
            self.producer_registration
                .unregister_producer_session(group, client.session_id());
        }

        if let Some(ref group) = request_header.consumer_group {
            let subscription_group_config = self.subscription_group_lookup.find_subscription_group_config(group);
            let is_notify_consumer_ids_changed_enable =
                if let Some(ref subscription_group_config) = subscription_group_config {
                    subscription_group_config.notify_consumer_ids_changed_enable()
                } else {
                    true
                };
            self.consumer_registration.unregister_consumer_session(
                group,
                client.session_id(),
                is_notify_consumer_ids_changed_enable,
            );
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
        let RegisteredClient::Session(session) = client;
        self.ensure_known_cross_role_session_identity(&session)?;

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
            consumer_session_registrations.push(ConsumerSessionRegistration {
                group: consumer_data.group_name.clone(),
                consume_type: consumer_data.consume_type,
                message_model: consumer_data.message_model,
                consume_from_where: consumer_data.consume_from_where,
                subscriptions: consumer_data.subscription_data_set.clone(),
                notify_consumer_ids_changed: is_notify_consumer_ids_changed_enable,
                update_subscription: true,
            });
        }
        //do producer data handle
        {
            let groups = heartbeat_data
                .producer_data_set
                .iter()
                .map(|producer| producer.group_name.clone())
                .collect();
            let (consumer_batch, producer_batch) = {
                let transition = self
                    .session_transition_locks
                    .lock(session.client_id(), session.session_id());
                self.ensure_cross_role_session_identity(&transition, &session)?;
                self.ensure_canonical_session_binding(&transition, &session)?;
                let consumer_batch = self.consumer_registration.prepare_consumer_sessions(
                    &transition,
                    session.clone(),
                    consumer_session_registrations,
                );
                let producer_batch = self
                    .producer_registration
                    .prepare_producer_sessions(&transition, groups, session);
                (consumer_batch, producer_batch)
            };
            let (changed_groups, mut retirements) =
                self.consumer_registration.complete_consumer_sessions(consumer_batch);
            retirements.extend(self.producer_registration.complete_producer_sessions(producer_batch));
            self.retire_replaced_sessions(retirements).await;
            self.consumer_registration
                .notify_consumer_ids_changed(&changed_groups)
                .await;
            debug!(
                changed_group_count = changed_groups.len(),
                "consumer session registrations applied"
            );
        }
        let mut response_command = self.command_factory.create_success_response_command();
        response_command.ensure_ext_fields_initialized();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), true.to_string());
        Ok(Some(response_command))
    }

    async fn heart_beat_v2(
        &self,
        _remote_address: &str,
        heartbeat_data: HeartbeatData,
        client: RegisteredClient,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let RegisteredClient::Session(session) = client;
        self.ensure_known_cross_role_session_identity(&session)?;
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

            if heartbeat_data.is_without_sub {
                consumer_session_registrations.push(ConsumerSessionRegistration {
                    group: consumer_data.group_name.clone(),
                    consume_type: consumer_data.consume_type,
                    message_model: consumer_data.message_model,
                    consume_from_where: consumer_data.consume_from_where,
                    subscriptions: HashSet::new(),
                    notify_consumer_ids_changed: is_notify_consumer_ids_changed_enable,
                    update_subscription: false,
                });
            } else {
                consumer_session_registrations.push(ConsumerSessionRegistration {
                    group: consumer_data.group_name.clone(),
                    consume_type: consumer_data.consume_type,
                    message_model: consumer_data.message_model,
                    consume_from_where: consumer_data.consume_from_where,
                    subscriptions: consumer_data.subscription_data_set.clone(),
                    notify_consumer_ids_changed: is_notify_consumer_ids_changed_enable,
                    update_subscription: true,
                });
            }
        }

        //handle producer data
        {
            let groups = heartbeat_data
                .producer_data_set
                .iter()
                .map(|producer| producer.group_name.clone())
                .collect();
            let (consumer_batch, producer_batch) = {
                let transition = self
                    .session_transition_locks
                    .lock(session.client_id(), session.session_id());
                self.ensure_cross_role_session_identity(&transition, &session)?;
                self.ensure_canonical_session_binding(&transition, &session)?;
                let consumer_batch = self.consumer_registration.prepare_consumer_sessions(
                    &transition,
                    session.clone(),
                    consumer_session_registrations,
                );
                let producer_batch = self
                    .producer_registration
                    .prepare_producer_sessions(&transition, groups, session);
                (consumer_batch, producer_batch)
            };
            let (changed_groups, mut retirements) =
                self.consumer_registration.complete_consumer_sessions(consumer_batch);
            retirements.extend(self.producer_registration.complete_producer_sessions(producer_batch));
            self.retire_replaced_sessions(retirements).await;
            self.consumer_registration
                .notify_consumer_ids_changed(&changed_groups)
                .await;
            if !changed_groups.is_empty() {
                is_sub_change = true;
            }
            debug!(
                changed_group_count = changed_groups.len(),
                "consumer session registrations applied"
            );
        }
        let mut response_command = self.command_factory.create_success_response_command();
        response_command.ensure_ext_fields_initialized();
        response_command.add_ext_field(IS_SUPPORT_HEART_BEAT_V2.to_string(), true.to_string());
        response_command.add_ext_field(IS_SUB_CHANGE.to_string(), is_sub_change.to_string());
        Ok(Some(response_command))
    }

    async fn retire_replaced_sessions(&self, retirements: Vec<ClientSessionRetirement>) {
        let mut retired = HashSet::with_capacity(retirements.len());
        for retirement in retirements {
            if !retired.insert(retirement.session_id()) {
                continue;
            }
            let outcome = retirement.retire(SessionCloseReason::ClientBindingRetired).await;
            debug!(?outcome, "replaced client session retirement completed");
        }
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

    fn ensure_canonical_session_binding(
        &self,
        transition: &ClientSessionTransitionGuard<'_>,
        session: &ClientSessionInfo,
    ) -> rocketmq_error::RocketMQResult<()> {
        if !self
            .producer_registration
            .session_is_active(transition, session.client_id(), session.session_id())
        {
            return Err(rocketmq_error::RocketMQError::request_body_invalid(
                "HEART_BEAT",
                "the transport session generation is no longer active",
            ));
        }
        if self
            .session_transition_locks
            .claim_binding(transition, session.client_id(), session.session_id())
        {
            return Ok(());
        }
        Err(rocketmq_error::RocketMQError::request_body_invalid(
            "HEART_BEAT",
            "the session generation is no longer the canonical client binding",
        ))
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
