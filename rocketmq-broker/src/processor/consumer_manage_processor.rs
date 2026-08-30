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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::get_consumer_list_by_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_protocol::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_store::BrokerStorePort;
use rocketmq_transport::api::request_code_not_supported_with_factory_remark_and_opaque;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::RpcClient;
use rocketmq_transport::api::RpcClientImpl;
use rocketmq_transport::api::RpcRequest;
use rocketmq_transport::api::SessionView;
use tracing::info;
use tracing::warn;

use crate::client::manager::consumer_manager::ConsumerAssignmentView;
use crate::offset::manager::consumer_offset_manager::ConsumerOffsetRequestCapability;
use crate::processor::response_plan::immediate_outcome_from_command_result;
use crate::subscription::manager::subscription_group_manager::SubscriptionGroupConfigLookup;
use crate::topic::manager::topic_config_manager::TopicConfigManager;
use crate::topic::manager::topic_queue_mapping_manager::TopicQueueMappingManager;

pub struct ConsumerManageProcessor<MS: BrokerStorePort> {
    command_factory: RemotingCommandFactory,
    consumer_view: ConsumerAssignmentView,
    consumer_offset: ConsumerOffsetRequestCapability<MS>,
    topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    subscription_group_lookup: SubscriptionGroupConfigLookup,
    topic_config_manager: Arc<TopicConfigManager>,
    rpc_client: RpcClientImpl,
    use_server_side_reset_offset: bool,
    forward_timeout: u64,
}

pub(crate) struct ConsumerManageProcessorContext<MS: BrokerStorePort> {
    pub(crate) command_factory: RemotingCommandFactory,
    pub(crate) consumer_view: ConsumerAssignmentView,
    pub(crate) consumer_offset: ConsumerOffsetRequestCapability<MS>,
    pub(crate) topic_queue_mapping_manager: Arc<TopicQueueMappingManager>,
    pub(crate) subscription_group_lookup: SubscriptionGroupConfigLookup,
    pub(crate) topic_config_manager: Arc<TopicConfigManager>,
    pub(crate) rpc_client: RpcClientImpl,
    pub(crate) use_server_side_reset_offset: bool,
    pub(crate) forward_timeout: u64,
}

impl<MS> RequestProcessor for ConsumerManageProcessor<MS>
where
    MS: BrokerStorePort + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

impl<MS: BrokerStorePort> ConsumerManageProcessor<MS> {
    pub(crate) async fn process_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let original_opaque = request.original_identity().original_opaque();
        let request_source = trusted_request_source(request)?;
        let result = self.process_command(request_source, request.command_mut()).await;
        immediate_outcome_from_command_result(
            &self.command_factory,
            result,
            original_opaque,
            "consumer manager returned no response",
        )
    }
}

impl<MS> ConsumerManageProcessor<MS>
where
    MS: BrokerStorePort,
{
    pub(crate) fn new(context: ConsumerManageProcessorContext<MS>) -> Self {
        Self {
            command_factory: context.command_factory,
            consumer_view: context.consumer_view,
            consumer_offset: context.consumer_offset,
            topic_queue_mapping_manager: context.topic_queue_mapping_manager,
            subscription_group_lookup: context.subscription_group_lookup,
            topic_config_manager: context.topic_config_manager,
            rpc_client: context.rpc_client,
            use_server_side_reset_offset: context.use_server_side_reset_offset,
            forward_timeout: context.forward_timeout,
        }
    }
}

impl<MS: BrokerStorePort> Clone for ConsumerManageProcessor<MS> {
    fn clone(&self) -> Self {
        Self {
            command_factory: self.command_factory,
            consumer_view: self.consumer_view.clone(),
            consumer_offset: self.consumer_offset.clone(),
            topic_queue_mapping_manager: Arc::clone(&self.topic_queue_mapping_manager),
            subscription_group_lookup: self.subscription_group_lookup.clone(),
            topic_config_manager: Arc::clone(&self.topic_config_manager),
            rpc_client: self.rpc_client.clone(),
            use_server_side_reset_offset: self.use_server_side_reset_offset,
            forward_timeout: self.forward_timeout,
        }
    }
}

#[allow(unused_variables)]
impl<MS> ConsumerManageProcessor<MS>
where
    MS: BrokerStorePort,
{
    async fn process_command(
        &self,
        request_source: CheetahString,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!("ConsumerManageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::GetConsumerListByGroup => self.get_consumer_list_by_group(&request_source, request).await,
            RequestCode::UpdateConsumerOffset => self.update_consumer_offset(request_source, request).await,
            RequestCode::QueryConsumerOffset => self.query_consumer_offset(request).await,
            _ => {
                warn!(
                    "ConsumerManageProcessor received unknown request code: {:?}",
                    request_code
                );
                Ok(Some(request_code_not_supported_with_factory_remark_and_opaque(
                    &self.command_factory,
                    request.code(),
                    format!("ConsumerManageProcessor request code {} not supported", request.code()),
                    request.opaque(),
                )))
            }
        }
    }

    pub async fn get_consumer_list_by_group(
        &self,
        request_source: &str,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = self.command_factory.create_success_response_command();
        let request_header = request.decode_command_custom_header::<GetConsumerListByGroupRequestHeader>()?;
        let consumer_group_info = self.consumer_view.client_ids_if_present(&request_header.consumer_group);

        match consumer_group_info {
            None => {
                warn!(
                    "getConsumerGroupInfo failed, {} {}",
                    request_header.consumer_group, request_source
                );
            }
            Some(client_ids) => {
                if !client_ids.is_empty() {
                    let body = GetConsumerListByGroupResponseBody {
                        consumer_id_list: client_ids,
                    };
                    return Ok(Some(
                        response
                            .set_body(body.encode().expect("GetConsumerListByGroupResponseBody encode error"))
                            .set_code(ResponseCode::Success),
                    ));
                } else {
                    warn!(
                        "getAllClientId failed, {} {}",
                        request_header.consumer_group, request_source
                    )
                }
            }
        }
        Ok(Some(
            response
                .set_remark(format!("no consumer for this group, {}", request_header.consumer_group))
                .set_code(ResponseCode::SystemError),
        ))
    }

    async fn update_consumer_offset(
        &self,
        request_source: CheetahString,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_header = request.decode_command_custom_header::<UpdateConsumerOffsetRequestHeader>()?;
        let mut mapping_context = self
            .topic_queue_mapping_manager
            .build_topic_queue_mapping_context(&request_header, false);

        let rewrite_result = self
            .rewrite_request_for_static_topic_for_consume_offset(&mut request_header, &mut mapping_context)
            .await;
        if let Some(result) = rewrite_result {
            return Ok(Some(result));
        }
        let topic = request_header.topic.as_ref();
        let group = request_header.consumer_group.as_ref();
        let queue_id = request_header.queue_id;
        let offset = request_header.commit_offset;
        let response = self.command_factory.create_success_response_command();
        if !self.subscription_group_lookup.contains_subscription_group(group) {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SubscriptionGroupNotExist)
                    .set_remark(format!("subscription group not exist, {group}")),
            ));
        }

        if !self.topic_config_manager.contains_topic(topic) {
            return Ok(Some(
                response
                    .set_code(ResponseCode::TopicNotExist)
                    .set_remark(format!("topic not exist, {topic}")),
            ));
        }

        // if queue_id.is_none() {
        //     return Some(
        //         response
        //             .set_code(ResponseCode::SystemError)
        //             .set_remark(format!("QueueId is null, topic is {}", topic)),
        //     );
        // }
        // if offset.is_none() {
        //     return Some(
        //         response
        //             .set_code(ResponseCode::SystemError)
        //             .set_remark(format!("Offset is null, topic is {}", topic)),
        //     );
        // }
        if self.use_server_side_reset_offset && self.consumer_offset.has_offset_reset(topic, group, queue_id) {
            info!(
                "Update consumer offset is rejected because of previous offset-reset. Group={},Topic={}, QueueId={}, \
                 Offset={}",
                group, topic, queue_id, offset
            );
            return Ok(Some(
                response
                    .set_code(ResponseCode::Success)
                    .set_remark("Offset has been previously reset"),
            ));
        }
        self.consumer_offset
            .commit_offset(request_source, group, topic, queue_id, offset);
        Ok(Some(response.set_code(ResponseCode::Success)))
    }

    async fn query_consumer_offset(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut request_header = request.decode_command_custom_header::<QueryConsumerOffsetRequestHeader>()?;
        let mut mapping_context = self
            .topic_queue_mapping_manager
            .build_topic_queue_mapping_context(&request_header, false);
        if let Some(result) = self
            .rewrite_request_for_static_topic(&mut request_header, &mut mapping_context)
            .await
        {
            return Ok(Some(result));
        }
        let offset = self.consumer_offset.query_offset(
            request_header.consumer_group.as_ref(),
            request_header.topic.as_ref(),
            request_header.queue_id,
        );
        let mut response = self.command_factory.create_success_response_command();
        let mut response_header = QueryConsumerOffsetResponseHeader::default();
        if offset >= 0 {
            response_header.offset = Some(offset);
            response = response.set_code(ResponseCode::Success);
        } else {
            let min_offset = self
                .consumer_offset
                .min_offset_in_queue(request_header.topic.as_ref(), request_header.queue_id);
            if let Some(value) = request_header.set_zero_if_not_found {
                if !value {
                    response = response
                        .set_code(ResponseCode::QueryNotFound)
                        .set_remark("Not found, do not set to zero, maybe this group boot first");
                }
            } else if min_offset <= 0
                && self
                    .consumer_offset
                    .check_in_mem_by_consume_offset(request_header.topic.as_ref(), request_header.queue_id)
            {
                response_header.offset = Some(0);
                response = response.set_code(ResponseCode::Success);
            } else {
                response = response
                    .set_code(ResponseCode::QueryNotFound)
                    .set_remark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }
        if let Some(result) = self.rewrite_response_for_static_topic(
            &request_header,
            &mut response_header,
            &mapping_context,
            response.code(),
        ) {
            return Ok(Some(result));
        }
        Ok(Some(response.set_command_custom_header(response_header)))
    }

    /// Rewrite request for static topic when updating consumer offset.
    /// This handles the case where the consumer offset needs to be committed to a different broker
    /// based on the static topic queue mapping.
    async fn rewrite_request_for_static_topic_for_consume_offset(
        &self,
        request_header: &mut UpdateConsumerOffsetRequestHeader,
        mapping_context: &mut TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        let mapping_detail = mapping_context.mapping_detail.as_ref()?;

        // Check if current broker is the leader for this queue
        if !mapping_context.is_leader() {
            return Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::NotLeaderForQueue,
                format!(
                    "{}-{} does not exit in request process of current broker {}",
                    request_header.topic,
                    request_header.queue_id,
                    mapping_detail
                        .topic_queue_mapping_info
                        .bname
                        .as_ref()
                        .cloned()
                        .unwrap_or_default()
                ),
            ));
        }

        let global_offset = request_header.commit_offset;

        // Find the mapping item for this offset
        let mapping_item = TopicQueueMappingUtils::find_logic_queue_mapping_item(
            &mapping_context.mapping_item_list,
            global_offset,
            true,
        )?;

        // Update request header with the physical queue info
        request_header.queue_id = mapping_item.queue_id;
        request_header.set_lo(Some(false));
        request_header.set_broker_name(mapping_item.bname.clone().unwrap_or_default());
        request_header.commit_offset = mapping_item.compute_physical_queue_offset(global_offset);

        // If this broker is the target, let it go through normal processing
        if mapping_detail.topic_queue_mapping_info.bname == mapping_item.bname {
            return None;
        }

        // For non-local broker, forward the request via RPC
        let rpc_request = RpcRequest::new(RequestCode::UpdateConsumerOffset.to_i32(), request_header.clone(), None);
        let rpc_response = self.rpc_client.invoke(rpc_request, self.forward_timeout).await;

        match rpc_response {
            Ok(response) => {
                if let Some(exception) = response.exception {
                    return Some(self.command_factory.create_response_command_with_code_remark(
                        ResponseCode::SystemError,
                        format!("RPC exception: {exception}"),
                    ));
                }
                if ResponseCode::from(response.code) == ResponseCode::Success {
                    Some(
                        self.command_factory
                            .create_response_command_with_code(ResponseCode::Success),
                    )
                } else {
                    Some(self.command_factory.create_response_command_with_code_remark(
                        ResponseCode::from(response.code),
                        format!("RPC to broker {:?} returned code {}", mapping_item.bname, response.code),
                    ))
                }
            }
            Err(e) => Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!("RPC forwarding to broker {:?} failed: {e}", mapping_item.bname),
            )),
        }
    }

    async fn rewrite_request_for_static_topic(
        &self,
        request_header: &mut QueryConsumerOffsetRequestHeader,
        mapping_context: &mut TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        let mapping_detail = mapping_context.mapping_detail.as_ref()?;
        if !mapping_context.is_leader() {
            return Some(self.command_factory.create_response_command_with_code_remark(
                ResponseCode::NotLeaderForQueue,
                format!(
                    "{}-{} does not exit in request process of current broker {}",
                    request_header.topic,
                    request_header.queue_id,
                    mapping_detail
                        .topic_queue_mapping_info
                        .bname
                        .as_ref()
                        .cloned()
                        .unwrap_or_default()
                ),
            ));
        }
        let mapping_item_list = &mapping_context.mapping_item_list;
        if mapping_item_list.len() == 1 && mapping_item_list[0].logic_offset == 0 {
            mapping_context.current_item = Some(mapping_item_list[0].clone());
            request_header.queue_id = mapping_context.leader_item.as_ref()?.queue_id;
            return None;
        }
        let mut offset = -1i64;
        // Clone mapping_item_list to avoid borrow issues
        let mapping_item_list_clone: Vec<_> = mapping_context.mapping_item_list.to_vec();
        let current_broker_name = mapping_detail.topic_queue_mapping_info.bname.clone();

        for mapping_item in mapping_item_list_clone.iter().rev() {
            mapping_context.current_item = Some(mapping_item.clone());
            if mapping_item.bname == current_broker_name {
                offset = self.consumer_offset.query_offset(
                    request_header.consumer_group.as_ref(),
                    request_header.topic.as_ref(),
                    mapping_item.queue_id,
                );
                if offset >= 0 {
                    break;
                }
            } else {
                // RPC call to remote broker
                let mut query_header = request_header.clone();
                query_header.set_broker_name(mapping_item.bname.clone().unwrap_or_default());
                query_header.queue_id = mapping_item.queue_id;
                query_header.set_lo(Some(false));
                query_header.set_zero_if_not_found = Some(false);

                let rpc_request = RpcRequest::new(RequestCode::QueryConsumerOffset.to_i32(), query_header, None);
                let rpc_response = self.rpc_client.invoke(rpc_request, self.forward_timeout).await;

                match rpc_response {
                    Ok(response) => {
                        if let Some(exception) = response.exception {
                            warn!(
                                "QueryConsumerOffset RPC exception for broker {:?}: {}",
                                mapping_item.bname, exception
                            );
                            return Some(self.command_factory.create_response_command_with_code_remark(
                                ResponseCode::SystemError,
                                format!("RPC exception: {exception}"),
                            ));
                        }

                        if ResponseCode::from(response.code) == ResponseCode::Success {
                            if let Some(header) = response.get_header::<QueryConsumerOffsetResponseHeader>() {
                                offset = header.offset.unwrap_or(-1);
                                if offset >= 0 {
                                    break;
                                }
                            }
                        } else if ResponseCode::from(response.code) == ResponseCode::QueryNotFound {
                            // Continue to next mapping item
                            continue;
                        } else {
                            warn!(
                                "QueryConsumerOffset RPC to broker {:?} returned unexpected code: {}",
                                mapping_item.bname, response.code
                            );
                            return Some(self.command_factory.create_response_command_with_code_remark(
                                ResponseCode::SystemError,
                                format!("RPC to broker {:?} returned code {}", mapping_item.bname, response.code),
                            ));
                        }
                    }
                    Err(e) => {
                        warn!(
                            "QueryConsumerOffset RPC to broker {:?} failed: {}",
                            mapping_item.bname, e
                        );
                        return Some(self.command_factory.create_response_command_with_code_remark(
                            ResponseCode::SystemError,
                            format!("RPC forwarding to broker {:?} failed: {e}", mapping_item.bname),
                        ));
                    }
                }
            }
        }
        let mut response = self.command_factory.create_success_response_command();
        let mut response_header = QueryConsumerOffsetResponseHeader { offset: None };
        if offset >= 0 {
            response_header.offset = Some(offset);
            response = response.set_code(ResponseCode::Success);
        } else {
            response = response
                .set_code(ResponseCode::QueryNotFound)
                .set_remark("Not found, maybe this group consumer boot first");
        }
        let rewrite_response_result = self.rewrite_response_for_static_topic(
            request_header,
            &mut response_header,
            mapping_context,
            response.code(),
        );
        if rewrite_response_result.is_some() {
            return rewrite_response_result;
        }
        Some(response.set_command_custom_header(response_header))
    }

    fn rewrite_response_for_static_topic(
        &self,
        request_header: &QueryConsumerOffsetRequestHeader,
        response_header: &mut QueryConsumerOffsetResponseHeader,
        mapping_context: &TopicQueueMappingContext,
        code: i32,
    ) -> Option<RemotingCommand> {
        mapping_context.mapping_detail.as_ref()?;
        if ResponseCode::from(code) != ResponseCode::Success {
            return None;
        }
        if let Some(current_item) = mapping_context.current_item.as_ref() {
            response_header.offset =
                Some(current_item.compute_static_queue_offset_strictly(response_header.offset.unwrap_or(0)));
        }
        None
    }
}

fn trusted_request_source(request: &RemotingRequest) -> rocketmq_error::RocketMQResult<CheetahString> {
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
    trusted_request_source_from_facts(origin, session)
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

fn trusted_request_source_from_facts(
    origin: TrustedOriginFact,
    session: TrustedSessionFact,
) -> rocketmq_error::RocketMQResult<CheetahString> {
    match (origin, session) {
        (TrustedOriginFact::Network(peer), TrustedSessionFact::Network(remote_addr)) if peer == remote_addr => {
            Ok(remote_addr.to_string().into())
        }
        (TrustedOriginFact::Embedded, TrustedSessionFact::Embedded) => Ok(CheetahString::from_static_str("embedded")),
        _ => Err(rocketmq_error::RocketMQError::invariant_violated(
            "consumer manager request origin does not match its session view",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
    use rocketmq_security_api::AuthenticatedRequestContext;
    use rocketmq_security_api::Decision;
    use rocketmq_security_api::Principal;
    use rocketmq_security_api::RequestPolicy;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::AuthorizedCommandDispatcher;
    use rocketmq_transport::api::EmbeddedDispatchOutcome;
    use rocketmq_transport::api::TransportSecurity;
    use rocketmq_transport::test_support::EmbeddedRequestHarness;

    use super::*;
    use crate::broker_runtime::BrokerMessageStore;
    use crate::broker_runtime::BrokerRuntime;
    use crate::config::broker_config::BrokerConfig;

    struct AllowEmbeddedPolicy;

    impl RequestPolicy for AllowEmbeddedPolicy {
        fn evaluate_authenticated(&self, _context: AuthenticatedRequestContext<'_>) -> Decision {
            Decision::Allow
        }
    }

    fn temp_test_root(label: &str) -> PathBuf {
        let unique = format!(
            "rocketmq-broker-consumer-manage-{label}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    async fn new_test_runtime(label: &str) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await.is_ok());
        runtime
    }

    fn consumer_processor_for_test(runtime: &mut BrokerRuntime) -> ConsumerManageProcessor<BrokerMessageStore> {
        let inner = runtime.runtime_state_mut();
        ConsumerManageProcessor::new(ConsumerManageProcessorContext {
            command_factory: application_remoting_command_factory(),
            consumer_view: inner.consumer_manager().assignment_view(),
            consumer_offset: inner.consumer_offset_manager_handle().request_capability(),
            topic_queue_mapping_manager: inner.topic_queue_mapping_manager_handle(),
            subscription_group_lookup: inner.subscription_group_manager().config_lookup(),
            topic_config_manager: inner.topic_config_manager_handle(),
            rpc_client: inner.broker_outer_api().rpc_client().clone(),
            use_server_side_reset_offset: inner.broker_config().use_server_side_reset_offset,
            forward_timeout: inner.broker_config().forward_timeout,
        })
    }

    async fn dispatch_request(
        processor: ConsumerManageProcessor<BrokerMessageStore>,
        command: RemotingCommand,
    ) -> EmbeddedDispatchOutcome {
        let dispatcher = Arc::new(AuthorizedCommandDispatcher::new(
            processor,
            Vec::new(),
            Arc::new(TransportSecurity::secure_enforced(
                Some(Arc::new(AllowEmbeddedPolicy)),
                None,
            )),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        EmbeddedRequestHarness::new(
            dispatcher,
            crate::test_task_group("consumer-manage"),
            Principal::new("consumer-manage-test"),
        )
        .dispatch(None, command)
        .await
        .expect("consumer manager dispatch should complete")
    }

    #[tokio::test]
    async fn consumer_manage_maps_header_errors_into_a_response_plan() {
        let mut runtime = new_test_runtime("consumer-manage").await;
        let processor = consumer_processor_for_test(&mut runtime);
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerListByGroup, EmptyHeader {})
            .set_opaque(4_204);

        let EmbeddedDispatchOutcome::Reply(plan) = dispatch_request(processor, request).await else {
            panic!("consumer manager must return an inline response plan");
        };

        assert_eq!(ResponseCode::from(plan.response_code()), ResponseCode::InvalidParameter);
        assert_eq!(plan.body_len(), 0);
        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[test]
    fn consumer_manage_rejects_origin_session_mismatch() {
        let remote_addr = "127.0.0.1:10911".parse().expect("remote address");
        let other_addr = "127.0.0.1:10912".parse().expect("other address");

        assert!(trusted_request_source_from_facts(
            TrustedOriginFact::Network(remote_addr),
            TrustedSessionFact::Embedded,
        )
        .is_err());
        assert!(trusted_request_source_from_facts(
            TrustedOriginFact::Embedded,
            TrustedSessionFact::Network(remote_addr),
        )
        .is_err());
        assert!(trusted_request_source_from_facts(
            TrustedOriginFact::Network(remote_addr),
            TrustedSessionFact::Network(other_addr),
        )
        .is_err());
    }

    #[test]
    fn production_processor_has_no_complete_runtime_owner() {
        let source = include_str!("consumer_manage_processor.rs");
        let production_source = source.split("#[cfg(test)]").next().expect("production source");

        assert!(!production_source.contains("ArcMut"));
        assert!(!production_source.contains("BrokerRuntimeState"));
    }
}
