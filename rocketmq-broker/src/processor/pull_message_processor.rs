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

pub(crate) mod capability;
mod resume;

use std::sync::Arc;
use std::sync::OnceLock;

use cheetah_string::CheetahString;
use rocketmq_error::PublicErrorView;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_model::common::FAQUrl;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::RemotingSysResponseCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::filter::filter_api::FilterAPI;
use rocketmq_protocol::protocol::forbidden_type::ForbiddenType;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::request_source::RequestSource;
use rocketmq_protocol::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::GetMessageResult;
use rocketmq_store::GetMessageStatus;
use rocketmq_store::MAX_PULL_MSG_SIZE;
use rocketmq_transport::api::error_response as remoting_error_response;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::RpcClient;
use rocketmq_transport::api::RpcClientUtils;
use rocketmq_transport::api::RpcRequest;
use rocketmq_transport::api::SessionId;
use rocketmq_transport::api::SessionView;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::client::consumer_group_info::ConsumerGroupInfo;
use crate::filter::consumer_filter_data::ConsumerFilterData;
use crate::filter::expression_for_retry_message_filter::ExpressionForRetryMessageFilter;
use crate::filter::expression_message_filter::ExpressionMessageFilter;
use crate::long_polling::pull_deferred::PullDeferredService;
use crate::long_polling::pull_deferred::PullHookMetadata;
use crate::long_polling::pull_deferred::PullMatchCriteria;
use crate::long_polling::pull_deferred::PullRetainedEstimate;
use crate::long_polling::pull_deferred::PullSessionClientLookup;
use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
use crate::processor::pull_message_processor::capability::PullMessageProcessorContext;
use crate::processor::pull_message_result_handler::PullBroadcastClientResolver;
use crate::processor::pull_message_result_handler::PullMessageResult;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;
use crate::processor::pull_message_result_handler::PullResponseContext;
use crate::processor::response_assembly::BrokerResponseParts;

fn store_read_max_msg_bytes(max_msg_bytes: Option<i32>) -> i32 {
    max_msg_bytes
        .filter(|max_msg_bytes| *max_msg_bytes > 0)
        .unwrap_or(MAX_PULL_MSG_SIZE)
}

/// Handles pull message requests from consumers.
///
/// This processor handles both `PullMessage` and `LitePullMessage` request codes,
/// managing subscription validation, message filtering, cold data flow control,
/// and message retrieval from the message store.
///
/// # Architecture
///
/// The processor is organized into several helper methods for better maintainability:
/// - [`error_response`] / [`error_response_with_header`]: Create error responses
/// - [`get_subscription_data_with_flag`]: Parse subscription from request
/// - [`get_subscription_data_without_flag`]: Retrieve subscription from broker storage
/// - [`build_message_filter`]: Create message filter based on subscription
///
/// # Cold Data Flow Control
///
/// When cold data flow control is enabled:
/// - PUSH consumers receive `SYSTEM_BUSY` immediately
/// - PULL consumers are either suspended or limited to 1 message
pub struct PullMessageProcessor<MS: BrokerReadStore> {
    pull_message_result_handler: Arc<DefaultPullMessageResultHandler<MS>>,
    context: Arc<PullMessageProcessorContext<MS>>,
    session_client_lookup: OnceLock<Arc<dyn PullSessionClientLookup>>,
    pull_deferred_service: OnceLock<Arc<PullDeferredService>>,
}

fn pull_command(response: RemotingCommand) -> rocketmq_error::RocketMQResult<PullMessageResult> {
    Ok(PullMessageResult::Reply(BrokerResponseParts::command(response)?))
}

#[derive(Debug, thiserror::Error)]
enum PullClientIdentityError {
    #[error("the Pull session client lookup is not installed")]
    LookupUnavailable,
    #[error("the resumed Pull session has no current client registration")]
    RegistrationMissing,
}

impl<MS> RequestProcessor for PullMessageProcessor<MS>
where
    MS: BrokerReadStore + Send + Sync + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

impl<MS> PullMessageProcessor<MS>
where
    MS: BrokerReadStore + Send + Sync + 'static,
{
    /// Arc-callable leaf boundary used by the Broker aggregate.
    pub(crate) async fn process_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let request_code = RequestCode::from(request.original_identity().original_code());
        info!(?request_code, "PullMessageProcessor received a request");
        match request_code {
            RequestCode::PullMessage | RequestCode::LitePullMessage => {
                self.process_supported(request_code, request).await
            }
            _ => {
                warn!(?request_code, "PullMessageProcessor received an unknown request code");
                BrokerResponseParts::command(remoting_error_response(
                    PublicErrorView::descriptor_only(&rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED),
                    RemotingErrorTarget::Reply {
                        factory: &self.context.command_factory,
                        opaque: request.original_identity().original_opaque(),
                    },
                ))?
                .into_handler_outcome()
            }
        }
    }
}

impl<MS> PullMessageProcessor<MS>
where
    MS: BrokerReadStore,
{
    pub(crate) fn rejection_response(&self) -> Option<RemotingCommand> {
        let policy = self.context.policy();
        if !policy.slave_read_enable && policy.broker_role == BrokerRole::Slave {
            return Some(self.context.command_factory.create_response_command_with_code_remark(
                ResponseCode::SlaveNotAvailable,
                "the slave broker not allow to read",
            ));
        }
        None
    }
}

/// Result of subscription data retrieval operation.
struct SubscriptionDataResult {
    subscription_data: rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData,
    consumer_filter_data: Option<ConsumerFilterData>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StaticTopicMappingField {
    LeaderItem,
    CurrentItem,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StaticTopicMappingItem {
    RequestedOffset,
    Earliest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StaticTopicRequestField {
    TopicRequest,
    RpcHeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StaticTopicRewriteError {
    NotStaticTopic,
    IncompleteMapping(StaticTopicMappingField),
    InvalidLogicOffset(i64),
    MappingItemMissing(StaticTopicMappingItem),
    IncompleteRequest(StaticTopicRequestField),
    MissingResponseHeader,
}

impl StaticTopicRewriteError {
    fn response_code(self) -> ResponseCode {
        match self {
            Self::IncompleteMapping(StaticTopicMappingField::LeaderItem) => ResponseCode::NotLeaderForQueue,
            _ => ResponseCode::SystemError,
        }
    }

    fn client_remark(self) -> &'static str {
        match self {
            Self::NotStaticTopic => "static topic mapping is unavailable",
            Self::IncompleteMapping(_) => "static topic mapping is incomplete",
            Self::InvalidLogicOffset(_) => "static topic mapping contains an invalid logic offset",
            Self::MappingItemMissing(_) => "static topic mapping item is unavailable",
            Self::IncompleteRequest(_) => "static topic request metadata is incomplete",
            Self::MissingResponseHeader => "static topic response header is missing",
        }
    }
}

struct ValidatedStaticTopicMapping<'a> {
    mapping_detail: &'a TopicQueueMappingDetail,
    leader_item: &'a LogicQueueMappingItem,
    current_item: &'a LogicQueueMappingItem,
    earliest_item: &'a LogicQueueMappingItem,
    mapping_items: &'a [LogicQueueMappingItem],
    global_id: Option<i32>,
}

impl<'a> ValidatedStaticTopicMapping<'a> {
    fn from_context(mapping_context: &'a TopicQueueMappingContext) -> Result<Self, StaticTopicRewriteError> {
        let mapping_detail = mapping_context
            .mapping_detail
            .as_ref()
            .ok_or(StaticTopicRewriteError::NotStaticTopic)?;
        let leader_item = mapping_context
            .leader_item
            .as_ref()
            .ok_or(StaticTopicRewriteError::IncompleteMapping(
                StaticTopicMappingField::LeaderItem,
            ))?;
        let current_item = mapping_context
            .current_item
            .as_ref()
            .ok_or(StaticTopicRewriteError::IncompleteMapping(
                StaticTopicMappingField::CurrentItem,
            ))?;
        if current_item.logic_offset < 0 {
            return Err(StaticTopicRewriteError::InvalidLogicOffset(current_item.logic_offset));
        }
        let mapping_items = mapping_context.mapping_item_list.as_slice();
        let earliest_item = TopicQueueMappingUtils::find_logic_queue_mapping_item(mapping_items, 0, true).ok_or(
            StaticTopicRewriteError::MappingItemMissing(StaticTopicMappingItem::Earliest),
        )?;

        Ok(Self {
            mapping_detail,
            leader_item,
            current_item,
            earliest_item,
            mapping_items,
            global_id: mapping_context.global_id,
        })
    }
}

pub(super) fn static_topic_rewrite_error_response(
    command_factory: &RemotingCommandFactory,
    error: StaticTopicRewriteError,
    mapping_context: &TopicQueueMappingContext,
) -> RemotingCommand {
    warn!(
        topic = %mapping_context.topic,
        queue_id = ?mapping_context.global_id,
        ?error,
        "rejecting invalid static topic request state"
    );
    command_factory.create_response_command_with_code_remark(error.response_code(), error.client_remark())
}

impl<MS> PullMessageProcessor<MS>
where
    MS: BrokerReadStore,
{
    pub fn new(
        pull_message_result_handler: Arc<DefaultPullMessageResultHandler<MS>>,
        context: Arc<PullMessageProcessorContext<MS>>,
    ) -> Self {
        Self {
            pull_message_result_handler,
            context,
            session_client_lookup: OnceLock::new(),
            pull_deferred_service: OnceLock::new(),
        }
    }

    pub(crate) fn install_session_client_lookup(
        &self,
        lookup: Arc<dyn PullSessionClientLookup>,
    ) -> Result<(), Arc<dyn PullSessionClientLookup>> {
        self.session_client_lookup.set(lookup)
    }

    /// Installs the Broker-owned deferred Pull service.
    ///
    /// Broker composition owns the service lifecycle and calls this once.
    /// Requests that need suspension fail closed with `SERVICE_NOT_AVAILABLE`
    /// until installation completes.
    pub(crate) fn install_pull_deferred_service(
        &self,
        service: Arc<PullDeferredService>,
    ) -> Result<(), Arc<PullDeferredService>> {
        self.pull_deferred_service.set(service)
    }

    #[cfg(test)]
    pub(crate) fn pull_deferred_service_is_installed_for_test(&self) -> bool {
        self.pull_deferred_service.get().is_some()
    }

    #[cfg(test)]
    pub(crate) fn session_client_id_for_test(
        &self,
        session_id: SessionId,
        consumer_group: &CheetahString,
    ) -> Option<CheetahString> {
        self.session_client_lookup
            .get()
            .and_then(|lookup| lookup.client_id(session_id, consumer_group))
    }

    pub(crate) fn deferred_current_max_offset(&self, topic: &CheetahString, queue_id: i32) -> Option<i64> {
        self.context.store().max_offset(topic, queue_id).ok()
    }

    /// Creates an error response with the given code and remark.
    #[inline]
    fn error_response(
        response: RemotingCommand,
        code: impl Into<i32>,
        remark: impl Into<CheetahString>,
    ) -> RemotingCommand {
        response.set_code(code).set_remark(remark)
    }

    /// Creates an error response with a custom header.
    #[inline]
    fn error_response_with_header(
        response: RemotingCommand,
        code: impl Into<i32>,
        remark: impl Into<CheetahString>,
        header: PullMessageResponseHeader,
    ) -> RemotingCommand {
        response
            .set_code(code)
            .set_command_custom_header(header)
            .set_remark(remark)
    }

    pub async fn rewrite_request_for_static_topic(
        &self,
        request_header: &mut PullMessageRequestHeader,
        mapping_context: &mut TopicQueueMappingContext,
    ) -> Option<RemotingCommand> {
        let mapping_detail = mapping_context.mapping_detail.as_ref()?;
        if !mapping_context.is_leader() {
            return Some(static_topic_rewrite_error_response(
                self.context.command_factory(),
                StaticTopicRewriteError::IncompleteMapping(StaticTopicMappingField::LeaderItem),
                mapping_context,
            ));
        }

        let global_offset = request_header.queue_offset;
        let Some(mapping_item) = TopicQueueMappingUtils::find_logic_queue_mapping_item(
            &mapping_context.mapping_item_list,
            global_offset,
            true,
        ) else {
            return Some(static_topic_rewrite_error_response(
                self.context.command_factory(),
                StaticTopicRewriteError::MappingItemMissing(StaticTopicMappingItem::RequestedOffset),
                mapping_context,
            ));
        };
        mapping_context.current_item = Some(mapping_item.clone());

        if global_offset < mapping_item.logic_offset {
            // Handle offset moved...
        }

        let bname = &mapping_item.bname;
        let phy_queue_id = mapping_item.queue_id;
        let phy_queue_offset = mapping_item.compute_physical_queue_offset(global_offset);
        request_header.queue_id = phy_queue_id;
        request_header.queue_offset = phy_queue_offset;
        if mapping_item.check_if_end_offset_decided() {
            request_header.max_msg_nums = std::cmp::min(
                (mapping_item.end_offset - mapping_item.start_offset) as i32,
                request_header.max_msg_nums,
            );
        }

        if &mapping_detail.topic_queue_mapping_info.bname == bname {
            return None;
        }

        let mut sys_flag = request_header.sys_flag;
        let Some(topic_request) = request_header.topic_request.as_mut() else {
            return Some(static_topic_rewrite_error_response(
                self.context.command_factory(),
                StaticTopicRewriteError::IncompleteRequest(StaticTopicRequestField::TopicRequest),
                mapping_context,
            ));
        };
        topic_request.lo = Some(false);
        let Some(rpc_header) = topic_request.rpc.as_mut() else {
            return Some(static_topic_rewrite_error_response(
                self.context.command_factory(),
                StaticTopicRewriteError::IncompleteRequest(StaticTopicRequestField::RpcHeader),
                mapping_context,
            ));
        };
        rpc_header.broker_name = bname.clone();
        sys_flag = PullSysFlag::clear_suspend_flag(sys_flag as u32) as i32;
        sys_flag = PullSysFlag::clear_commit_offset_flag(sys_flag as u32) as i32;
        request_header.sys_flag = sys_flag;
        let rpc_request = RpcRequest::new(RequestCode::PullMessage.to_i32(), request_header.clone(), None);
        let forward_timeout = self.context.policy().forward_timeout;
        let rpc_response = self.context.rpc_client().invoke(rpc_request, forward_timeout).await;
        let mut rpc_response = match rpc_response {
            Ok(value) => value,
            Err(err) => {
                return Some(self.context.command_factory.create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("invoke rpc failed: {err:?}"),
                ));
            }
        };
        let response_code = ResponseCode::from(rpc_response.code);
        let Some(response_header) = rpc_response.get_header_mut::<PullMessageResponseHeader>() else {
            return Some(static_topic_rewrite_error_response(
                self.context.command_factory(),
                StaticTopicRewriteError::MissingResponseHeader,
                mapping_context,
            ));
        };
        match rewrite_response_for_static_topic(
            self.context.command_factory(),
            request_header,
            response_header,
            mapping_context,
            response_code,
        ) {
            Ok(Some(response)) => return Some(response),
            Ok(None) => {}
            Err(error) => {
                return Some(static_topic_rewrite_error_response(
                    self.context.command_factory(),
                    error,
                    mapping_context,
                ));
            }
        }
        Some(RpcClientUtils::create_command_for_rpc_response_with_factory(
            self.context.command_factory(),
            rpc_response,
        ))
    }

    /// Gets subscription data when HAS_SUBSCRIPTION_FLAG is set.
    ///
    /// Parses subscription from request and builds consumer filter data if needed.
    fn get_subscription_data_with_flag(
        &self,
        request_header: &PullMessageRequestHeader,
        response: &RemotingCommand,
    ) -> Result<SubscriptionDataResult, RemotingCommand> {
        let subscription_data = FilterAPI::build(
            request_header.topic.as_ref(),
            request_header
                .subscription
                .as_ref()
                .unwrap_or(&CheetahString::default()),
            request_header.expression_type.clone(),
        );
        if subscription_data.is_err() {
            return Err(Self::error_response(
                response.clone(),
                ResponseCode::SubscriptionParseFailed,
                "parse the consumer's subscription failed",
            ));
        }
        let subscription_data = subscription_data.unwrap();
        self.context.consumers().compensate_subscribe_data(
            request_header.consumer_group.as_ref(),
            request_header.topic.as_ref(),
            &subscription_data,
        );
        let consumer_filter_data = if !ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
            let consumer_filter_data = self.context.filters().resolve(
                request_header.topic.clone(),
                request_header.consumer_group.clone(),
                request_header.subscription.clone(),
                request_header.expression_type.clone(),
                request_header.sub_version as u64,
            );
            if consumer_filter_data.is_none() {
                return Err(Self::error_response(
                    response.clone(),
                    ResponseCode::SubscriptionParseFailed,
                    "parse the consumer's subscription failed",
                ));
            }
            consumer_filter_data
        } else {
            None
        };
        Ok(SubscriptionDataResult {
            subscription_data,
            consumer_filter_data,
        })
    }

    /// Gets subscription data when HAS_SUBSCRIPTION_FLAG is not set.
    ///
    /// Retrieves subscription from consumer group info stored on broker.
    fn get_subscription_data_without_flag(
        &self,
        request_header: &PullMessageRequestHeader,
        subscription_group_config: &SubscriptionGroupConfig,
        response: &RemotingCommand,
        response_header: &mut PullMessageResponseHeader,
    ) -> Result<SubscriptionDataResult, RemotingCommand> {
        let consumer_group_info = self
            .context
            .consumers()
            .get_consumer_group_info_internal(request_header.consumer_group.as_ref(), true);
        if consumer_group_info.is_none() {
            warn!(
                "the consumer's group info not exist, group: {}",
                request_header.consumer_group.as_str()
            );
            return Err(Self::error_response(
                response.clone(),
                ResponseCode::SubscriptionNotExist,
                format!(
                    "the consumer's group info not exist {}",
                    FAQUrl::suggest_todo(FAQUrl::SAME_GROUP_DIFFERENT_TOPIC),
                ),
            ));
        }
        let consumer_group_info = consumer_group_info.unwrap();

        if !subscription_group_config.consume_broadcast_enable()
            && consumer_group_info.get_message_model() == MessageModel::Broadcasting
        {
            response_header.forbidden_type = Some(ForbiddenType::BROADCASTING_DISABLE_FORBIDDEN);
            return Err(Self::error_response_with_header(
                response.clone(),
                ResponseCode::NoPermission,
                format!(
                    " the consumer group[{}] can not consume by broadcast way",
                    request_header.consumer_group.as_str(),
                ),
                response_header.clone(),
            ));
        }

        let read_forbidden = self.context.subscription_groups().get_forbidden(
            subscription_group_config.group_name(),
            &request_header.topic,
            PermName::INDEX_PERM_READ as i32,
        );
        if read_forbidden {
            response_header.forbidden_type = Some(ForbiddenType::SUBSCRIPTION_FORBIDDEN);
            return Err(Self::error_response_with_header(
                response.clone(),
                ResponseCode::NoPermission,
                format!(
                    "the consumer group[{}] is forbidden for topic[{}]",
                    request_header.consumer_group.as_str(),
                    request_header.topic
                ),
                response_header.clone(),
            ));
        }

        let subscription_data = consumer_group_info.find_subscription_data(request_header.topic.as_ref());
        if subscription_data.is_none() {
            warn!(
                "the consumer's subscription not exist, group: {}, topic:{}",
                request_header.consumer_group, request_header.topic
            );
            return Err(Self::error_response(
                response.clone(),
                ResponseCode::SubscriptionNotExist,
                format!(
                    "the consumer's subscription not exist {}",
                    FAQUrl::suggest_todo(FAQUrl::SAME_GROUP_DIFFERENT_TOPIC),
                ),
            ));
        }
        let subscription_data = subscription_data.unwrap();

        if subscription_data.sub_version < request_header.sub_version {
            warn!(
                "The broker's subscription is not latest, group: {} {}",
                request_header.consumer_group, subscription_data.sub_string
            );
            return Err(Self::error_response(
                response.clone(),
                ResponseCode::SubscriptionNotExist,
                "the consumer's subscription not latest",
            ));
        }

        let consumer_filter_data = if !ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
            let consumer_filter_data = self
                .context
                .filters()
                .get_consumer_filter_data(request_header.topic.as_ref(), request_header.consumer_group.as_ref());
            if consumer_filter_data.is_none() {
                return Err(Self::error_response(
                    response.clone(),
                    ResponseCode::FilterDataNotExist,
                    "The broker's consumer filter data is not exist!Your expression may be wrong!",
                ));
            }
            if consumer_filter_data.as_ref().unwrap().client_version() < request_header.sub_version as u64 {
                warn!(
                    "The broker's consumer filter data is not latest, group: {}, topic: {}, serverV: {}, clientV: {}",
                    request_header.consumer_group,
                    request_header.topic,
                    consumer_filter_data.as_ref().unwrap().client_version(),
                    request_header.sub_version,
                );
                return Err(Self::error_response(
                    response.clone(),
                    ResponseCode::FilterDataNotLatest,
                    "the consumer's consumer filter data not latest",
                ));
            }
            consumer_filter_data
        } else {
            None
        };

        Ok(SubscriptionDataResult {
            subscription_data,
            consumer_filter_data,
        })
    }

    /// Builds the message filter based on broker configuration and subscription data.
    fn build_message_filter(
        &self,
        subscription_data: &rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData,
        consumer_filter_data: Option<ConsumerFilterData>,
    ) -> ArcMessageFilter {
        // TODO: Consider optimizing consumer_filter_manager clone - Arc wrapper might be better
        if self.context.policy().filter_support_retry {
            Arc::new(ExpressionForRetryMessageFilter)
        } else {
            Arc::new(ExpressionMessageFilter::new(
                Some(subscription_data.clone()),
                consumer_filter_data,
                Arc::clone(self.context.filters()),
            ))
        }
    }
}

pub(super) fn rewrite_response_for_static_topic(
    command_factory: &RemotingCommandFactory,
    request_header: &PullMessageRequestHeader,
    response_header: &mut PullMessageResponseHeader,
    mapping_context: &mut TopicQueueMappingContext,
    code: ResponseCode,
) -> Result<Option<RemotingCommand>, StaticTopicRewriteError> {
    let validated_mapping = match ValidatedStaticTopicMapping::from_context(mapping_context) {
        Ok(validated_mapping) => validated_mapping,
        Err(StaticTopicRewriteError::NotStaticTopic) => return Ok(None),
        Err(error) => return Err(error),
    };
    let mapping_detail = validated_mapping.mapping_detail;
    let leader_item = validated_mapping.leader_item;
    let current_item = validated_mapping.current_item;
    let earliest_item = validated_mapping.earliest_item;
    let mapping_items = validated_mapping.mapping_items;

    let request_offset = request_header.queue_offset;
    let mut next_begin_offset = response_header.next_begin_offset;
    let mut min_offset = response_header.min_offset;
    let mut max_offset = response_header.max_offset;
    let mut response_code = code;

    if code != ResponseCode::Success {
        let mut is_revised = false;
        if leader_item.gen == current_item.gen {
            if request_offset > max_offset {
                if code == ResponseCode::PullOffsetMoved {
                    response_code = ResponseCode::PullOffsetMoved;
                    next_begin_offset = max_offset;
                } else {
                    response_code = code;
                }
            } else if request_offset < min_offset {
                next_begin_offset = min_offset;
                response_code = ResponseCode::PullRetryImmediately;
            } else {
                response_code = code;
            }
        }

        if earliest_item.gen == current_item.gen {
            if request_offset < min_offset {
                /*if code == ResponseCode::PullOffsetMoved {
                    response_code = ResponseCode::PullOffsetMoved;
                    next_begin_offset = min_offset;
                } else {
                    response_code = ResponseCode::PullOffsetMoved;
                    next_begin_offset = min_offset;
                }*/
                response_code = ResponseCode::PullOffsetMoved;
                next_begin_offset = min_offset;
            } else if request_offset >= max_offset {
                if let Some(next_item) = TopicQueueMappingUtils::find_next(mapping_items, Some(current_item), true) {
                    is_revised = true;
                    next_begin_offset = next_item.start_offset;
                    min_offset = next_item.start_offset;
                    max_offset = min_offset;
                    response_code = ResponseCode::PullRetryImmediately;
                } else {
                    response_code = ResponseCode::PullNotFound;
                }
            } else {
                response_code = code;
            }
        }

        if !is_revised && leader_item.gen != current_item.gen && earliest_item.gen != current_item.gen {
            if request_offset < min_offset {
                next_begin_offset = min_offset;
                response_code = ResponseCode::PullRetryImmediately;
            } else if request_offset >= max_offset {
                if let Some(next_item) = TopicQueueMappingUtils::find_next(mapping_items, Some(current_item), true) {
                    next_begin_offset = next_item.start_offset;
                    min_offset = next_item.start_offset;
                    max_offset = min_offset;
                    response_code = ResponseCode::PullRetryImmediately;
                } else {
                    response_code = ResponseCode::PullNotFound;
                }
            } else {
                response_code = code;
            }
        }
    }

    if current_item.check_if_end_offset_decided() && next_begin_offset >= current_item.end_offset {
        next_begin_offset = current_item.end_offset;
    }

    response_header.next_begin_offset = current_item.compute_static_queue_offset_strictly(next_begin_offset);
    response_header.min_offset =
        current_item.compute_static_queue_offset_strictly(min_offset.max(current_item.start_offset));
    response_header.max_offset = current_item.compute_static_queue_offset_strictly(max_offset).max(
        TopicQueueMappingDetail::compute_max_offset_from_mapping(mapping_detail, validated_mapping.global_id),
    );
    response_header.offset_delta = Some(current_item.compute_offset_delta());

    if code != ResponseCode::Success {
        Ok(Some(command_factory.create_response_command_with_code_and_header(
            response_code,
            response_header.clone(),
        )))
    } else {
        Ok(None)
    }
}

#[allow(unused_variables)]
impl<MS> PullMessageProcessor<MS>
where
    MS: BrokerReadStore + Send + Sync + 'static,
{
    async fn process_supported(
        &self,
        request_code: RequestCode,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let effective_peer = trusted_pull_peer(request)?;
        let session_id = request.session().id();
        let request_header = request
            .command()
            .decode_required_header_fast::<PullMessageRequestHeader>("decode pull-message request header")?;
        let hook_metadata = PullHookMetadata::from_command(request.command());
        let broadcast_client_resolver = |request_header: &PullMessageRequestHeader| {
            self.resolve_session_broadcast_client_id(request_header, session_id)
        };
        let result = self
            .execute_pull(
                request_code,
                request_header,
                effective_peer,
                &hook_metadata,
                &broadcast_client_resolver,
                true,
            )
            .await?;
        match result {
            PullMessageResult::Reply(parts) => parts.into_handler_outcome(),
            PullMessageResult::Suspend(suspension) => {
                let suspension = *suspension;
                let Some(service) = self.pull_deferred_service.get() else {
                    return BrokerResponseParts::command(
                        self.context.command_factory.create_response_command_with_code_remark(
                            ResponseCode::ServiceNotAvailable,
                            "the deferred Pull service is not installed",
                        ),
                    )?
                    .into_handler_outcome();
                };
                let criteria = PullMatchCriteria::new(
                    suspension.request_header.topic.clone(),
                    suspension.request_header.queue_id,
                    suspension.request_header.queue_offset,
                    suspension.subscription_data,
                    suspension.message_filter,
                );
                let fallback = suspension.fallback.into_remoting_response()?;
                let prepared = match service.prepare(
                    request,
                    criteria,
                    fallback,
                    suspension.timing,
                    PullRetainedEstimate::default(),
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(HandlerOutcome::Reply(error.into_fallback())),
                };
                match service.register(prepared, request) {
                    Ok(registration) => Ok(HandlerOutcome::Deferred(registration)),
                    Err(error) => match error.into_pre_take_fallback() {
                        Ok(fallback) => Ok(HandlerOutcome::Reply(fallback)),
                        Err(error) => self
                            .register_deferred_pull_error_outcome(request.original_identity().original_opaque(), error),
                    },
                }
            }
        }
    }

    fn register_deferred_pull_error_outcome(
        &self,
        opaque: i32,
        error: crate::long_polling::pull_deferred::PullDeferredRegisterError,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        use crate::long_polling::pull_deferred::PullDeferredRegisterError;

        match error {
            PullDeferredRegisterError::PreTake {
                prepared, source: _, ..
            } => {
                let fallback = (*prepared).into_candidate().into_fallback();
                Ok(HandlerOutcome::Reply(fallback))
            }
            PullDeferredRegisterError::Expiry { outcome: _, parts } => {
                drop(parts);
                BrokerResponseParts::command(remoting_error_response(
                    PublicErrorView::descriptor_only(&rocketmq_error::CORE_INTERNAL_FAILURE),
                    RemotingErrorTarget::Reply {
                        factory: &self.context.command_factory,
                        opaque,
                    },
                ))?
                .into_handler_outcome()
            }
            PullDeferredRegisterError::RegistryRejected => BrokerResponseParts::command(remoting_error_response(
                PublicErrorView::descriptor_only(&rocketmq_error::CORE_INTERNAL_FAILURE),
                RemotingErrorTarget::Reply {
                    factory: &self.context.command_factory,
                    opaque,
                },
            ))?
            .into_handler_outcome(),
            PullDeferredRegisterError::RegistryIdentityExhausted => {
                BrokerResponseParts::command(remoting_error_response(
                    PublicErrorView::descriptor_only(&rocketmq_error::CORE_INTERNAL_FAILURE),
                    RemotingErrorTarget::Reply {
                        factory: &self.context.command_factory,
                        opaque,
                    },
                ))?
                .into_handler_outcome()
            }
            PullDeferredRegisterError::RegistryContract(violation) => Err(rocketmq_error::RocketMQError::internal(
                "register deferred Pull request",
                violation,
            )),
            PullDeferredRegisterError::RegistryOperational(error) => Err(rocketmq_error::RocketMQError::internal(
                "register deferred Pull request",
                error,
            )),
            PullDeferredRegisterError::Contract { violation, parts } => {
                drop(parts);
                Err(rocketmq_error::RocketMQError::internal(
                    "register deferred Pull request",
                    violation,
                ))
            }
        }
    }

    #[allow(unused_assignments)]
    #[allow(clippy::too_many_arguments, reason = "typed Pull execution inputs are kept explicit")]
    async fn execute_pull(
        &self,
        request_code: RequestCode,
        mut request_header: PullMessageRequestHeader,
        effective_peer: std::net::SocketAddr,
        hook_metadata: &PullHookMetadata,
        broadcast_client_resolver: &PullBroadcastClientResolver<'_>,
        broker_allow_suspend: bool,
    ) -> rocketmq_error::RocketMQResult<PullMessageResult> {
        let begin_time_mills = current_millis();
        let response = self
            .context
            .command_factory
            .create_java_default_error_response_command();
        //info!("receive pull message request: {:?}", request_header);
        let mut response_header = PullMessageResponseHeader::default();
        let policy = self.context.policy();

        if !PermName::is_readable(policy.broker_permission.get()) {
            response_header.forbidden_type = Some(ForbiddenType::BROKER_FORBIDDEN);
            return pull_command(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(format!("the broker[{}] pulling message is forbidden", policy.broker_ip)),
            );
        }
        if RequestCode::LitePullMessage == request_code && !policy.lite_pull_message_enable {
            response_header.forbidden_type = Some(ForbiddenType::BROKER_FORBIDDEN);
            return pull_command(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(format!("the broker[{}] pulling message is forbidden", policy.broker_ip)),
            );
        }
        let subscription_group_config = self
            .context
            .subscription_groups()
            .find_subscription_group_config(request_header.consumer_group.as_ref());

        if subscription_group_config.is_none() {
            return pull_command(
                response
                    .set_code(ResponseCode::SubscriptionGroupNotExist)
                    .set_remark(format!(
                        "subscription group [{}] does not exist, {}",
                        request_header.consumer_group,
                        FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                    )),
            );
        }

        if !subscription_group_config.as_ref().unwrap().consume_enable() {
            response_header.forbidden_type = Some(ForbiddenType::GROUP_FORBIDDEN);
            return pull_command(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(format!(
                        "subscription group no permission, {}",
                        request_header.consumer_group,
                    )),
            );
        }
        let topic_config = self.context.topics().select_topic_config(request_header.topic.as_ref());
        if topic_config.is_none() {
            error!(
                "the topic {} not exist, consumer: {}",
                request_header.topic, effective_peer
            );
            return pull_command(response.set_code(ResponseCode::TopicNotExist).set_remark(format!(
                "topic[{}] not exist, apply first please! {}",
                request_header.topic,
                FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
            )));
        }
        if !PermName::is_readable(topic_config.as_ref().unwrap().perm) {
            response_header.forbidden_type = Some(ForbiddenType::TOPIC_FORBIDDEN);
            return pull_command(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(format!(
                        "the topic[{}] pulling message is forbidden",
                        request_header.topic,
                    )),
            );
        }
        let mut topic_queue_mapping_context = self
            .context
            .topic_mappings()
            .build_topic_queue_mapping_context(&request_header, false);
        if let Some(resp) = self
            .rewrite_request_for_static_topic(&mut request_header, &mut topic_queue_mapping_context)
            .await
        {
            return pull_command(resp);
        }
        if request_header.queue_id < 0
            || request_header.queue_id >= topic_config.as_ref().unwrap().read_queue_nums as i32
        {
            return pull_command(
                response
                    .set_code(RemotingSysResponseCode::SystemError)
                    .set_remark(format!(
                        "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                        request_header.queue_id,
                        request_header.topic,
                        topic_config.as_ref().unwrap().read_queue_nums,
                        effective_peer
                    )),
            );
        }
        let (consume_type, message_model) =
            consumer_compensation_for_request_source(RequestSource::parse_integer(request_header.request_source));
        self.context.consumers().compensate_basic_consumer_info(
            request_header.consumer_group.as_ref(),
            consume_type,
            message_model,
        );
        let has_subscription_flag = PullSysFlag::has_subscription_flag(request_header.sys_flag as u32);

        // Get subscription data and consumer filter data using helper methods
        let subscription_result = if has_subscription_flag {
            self.get_subscription_data_with_flag(&request_header, &response)
        } else {
            self.get_subscription_data_without_flag(
                &request_header,
                subscription_group_config.as_ref().unwrap(),
                &response,
                &mut response_header,
            )
        };

        let SubscriptionDataResult {
            subscription_data,
            consumer_filter_data,
        } = match subscription_result {
            Ok(result) => result,
            Err(err_response) => return pull_command(err_response),
        };

        if !ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str()))
            && !policy.enable_property_filter
        {
            return pull_command(
                response
                    .set_code(RemotingSysResponseCode::SystemError)
                    .set_remark(format!(
                        "The broker does not support consumer to filter message by {}",
                        subscription_data.expression_type
                    )),
            );
        }

        // Build message filter using helper method
        let message_filter = self.build_message_filter(&subscription_data, consumer_filter_data);

        // ColdDataFlow control
        cfg_if::cfg_if! {
            if #[cfg(feature = "local_file_store")] {
                if let Some(cold_data_cg_ctr_service) = self.context.cold_data_flow() {
                    if cold_data_cg_ctr_service.is_cg_need_cold_data_flow_ctr(request_header.consumer_group.as_str()) {
                        let is_msg_logic_cold = self.context.store().is_message_in_cold_area(
                            &request_header.consumer_group,
                            &request_header.topic,
                            request_header.queue_id,
                            request_header.queue_offset,
                        ).unwrap_or(false);

                        if is_msg_logic_cold {
                            let consumer_group_info = self
                                .context
                                .consumers()
                                .get_consumer_group_info(request_header.consumer_group.as_ref());

                            if let Some(ref cg_info) = consumer_group_info {
                                match cg_info.get_consume_type() {
                                    ConsumeType::ConsumePassively => {
                                        return pull_command(
                                            response
                                                .set_code(ResponseCode::SystemBusy)
                                                .set_remark("This consumer group is reading cold data. It has been flow control"),
                                        );
                                    }
                                    ConsumeType::ConsumeActively => {
                                        if broker_allow_suspend
                                            && cold_data_cg_ctr_service
                                                .short_suspend_active_read()
                                                .await
                                                == crate::coldctr::cold_data_cg_ctr_service::ColdDataShortSuspendOutcome::QueueFull
                                        {
                                            return pull_command(
                                                response
                                                    .set_code(ResponseCode::SystemBusy)
                                                    .set_remark("Cold-data pull suspension queue is full"),
                                            );
                                        }
                                        request_header.max_msg_nums = 1;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        let use_reset_offset_feature = policy.use_server_side_reset_offset;
        let topic = request_header.topic.as_ref();
        let group = request_header.consumer_group.as_ref();
        let queue_id = request_header.queue_id;
        let reset_offset = self.context.query_reset_offset(topic, group, queue_id);
        let get_message_result = if let (true, Some(reset_offset)) = (use_reset_offset_feature, reset_offset) {
            let (Ok(min_offset), Ok(max_offset)) = (
                self.context.store().min_offset(topic, queue_id),
                self.context.store().max_offset(topic, queue_id),
            ) else {
                return pull_command(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is unavailable"),
                );
            };
            let mut get_message_result = GetMessageResult::new();
            get_message_result.set_status(Some(GetMessageStatus::OffsetReset));
            get_message_result.set_next_begin_offset(reset_offset);
            get_message_result.set_min_offset(min_offset);
            get_message_result.set_max_offset(max_offset);
            get_message_result.set_suggest_pulling_from_slave(false);
            Some(get_message_result)
        } else {
            let broadcast_init_client_id = broadcast_client_resolver(&request_header)?;
            let broadcast_init_offset = self.query_broadcast_pull_init_offset(
                topic,
                group,
                queue_id,
                &request_header,
                broadcast_init_client_id.as_ref(),
            );
            drop(broadcast_init_client_id);
            if broadcast_init_offset >= 0 {
                let mut get_message_result = GetMessageResult::new();
                get_message_result.set_status(Some(GetMessageStatus::OffsetReset));
                get_message_result.set_next_begin_offset(broadcast_init_offset);
                Some(get_message_result)
            } else {
                let result = match self
                    .context
                    .store()
                    .get_message(
                        group,
                        topic,
                        queue_id,
                        request_header.queue_offset,
                        request_header.max_msg_nums,
                        store_read_max_msg_bytes(request_header.max_msg_bytes),
                        message_filter.clone(),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        return pull_command(
                            response
                                .set_code(ResponseCode::SystemError)
                                .set_remark("message store is unavailable"),
                        );
                    }
                };
                if result.is_none() {
                    return pull_command(
                        response
                            .set_code(ResponseCode::SystemError)
                            .set_remark("store getMessage return None"),
                    );
                }
                // Accumulate cold data read bytes for flow control
                if let Some(ref result) = result {
                    if let Some(cold_data_cg_ctr_service) = self.context.cold_data_flow() {
                        cold_data_cg_ctr_service.cold_acc(group.as_str(), result.cold_data_sum());
                    }
                }
                result
            }
        };
        if let Some(get_message_result) = get_message_result {
            let result = self
                .pull_message_result_handler
                .handle(
                    get_message_result,
                    request_header,
                    subscription_data,
                    &subscription_group_config.unwrap(),
                    message_filter,
                    response,
                    topic_queue_mapping_context,
                    PullResponseContext {
                        effective_peer,
                        hook_metadata,
                        broadcast_client_resolver,
                        allow_suspend: broker_allow_suspend,
                        begin_time_millis: begin_time_mills,
                    },
                )
                .await?;
            return Ok(result);
        }
        pull_command(
            response
                .set_code(ResponseCode::SystemError)
                .set_remark("store getMessage return None"),
        )
    }

    fn query_broadcast_pull_init_offset(
        &self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        request_header: &PullMessageRequestHeader,
        client_id: Option<&CheetahString>,
    ) -> i64 {
        if !self.context.policy().enable_broadcast_offset_store {
            return -1;
        }
        let consumer_group_info = self.context.consumers().get_consumer_group_info(group);
        let proxy_pull_broadcast =
            RequestSource::ProxyForBroadcast == From::from(request_header.request_source.unwrap_or(-2));

        if is_broadcast(proxy_pull_broadcast, consumer_group_info.as_ref()) {
            let client_id = if proxy_pull_broadcast {
                request_header.proxy_forward_client_id.as_ref()
            } else {
                client_id
            };
            let Some(client_id) = client_id else {
                return -1;
            };
            return self.context.query_broadcast_offset(
                topic,
                group,
                queue_id,
                client_id.as_str(),
                request_header.queue_offset,
                proxy_pull_broadcast,
            );
        }
        -1
    }

    fn resolve_session_broadcast_client_id(
        &self,
        request_header: &PullMessageRequestHeader,
        session_id: SessionId,
    ) -> rocketmq_error::RocketMQResult<Option<CheetahString>> {
        self.resolve_broadcast_client_id_with(request_header, || {
            let lookup = self.session_client_lookup.get().ok_or_else(|| {
                rocketmq_error::RocketMQError::internal(
                    "resume-pull-client-lookup",
                    PullClientIdentityError::LookupUnavailable,
                )
            })?;
            lookup
                .client_id(session_id, &request_header.consumer_group)
                .map(Some)
                .ok_or_else(|| {
                    rocketmq_error::RocketMQError::internal(
                        "resume-pull-client-lookup",
                        PullClientIdentityError::RegistrationMissing,
                    )
                })
        })
    }

    fn resolve_broadcast_client_id_with(
        &self,
        request_header: &PullMessageRequestHeader,
        resolve_normal_client: impl FnOnce() -> rocketmq_error::RocketMQResult<Option<CheetahString>>,
    ) -> rocketmq_error::RocketMQResult<Option<CheetahString>> {
        if !self.context.policy().enable_broadcast_offset_store {
            return Ok(None);
        }
        let proxy = RequestSource::ProxyForBroadcast == From::from(request_header.request_source.unwrap_or(-2));
        if proxy {
            return Ok(request_header.proxy_forward_client_id.clone());
        }
        let consumer_group_info = self
            .context
            .consumers()
            .get_consumer_group_info(request_header.consumer_group.as_ref());
        if !is_broadcast(false, consumer_group_info.as_ref()) {
            return Ok(None);
        }
        resolve_normal_client()
    }
}

fn trusted_pull_peer(request: &RemotingRequest) -> rocketmq_error::RocketMQResult<std::net::SocketAddr> {
    match (request.origin(), request.session()) {
        (
            RequestOrigin::Network { peer },
            SessionView::Network {
                remote_addr,
                transport_peer_addr: _,
                ..
            },
        ) if peer.address() == *remote_addr => Ok(*remote_addr),
        _ => Err(rocketmq_error::RocketMQError::invariant_violated(
            "Pull request origin does not match its network session view",
        )),
    }
}

pub(crate) fn is_broadcast(proxy_pull_broadcast: bool, consumer_group_info: Option<&ConsumerGroupInfo>) -> bool {
    proxy_pull_broadcast
        || consumer_group_info.is_some_and(|info| {
            matches!(info.get_message_model(), MessageModel::Broadcasting)
                && matches!(info.get_consume_type(), ConsumeType::ConsumePassively)
        })
}

fn consumer_compensation_for_request_source(request_source: RequestSource) -> (ConsumeType, MessageModel) {
    match request_source {
        RequestSource::ProxyForBroadcast => (ConsumeType::ConsumePassively, MessageModel::Broadcasting),
        RequestSource::ProxyForStream => (ConsumeType::ConsumeActively, MessageModel::Clustering),
        _ => (ConsumeType::ConsumePassively, MessageModel::Clustering),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/processor/pull_message.rs"]
mod processor_tests;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::config::broker_config::BrokerConfig;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_model::common::filter::expression_type::ExpressionType;
    use rocketmq_model::common::sys_flag::pull_sys_flag::PullSysFlag;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
    use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
    use rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
    use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
    use rocketmq_protocol::protocol::request_source::RequestSource;
    use rocketmq_protocol::protocol::static_topic::logic_queue_mapping_item::LogicQueueMappingItem;
    use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
    use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
    use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_info::TopicQueueMappingInfo;
    use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
    use rocketmq_store::BrokerReadStore;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_store::MAX_PULL_MSG_SIZE;

    use super::consumer_compensation_for_request_source;
    use super::is_broadcast;
    use super::rewrite_response_for_static_topic;
    use super::static_topic_rewrite_error_response;
    use super::store_read_max_msg_bytes;
    use super::PullMessageProcessor;
    use super::StaticTopicMappingField;
    use super::StaticTopicMappingItem;
    use super::StaticTopicRewriteError;
    use crate::broker_runtime::BrokerRuntime;
    use crate::client::consumer_group_info::ConsumerGroupInfo;
    use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
    use crate::processor::pull_message_processor::capability::PullMessageProcessorContext;

    fn temp_test_root(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("rocketmq-rust-pull-message-{}-{}", std::process::id(), label));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp test root");
        path
    }

    async fn new_test_runtime(label: &str, enable_property_filter: bool) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            enable_property_filter,
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

    fn new_processor<MS: BrokerReadStore>(context: Arc<PullMessageProcessorContext<MS>>) -> PullMessageProcessor<MS> {
        let handler = Arc::new(DefaultPullMessageResultHandler::new(
            Arc::new(vec![]),
            Arc::clone(&context),
            None,
        ));
        PullMessageProcessor::new(handler, context)
    }

    fn request_with_subscription(topic: &str, group: &str, expression: &str, version: i64) -> PullMessageRequestHeader {
        PullMessageRequestHeader {
            consumer_group: group.into(),
            topic: topic.into(),
            queue_id: 0,
            queue_offset: 0,
            max_msg_nums: 32,
            sys_flag: PullSysFlag::build_sys_flag(false, false, true, false) as i32,
            commit_offset: 0,
            suspend_timeout_millis: 0,
            sub_version: version,
            subscription: Some(expression.into()),
            expression_type: Some(ExpressionType::SQL92.into()),
            ..Default::default()
        }
    }

    fn request_without_subscription(topic: &str, group: &str, version: i64) -> PullMessageRequestHeader {
        PullMessageRequestHeader {
            consumer_group: group.into(),
            topic: topic.into(),
            queue_id: 0,
            queue_offset: 0,
            max_msg_nums: 32,
            sys_flag: PullSysFlag::build_sys_flag(false, false, false, false) as i32,
            commit_offset: 0,
            suspend_timeout_millis: 0,
            sub_version: version,
            ..Default::default()
        }
    }

    fn static_topic_mapping_item(generation: i32, logic_offset: i64) -> LogicQueueMappingItem {
        LogicQueueMappingItem {
            gen: generation,
            queue_id: 3,
            bname: Some("broker-a".into()),
            logic_offset,
            start_offset: 0,
            end_offset: 100,
            time_of_start: -1,
            time_of_end: -1,
        }
    }

    fn static_topic_mapping_context(
        mapping_items: Vec<LogicQueueMappingItem>,
        leader_item: Option<LogicQueueMappingItem>,
        current_item: Option<LogicQueueMappingItem>,
    ) -> TopicQueueMappingContext {
        let mapping_detail = TopicQueueMappingDetail {
            topic_queue_mapping_info: TopicQueueMappingInfo {
                topic: Some("topic-a".into()),
                total_queues: 1,
                bname: Some("broker-a".into()),
                ..TopicQueueMappingInfo::default()
            },
            hosted_queues: Some(HashMap::from([(0, mapping_items.clone())])),
        };
        TopicQueueMappingContext {
            topic: "topic-a".into(),
            global_id: Some(0),
            mapping_detail: Some(mapping_detail),
            mapping_item_list: mapping_items,
            leader_item,
            current_item,
        }
    }

    fn assert_static_topic_rewrite_error(
        mut mapping_context: TopicQueueMappingContext,
        expected_error: StaticTopicRewriteError,
        expected_code: ResponseCode,
        expected_remark: &str,
    ) {
        let request_header = PullMessageRequestHeader::default();
        let mut response_header = PullMessageResponseHeader::default();
        let error = match rewrite_response_for_static_topic(
            &application_remoting_command_factory(),
            &request_header,
            &mut response_header,
            &mut mapping_context,
            ResponseCode::Success,
        ) {
            Err(error) => error,
            Ok(_) => panic!("incomplete static topic mapping should be rejected"),
        };

        assert_eq!(error, expected_error);
        let response =
            static_topic_rewrite_error_response(&application_remoting_command_factory(), error, &mapping_context);
        assert_eq!(response.code(), expected_code as i32);
        assert_eq!(response.remark().map(|remark| remark.as_str()), Some(expected_remark));
        assert!(!response.remark().is_some_and(|remark| remark.contains("broker-a")));
    }

    #[test]
    fn static_topic_response_rewrite_ignores_non_static_topics() {
        let request_header = PullMessageRequestHeader::default();
        let mut response_header = PullMessageResponseHeader::default();
        let mut mapping_context = TopicQueueMappingContext::default();

        let result = rewrite_response_for_static_topic(
            &application_remoting_command_factory(),
            &request_header,
            &mut response_header,
            &mut mapping_context,
            ResponseCode::Success,
        );

        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn static_topic_response_rewrite_rejects_missing_leader() {
        let current_item = static_topic_mapping_item(0, 0);
        let mapping_context = static_topic_mapping_context(vec![current_item.clone()], None, Some(current_item));

        assert_static_topic_rewrite_error(
            mapping_context,
            StaticTopicRewriteError::IncompleteMapping(StaticTopicMappingField::LeaderItem),
            ResponseCode::NotLeaderForQueue,
            "static topic mapping is incomplete",
        );
    }

    #[test]
    fn static_topic_response_rewrite_rejects_missing_current_item() {
        let leader_item = static_topic_mapping_item(0, 0);
        let mapping_context = static_topic_mapping_context(vec![leader_item.clone()], Some(leader_item), None);

        assert_static_topic_rewrite_error(
            mapping_context,
            StaticTopicRewriteError::IncompleteMapping(StaticTopicMappingField::CurrentItem),
            ResponseCode::SystemError,
            "static topic mapping is incomplete",
        );
    }

    #[test]
    fn static_topic_response_rewrite_rejects_empty_mapping_list() {
        let leader_item = static_topic_mapping_item(0, 0);
        let current_item = leader_item.clone();
        let mapping_context = static_topic_mapping_context(vec![], Some(leader_item), Some(current_item));

        assert_static_topic_rewrite_error(
            mapping_context,
            StaticTopicRewriteError::MappingItemMissing(StaticTopicMappingItem::Earliest),
            ResponseCode::SystemError,
            "static topic mapping item is unavailable",
        );
    }

    #[test]
    fn static_topic_response_rewrite_rejects_missing_earliest_item() {
        let unavailable_item = static_topic_mapping_item(0, -1);
        let leader_item = static_topic_mapping_item(1, 0);
        let current_item = leader_item.clone();
        let mapping_context =
            static_topic_mapping_context(vec![unavailable_item], Some(leader_item), Some(current_item));

        assert_static_topic_rewrite_error(
            mapping_context,
            StaticTopicRewriteError::MappingItemMissing(StaticTopicMappingItem::Earliest),
            ResponseCode::SystemError,
            "static topic mapping item is unavailable",
        );
    }

    #[test]
    fn static_topic_response_rewrite_rejects_negative_current_logic_offset() {
        let leader_item = static_topic_mapping_item(0, 0);
        let current_item = static_topic_mapping_item(0, -1);
        let mapping_context =
            static_topic_mapping_context(vec![leader_item.clone()], Some(leader_item), Some(current_item));

        assert_static_topic_rewrite_error(
            mapping_context,
            StaticTopicRewriteError::InvalidLogicOffset(-1),
            ResponseCode::SystemError,
            "static topic mapping contains an invalid logic offset",
        );
    }

    #[test]
    fn static_topic_response_rewrite_preserves_valid_non_success_response_codes() {
        for response_code in [
            ResponseCode::PullNotFound,
            ResponseCode::PullRetryImmediately,
            ResponseCode::PullOffsetMoved,
        ] {
            let mapping_item = static_topic_mapping_item(0, 0);
            let mut mapping_context = static_topic_mapping_context(
                vec![mapping_item.clone()],
                Some(mapping_item.clone()),
                Some(mapping_item),
            );
            let request_header = PullMessageRequestHeader {
                queue_offset: 5,
                ..PullMessageRequestHeader::default()
            };
            let mut response_header = PullMessageResponseHeader {
                next_begin_offset: 5,
                min_offset: 0,
                max_offset: 10,
                ..PullMessageResponseHeader::default()
            };

            let mut response = match rewrite_response_for_static_topic(
                &application_remoting_command_factory(),
                &request_header,
                &mut response_header,
                &mut mapping_context,
                response_code,
            ) {
                Ok(Some(response)) => response,
                _ => panic!("valid static topic response should be rewritten"),
            };

            assert_eq!(response.code(), response_code as i32);
            let encoded_header = response
                .read_custom_header_mut::<PullMessageResponseHeader>()
                .expect("rewritten pull response should retain its typed header");
            assert_eq!(
                (
                    encoded_header.suggest_which_broker_id,
                    encoded_header.next_begin_offset,
                    encoded_header.min_offset,
                    encoded_header.max_offset,
                    encoded_header.offset_delta,
                    encoded_header.topic_sys_flag,
                    encoded_header.group_sys_flag,
                    encoded_header.forbidden_type,
                ),
                (
                    response_header.suggest_which_broker_id,
                    response_header.next_begin_offset,
                    response_header.min_offset,
                    response_header.max_offset,
                    response_header.offset_delta,
                    response_header.topic_sys_flag,
                    response_header.group_sys_flag,
                    response_header.forbidden_type,
                )
            );
        }
    }

    #[test]
    fn static_topic_response_rewrite_preserves_valid_success_path() {
        let mapping_item = static_topic_mapping_item(0, 0);
        let mut mapping_context = static_topic_mapping_context(
            vec![mapping_item.clone()],
            Some(mapping_item.clone()),
            Some(mapping_item),
        );
        let request_header = PullMessageRequestHeader::default();
        let mut response_header = PullMessageResponseHeader::default();

        let result = rewrite_response_for_static_topic(
            &application_remoting_command_factory(),
            &request_header,
            &mut response_header,
            &mut mapping_context,
            ResponseCode::Success,
        );

        assert!(matches!(result, Ok(None)));
        assert_eq!(response_header.offset_delta, Some(0));
    }

    async fn register_consumer_group_without_subscriptions<MS: BrokerReadStore>(
        context: &PullMessageProcessorContext<MS>,
        group: &str,
        _client_id: &str,
    ) {
        context.consumers().compensate_basic_consumer_info(
            &group.into(),
            ConsumeType::ConsumePassively,
            MessageModel::Clustering,
        );
    }

    fn inject_subscription<MS: BrokerReadStore>(
        context: &PullMessageProcessorContext<MS>,
        group: &str,
        topic: &str,
        expression: &str,
        version: i64,
    ) {
        let group = CheetahString::from(group);
        let topic = CheetahString::from(topic);
        let subscription = SubscriptionData {
            topic: topic.clone(),
            sub_string: expression.into(),
            expression_type: ExpressionType::SQL92.into(),
            sub_version: version,
            ..Default::default()
        };
        context
            .consumers()
            .compensate_subscribe_data(&group, &topic, &subscription);
    }

    #[test]
    fn returns_true_for_proxy_pull_broadcast() {
        let result = is_broadcast(true, None);
        assert!(result, "Should return true when proxy_pull_broadcast is true");
    }

    #[test]
    fn returns_false_for_non_broadcast_and_active_consumption() {
        let consumer_group_info = ConsumerGroupInfo::new(
            "test_group".to_string(),
            ConsumeType::ConsumeActively,
            MessageModel::Clustering,
            ConsumeFromWhere::ConsumeFromLastOffset,
        );
        let result = is_broadcast(false, Some(&consumer_group_info));
        assert!(!result, "Should return false for non-broadcast and active consumption");
    }

    #[test]
    fn returns_true_for_broadcast_and_passive_consumption() {
        let consumer_group_info = ConsumerGroupInfo::new(
            "test_group".to_string(),
            ConsumeType::ConsumePassively,
            MessageModel::Broadcasting,
            ConsumeFromWhere::ConsumeFromLastOffset,
        );
        let result = is_broadcast(false, Some(&consumer_group_info));
        assert!(result, "Should return true for broadcast and passive consumption");
    }

    #[test]
    fn returns_false_when_no_consumer_group_info_provided() {
        let result = is_broadcast(false, None);
        assert!(!result, "Should return false when no consumer group info is provided");
    }

    #[test]
    fn proxy_for_broadcast_compensates_broadcasting_passive_consumer() {
        let (consume_type, message_model) = consumer_compensation_for_request_source(RequestSource::ProxyForBroadcast);
        assert_eq!(consume_type, ConsumeType::ConsumePassively);
        assert_eq!(message_model, MessageModel::Broadcasting);
    }

    #[test]
    fn proxy_for_stream_compensates_clustering_active_consumer() {
        let (consume_type, message_model) = consumer_compensation_for_request_source(RequestSource::ProxyForStream);
        assert_eq!(consume_type, ConsumeType::ConsumeActively);
        assert_eq!(message_model, MessageModel::Clustering);
    }

    #[test]
    fn unknown_request_source_falls_back_to_passive_clustering() {
        let (consume_type, message_model) = consumer_compensation_for_request_source(RequestSource::Unknown);
        assert_eq!(consume_type, ConsumeType::ConsumePassively);
        assert_eq!(message_model, MessageModel::Clustering);
    }

    #[test]
    fn store_read_max_msg_bytes_uses_header_or_store_default() {
        assert_eq!(store_read_max_msg_bytes(Some(4096)), 4096);
        assert_eq!(store_read_max_msg_bytes(Some(0)), MAX_PULL_MSG_SIZE);
        assert_eq!(store_read_max_msg_bytes(Some(-1)), MAX_PULL_MSG_SIZE);
        assert_eq!(store_read_max_msg_bytes(None), MAX_PULL_MSG_SIZE);
    }

    #[test]
    fn get_subscription_data_with_flag_builds_request_scoped_filter_data() {
        let async_runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
        let runtime = async_runtime.block_on(new_test_runtime("with-flag-builds-request-scoped", true));
        let context = runtime.pull_message_context_for_test();
        let processor = new_processor(Arc::clone(&context));
        let request_header = request_with_subscription("topic-a", "group-a", "color = 'blue'", 11);

        let result = match processor.get_subscription_data_with_flag(
            &request_header,
            &RemotingCommand::create_java_default_error_response_command(),
        ) {
            Ok(result) => result,
            Err(_) => panic!("subscription with flag should parse"),
        };

        let filter_data = result
            .consumer_filter_data
            .expect("request scoped filter data should be returned");
        assert!(filter_data.compiled_expression().is_some());
        assert!(filter_data.bloom_filter_data().is_some());
        assert!(context
            .filters()
            .get_consumer_filter_data(&"topic-a".into(), &"group-a".into())
            .is_none());

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[test]
    fn get_subscription_data_with_flag_reuses_registered_filter_data() {
        let async_runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
        let runtime = async_runtime.block_on(new_test_runtime("with-flag-reuses-registered", true));
        let context = runtime.pull_message_context_for_test();
        context.filters().register(
            "group-a",
            &std::collections::HashSet::from([SubscriptionData {
                topic: "topic-a".into(),
                sub_string: "color = 'blue'".into(),
                expression_type: ExpressionType::SQL92.into(),
                sub_version: 11,
                ..Default::default()
            }]),
        );
        let registered = context
            .filters()
            .get_consumer_filter_data(&"topic-a".into(), &"group-a".into())
            .expect("registered filter data should exist");
        let processor = new_processor(Arc::clone(&context));
        let request_header = request_with_subscription("topic-a", "group-a", "color = 'blue'", 11);

        let result = match processor.get_subscription_data_with_flag(
            &request_header,
            &RemotingCommand::create_java_default_error_response_command(),
        ) {
            Ok(result) => result,
            Err(_) => panic!("subscription with flag should parse"),
        };

        let resolved = result
            .consumer_filter_data
            .expect("resolved filter data should be returned");
        assert!(std::sync::Arc::ptr_eq(
            registered.compiled_expression().as_ref().unwrap(),
            resolved.compiled_expression().as_ref().unwrap()
        ));

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[test]
    fn get_subscription_data_without_flag_returns_filter_data_not_exist_when_sql_filter_missing() {
        let async_runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
        let runtime = async_runtime.block_on(new_test_runtime("without-flag-filter-missing", true));
        let context = runtime.pull_message_context_for_test();
        async_runtime.block_on(register_consumer_group_without_subscriptions(
            &context, "group-a", "client-a",
        ));
        inject_subscription(&context, "group-a", "topic-a", "color = 'blue'", 11);
        let processor = new_processor(Arc::clone(&context));
        let request_header = request_without_subscription("topic-a", "group-a", 11);
        let mut response_header = PullMessageResponseHeader::default();

        let response = match processor.get_subscription_data_without_flag(
            &request_header,
            &SubscriptionGroupConfig::new("group-a".into()),
            &RemotingCommand::create_java_default_error_response_command(),
            &mut response_header,
        ) {
            Ok(_) => panic!("missing consumer filter data should be rejected"),
            Err(response) => response,
        };

        assert_eq!(response.code(), ResponseCode::FilterDataNotExist as i32);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[test]
    fn get_subscription_data_without_flag_returns_filter_data_not_latest_when_filter_version_lags() {
        let async_runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
        let runtime = async_runtime.block_on(new_test_runtime("without-flag-filter-stale", true));
        let context = runtime.pull_message_context_for_test();
        async_runtime.block_on(register_consumer_group_without_subscriptions(
            &context, "group-a", "client-a",
        ));
        inject_subscription(&context, "group-a", "topic-a", "color = 'blue'", 11);
        context.filters().register(
            "group-a",
            &HashSet::from([SubscriptionData {
                topic: "topic-a".into(),
                sub_string: "color = 'blue'".into(),
                expression_type: ExpressionType::SQL92.into(),
                sub_version: 10,
                ..Default::default()
            }]),
        );
        let processor = new_processor(Arc::clone(&context));
        let request_header = request_without_subscription("topic-a", "group-a", 11);
        let mut response_header = PullMessageResponseHeader::default();

        let response = match processor.get_subscription_data_without_flag(
            &request_header,
            &SubscriptionGroupConfig::new("group-a".into()),
            &RemotingCommand::create_java_default_error_response_command(),
            &mut response_header,
        ) {
            Ok(_) => panic!("stale consumer filter data should be rejected"),
            Err(response) => response,
        };

        assert_eq!(response.code(), ResponseCode::FilterDataNotLatest as i32);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
