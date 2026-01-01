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
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::constant::PermName;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::filter::filter_api::FilterAPI;
use rocketmq_remoting::protocol::forbidden_type::ForbiddenType;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::request_source::RequestSource;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_detail::TopicQueueMappingDetail;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_remoting::rpc::rpc_client::RpcClient;
use rocketmq_remoting::rpc::rpc_client_utils::RpcClientUtils;
use rocketmq_remoting::rpc::rpc_request::RpcRequest;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor::RejectRequestResponse;
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::get_message_result::GetMessageResult;
use rocketmq_store::base::message_status_enum::GetMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::filter::MessageFilter;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::client::consumer_group_info::ConsumerGroupInfo;
use crate::coldctr::cold_data_pull_request_hold_service::ColdDataPullRequest;
use crate::coldctr::cold_data_pull_request_hold_service::NO_SUSPEND_KEY;
use crate::filter::consumer_filter_data::ConsumerFilterData;
use crate::filter::expression_for_retry_message_filter::ExpressionForRetryMessageFilter;
use crate::filter::expression_message_filter::ExpressionMessageFilter;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;
use crate::processor::pull_message_result_handler::PullMessageResultHandler;

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
pub struct PullMessageProcessor<MS: MessageStore> {
    pull_message_result_handler: ArcMut<DefaultPullMessageResultHandler<MS>>,
    write_message_runtime: Arc<RocketMQRuntime>,
    /// Lock to serialize writes to channels when waking up suspended requests.
    ///
    /// This is a global lock which may become a bottleneck under high concurrency.
    /// Future optimization: consider per-channel locks for better parallelism.
    write_message_lock: Arc<Mutex<()>>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> RequestProcessor for PullMessageProcessor<MS>
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
        info!("PullMessageProcessor received request code: {:?}", request_code);
        match request_code {
            RequestCode::PullMessage | RequestCode::LitePullMessage => {
                self.process_request_(channel, ctx, request_code, request).await
            }
            _ => {
                warn!("PullMessageProcessor received unknown request code: {:?}", request_code);
                let response = RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::RequestCodeNotSupported,
                    format!("ClientManageProcessor request code {} not supported", request.code()),
                );
                Ok(Some(response.set_opaque(request.opaque())))
            }
        }
    }

    fn reject_request(&self, _code: i32) -> RejectRequestResponse {
        if !self.broker_runtime_inner.broker_config().slave_read_enable
            && self.broker_runtime_inner.message_store_config().broker_role == BrokerRole::Slave
        {
            return (
                true,
                Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SlaveNotAvailable,
                    "the slave broker not allow to read",
                )),
            );
        }
        (false, None)
    }
}

/// Result of subscription data retrieval operation.
struct SubscriptionDataResult {
    subscription_data: rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData,
    consumer_filter_data: Option<ConsumerFilterData>,
}

impl<MS> PullMessageProcessor<MS>
where
    MS: MessageStore,
{
    pub fn new(
        pull_message_result_handler: ArcMut<DefaultPullMessageResultHandler<MS>>,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        let cpus = num_cpus::get();
        Self {
            pull_message_result_handler,
            write_message_runtime: Arc::new(RocketMQRuntime::new_multi(cpus, "write_consumer_message_runtime")),
            write_message_lock: Arc::new(Default::default()),
            broker_runtime_inner,
        }
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
        mapping_context.mapping_detail.as_ref()?;
        let mapping_detail = mapping_context.mapping_detail.as_ref().unwrap();
        let topic = mapping_context.topic.as_str();
        let global_id = mapping_context.global_id;
        if !mapping_context.is_leader() {
            return Some(RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::NotLeaderForQueue,
                format!(
                    "{}-{} cannot find mapping item in request process of current broker {}",
                    topic,
                    global_id.unwrap_or_default(),
                    mapping_detail
                        .topic_queue_mapping_info
                        .bname
                        .clone()
                        .unwrap_or_default()
                ),
            ));
        }

        let global_offset = request_header.queue_offset;
        let mapping_item = TopicQueueMappingUtils::find_logic_queue_mapping_item(
            &mapping_context.mapping_item_list,
            global_offset,
            true,
        )?;
        mapping_context.current_item = Some(mapping_item.clone());

        if global_offset < mapping_item.logic_offset {
            // Handle offset moved...
        }

        let bname = &mapping_item.bname;
        let phy_queue_id = mapping_item.queue_id;
        let phy_queue_offset = mapping_item.compute_physical_queue_offset(global_offset);
        request_header.queue_id = phy_queue_id;
        request_header.queue_offset = phy_queue_offset;
        if mapping_item.check_if_end_offset_decided()
        /* && request_header.max_msg_nums.is_some() */
        {
            request_header.max_msg_nums = std::cmp::min(
                (mapping_item.end_offset - mapping_item.start_offset) as i32,
                request_header.max_msg_nums,
            );
        }

        if &mapping_detail.topic_queue_mapping_info.bname == bname {
            return None;
        }

        let mut sys_flag = request_header.sys_flag;
        let topic_request = request_header.topic_request.as_mut().unwrap();
        topic_request.lo = Some(false);
        topic_request.rpc.as_mut().unwrap().broker_name = bname.clone();
        sys_flag = PullSysFlag::clear_suspend_flag(sys_flag as u32) as i32;
        sys_flag = PullSysFlag::clear_commit_offset_flag(sys_flag as u32) as i32;
        request_header.sys_flag = sys_flag;
        let rpc_request = RpcRequest::new(RequestCode::PullMessage.to_i32(), request_header.clone(), None);
        let rpc_response = self
            .broker_runtime_inner
            .broker_outer_api()
            .rpc_client()
            .invoke(rpc_request, self.broker_runtime_inner.broker_config().forward_timeout)
            .await;
        let rpc_response = match rpc_response {
            Ok(value) => value,
            Err(err) => {
                return Some(RemotingCommand::create_response_command_with_code_remark(
                    ResponseCode::SystemError,
                    format!("invoke rpc failed: {err:?}"),
                ));
            }
        };
        let response_header = rpc_response.get_header_mut_from_ref::<PullMessageResponseHeader>();
        let rewrite_result = rewrite_response_for_static_topic(
            request_header,
            response_header?,
            mapping_context,
            ResponseCode::from(rpc_response.code),
        );
        if rewrite_result.is_some() {
            return rewrite_result;
        }
        Some(RpcClientUtils::create_command_for_rpc_response(rpc_response))
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
        self.broker_runtime_inner.consumer_manager().compensate_subscribe_data(
            request_header.consumer_group.as_ref(),
            request_header.topic.as_ref(),
            &subscription_data,
        );
        let consumer_filter_data = if !ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
            let consumer_filter_data = ConsumerFilterManager::build(
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
            .broker_runtime_inner
            .consumer_manager()
            .get_consumer_group_info(request_header.consumer_group.as_ref());
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

        let read_forbidden = self.broker_runtime_inner.subscription_group_manager().get_forbidden(
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
                .broker_runtime_inner
                .consumer_filter_manager()
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
        subscription_data: &rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData,
        consumer_filter_data: Option<ConsumerFilterData>,
    ) -> Arc<Box<dyn MessageFilter>> {
        // TODO: Consider optimizing consumer_filter_manager clone - Arc wrapper might be better
        if self.broker_runtime_inner.broker_config().filter_support_retry {
            Arc::new(Box::new(ExpressionForRetryMessageFilter))
        } else {
            Arc::new(Box::new(ExpressionMessageFilter::new(
                Some(subscription_data.clone()),
                consumer_filter_data,
                Arc::new(self.broker_runtime_inner.consumer_filter_manager().clone()),
            )))
        }
    }
}

pub fn rewrite_response_for_static_topic(
    request_header: &PullMessageRequestHeader,
    response_header: &mut PullMessageResponseHeader,
    mapping_context: &mut TopicQueueMappingContext,
    code: ResponseCode,
) -> Option<RemotingCommand> {
    mapping_context.mapping_detail.as_ref()?;
    let mapping_detail = mapping_context.mapping_detail.as_ref().unwrap();
    let leader_item = mapping_context.leader_item.as_ref().unwrap();
    let current_item = mapping_context.current_item.as_ref().unwrap();
    let mapping_items = &mut mapping_context.mapping_item_list;
    let earlist_item = TopicQueueMappingUtils::find_logic_queue_mapping_item(mapping_items, 0, true).unwrap();

    assert!(current_item.logic_offset >= 0);

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

        if earlist_item.gen == current_item.gen {
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

        if !is_revised && leader_item.gen != current_item.gen && earlist_item.gen != current_item.gen {
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
        TopicQueueMappingDetail::compute_max_offset_from_mapping(mapping_detail, mapping_context.global_id),
    );
    response_header.offset_delta = Some(current_item.compute_offset_delta());

    if code != ResponseCode::Success {
        Some(RemotingCommand::create_response_command_with_header(response_header.clone()).set_code(response_code))
    } else {
        None
    }
}

#[allow(unused_variables)]
impl<MS> PullMessageProcessor<MS>
where
    MS: MessageStore + Send + Sync + 'static,
{
    /// Processes a pull message request with all the entry point options.
    pub async fn process_request_(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.process_request_inner(request_code, channel, ctx, request, true, true)
            .await
    }

    /// Core pull message processing logic.
    ///
    /// # Processing Flow
    ///
    /// 1. **Permission Check**: Validates broker and topic read permissions
    /// 2. **Subscription Validation**: Validates subscription group and consumer info
    /// 3. **Topic Validation**: Checks topic existence and queue ID validity
    /// 4. **Subscription Data**: Retrieves or parses subscription data
    /// 5. **Cold Data Flow Control**: Applies flow control for cold data reads
    /// 6. **Message Retrieval**: Gets messages from message store
    /// 7. **Result Handling**: Delegates to `PullMessageResultHandler`
    ///
    /// # Arguments
    ///
    /// * `broker_allow_suspend` - Whether the broker allows suspending the request
    /// * `broker_allow_flow_ctr_suspend` - Whether cold data flow control suspension is allowed
    ///
    /// # Returns
    ///
    /// * `Ok(Some(response))` - Response to send to client
    /// * `Ok(None)` - Request was suspended (cold data flow control or long polling)
    #[allow(unused_assignments)]
    async fn process_request_inner(
        &mut self,
        request_code: RequestCode,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
        broker_allow_suspend: bool,
        broker_allow_flow_ctr_suspend: bool,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let begin_time_mills = get_current_millis();
        let mut response = RemotingCommand::create_response_command();
        response.set_opaque_mut(request.opaque());
        let mut request_header = request
            .decode_command_custom_header_fast::<PullMessageRequestHeader>()
            .unwrap();
        //info!("receive pull message request: {:?}", request_header);
        let mut response_header = PullMessageResponseHeader::default();

        if !PermName::is_readable(self.broker_runtime_inner.broker_config().broker_permission) {
            response_header.forbidden_type = Some(ForbiddenType::BROKER_FORBIDDEN);
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(format!(
                        "the broker[{}] pulling message is forbidden",
                        self.broker_runtime_inner.broker_config().broker_ip1
                    )),
            ));
        }
        if RequestCode::LitePullMessage == request_code
            && !self.broker_runtime_inner.broker_config().lite_pull_message_enable
        {
            response_header.forbidden_type = Some(ForbiddenType::BROKER_FORBIDDEN);
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(format!(
                        "the broker[{}] pulling message is forbidden",
                        self.broker_runtime_inner.broker_config().broker_ip1
                    )),
            ));
        }
        let subscription_group_config = self
            .broker_runtime_inner
            .subscription_group_manager()
            .find_subscription_group_config(request_header.consumer_group.as_ref());

        if subscription_group_config.is_none() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SubscriptionGroupNotExist)
                    .set_remark(format!(
                        "subscription group [{}] does not exist, {}",
                        request_header.consumer_group,
                        FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                    )),
            ));
        }

        if !subscription_group_config.as_ref().unwrap().consume_enable() {
            response_header.forbidden_type = Some(ForbiddenType::GROUP_FORBIDDEN);
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(format!(
                        "subscription group no permission, {}",
                        request_header.consumer_group,
                    )),
            ));
        }
        let topic_config = self
            .broker_runtime_inner
            .topic_config_manager()
            .select_topic_config(request_header.topic.as_ref());
        if topic_config.is_none() {
            error!(
                "the topic {} not exist, consumer: {}",
                request_header.topic,
                channel.remote_address()
            );
            return Ok(Some(response.set_code(ResponseCode::TopicNotExist).set_remark(
                format!(
                    "topic[{}] not exist, apply first please! {}",
                    request_header.topic,
                    FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                ),
            )));
        }
        if !PermName::is_readable(topic_config.as_ref().unwrap().perm) {
            response_header.forbidden_type = Some(ForbiddenType::TOPIC_FORBIDDEN);
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_command_custom_header(response_header)
                    .set_remark(format!(
                        "the topic[{}] pulling message is forbidden",
                        request_header.topic,
                    )),
            ));
        }
        let mut topic_queue_mapping_context = self
            .broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);
        if let Some(resp) = self
            .rewrite_request_for_static_topic(&mut request_header, &mut topic_queue_mapping_context)
            .await
        {
            return Ok(Some(resp));
        }
        if request_header.queue_id < 0
            || request_header.queue_id >= topic_config.as_ref().unwrap().read_queue_nums as i32
        {
            return Ok(Some(
                response
                    .set_code(RemotingSysResponseCode::SystemError)
                    .set_remark(format!(
                        "queueId[{}] is illegal, topic:[{}] topicConfig.readQueueNums:[{}] consumer:[{}]",
                        request_header.queue_id,
                        request_header.topic,
                        topic_config.as_ref().unwrap().read_queue_nums,
                        channel.remote_address()
                    )),
            ));
        }
        match RequestSource::parse_integer(request_header.request_source) {
            RequestSource::ProxyForBroadcast => {
                unimplemented!("ProxyForBroadcast not implement")
            }
            RequestSource::ProxyForStream => {
                unimplemented!("ProxyForStream not implement")
            }
            _ => self
                .broker_runtime_inner
                .consumer_manager()
                .compensate_basic_consumer_info(
                    request_header.consumer_group.as_ref(),
                    ConsumeType::ConsumePassively,
                    MessageModel::Clustering,
                ),
        }
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
            Err(err_response) => return Ok(Some(err_response)),
        };

        if !ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str()))
            && !self.broker_runtime_inner.broker_config().enable_property_filter
        {
            return Ok(Some(
                response
                    .set_code(RemotingSysResponseCode::SystemError)
                    .set_remark(format!(
                        "The broker does not support consumer to filter message by {}",
                        subscription_data.expression_type
                    )),
            ));
        }

        // Build message filter using helper method
        let message_filter = self.build_message_filter(&subscription_data, consumer_filter_data);

        // ColdDataFlow control
        cfg_if::cfg_if! {
            if #[cfg(feature = "local_file_store")] {
                if let Some(cold_data_cg_ctr_service) = self.broker_runtime_inner.cold_data_cg_ctr_service() {
                    if cold_data_cg_ctr_service.is_cg_need_cold_data_flow_ctr(request_header.consumer_group.as_str()) {
                        // Check if message is in cold data area
                        if let Some(message_store) = self.broker_runtime_inner.message_store() {
                            let is_msg_logic_cold = message_store
                                .get_commit_log()
                                .get_cold_data_check_service()
                                .is_msg_in_cold_area(
                                    &request_header.consumer_group,
                                    &request_header.topic,
                                    request_header.queue_id,
                                    request_header.queue_offset,
                                );

                            if is_msg_logic_cold {
                                // Get consumer type
                                let consumer_group_info = self
                                    .broker_runtime_inner
                                    .consumer_manager()
                                    .get_consumer_group_info(request_header.consumer_group.as_ref());

                                if let Some(ref cg_info) = consumer_group_info {
                                    match cg_info.get_consume_type() {
                                        ConsumeType::ConsumePassively => {
                                            // PUSH consumer: return SYSTEM_BUSY immediately
                                            return Ok(Some(
                                                response
                                                    .set_code(ResponseCode::SystemBusy)
                                                    .set_remark("This consumer group is reading cold data. It has been flow control"),
                                            ));
                                        }
                                        ConsumeType::ConsumeActively => {
                                            // PULL consumer: suspend request if allowed
                                            if broker_allow_flow_ctr_suspend {
                                                if let Some(cold_data_hold_service) = self.broker_runtime_inner.cold_data_pull_request_hold_service() {
                                                    let now = self.broker_runtime_inner.message_store().unwrap().now();
                                                    let pull_request = ColdDataPullRequest::new(
                                                        request.clone(),
                                                        channel.clone(),
                                                        1000, // timeout millis
                                                        now,
                                                        request_header.queue_offset,
                                                        subscription_data.clone(),
                                                        message_filter.clone(),
                                                    );
                                                    cold_data_hold_service.suspend_cold_data_read_request(pull_request);
                                                    return Ok(None);
                                                }
                                            }
                                            // If not suspended, limit to 1 message
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
        }

        let use_reset_offset_feature = self.broker_runtime_inner.broker_config().use_server_side_reset_offset;
        let topic = request_header.topic.as_ref();
        let group = request_header.consumer_group.as_ref();
        let queue_id = request_header.queue_id;
        let reset_offset = self
            .broker_runtime_inner
            .consumer_offset_manager()
            .query_then_erase_reset_offset(topic, group, queue_id);
        let get_message_result = if let (true, Some(reset_offset)) = (use_reset_offset_feature, reset_offset) {
            let mut get_message_result = GetMessageResult::new();
            get_message_result.set_status(Some(GetMessageStatus::OffsetReset));
            get_message_result.set_next_begin_offset(reset_offset);
            get_message_result.set_min_offset(
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_min_offset_in_queue(topic, queue_id),
            );
            get_message_result.set_max_offset(
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_max_offset_in_queue(topic, queue_id),
            );
            get_message_result.set_suggest_pulling_from_slave(false);
            Some(get_message_result)
        } else {
            let broadcast_init_offset =
                self.query_broadcast_pull_init_offset(topic, group, queue_id, &request_header, &channel);
            if broadcast_init_offset >= 0 {
                let mut get_message_result = GetMessageResult::new();
                get_message_result.set_status(Some(GetMessageStatus::OffsetReset));
                get_message_result.set_next_begin_offset(broadcast_init_offset);
                Some(get_message_result)
            } else {
                let result = self
                    .broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_message(
                        group,
                        topic,
                        queue_id,
                        request_header.queue_offset,
                        request_header.max_msg_nums,
                        //   MAX_PULL_MSG_SIZE,
                        Some(message_filter.clone()),
                    )
                    .await;
                if result.is_none() {
                    return Ok(Some(
                        response
                            .set_code(ResponseCode::SystemError)
                            .set_remark("store getMessage return None"),
                    ));
                }
                // Accumulate cold data read bytes for flow control
                if let Some(ref result) = result {
                    if let Some(cold_data_cg_ctr_service) = self.broker_runtime_inner.cold_data_cg_ctr_service() {
                        cold_data_cg_ctr_service.cold_acc(group.as_str(), result.cold_data_sum());
                    }
                }
                result
            }
        };
        if let Some(get_message_result) = get_message_result {
            return Ok(self
                .pull_message_result_handler
                .handle(
                    get_message_result,
                    request,
                    request_header,
                    channel,
                    ctx,
                    subscription_data,
                    &subscription_group_config.unwrap(),
                    broker_allow_suspend,
                    message_filter,
                    response,
                    topic_queue_mapping_context,
                    begin_time_mills,
                )
                .await);
        }
        Ok(None)
    }

    fn query_broadcast_pull_init_offset(
        &mut self,
        topic: &CheetahString,
        group: &CheetahString,
        queue_id: i32,
        request_header: &PullMessageRequestHeader,
        channel: &Channel,
    ) -> i64 {
        if !self.broker_runtime_inner.broker_config().enable_broadcast_offset_store {
            return -1;
        }
        let consumer_group_info = self
            .broker_runtime_inner
            .consumer_manager()
            .get_consumer_group_info(group);
        let proxy_pull_broadcast =
            RequestSource::ProxyForBroadcast == From::from(request_header.request_source.unwrap_or(-2));

        if is_broadcast(proxy_pull_broadcast, consumer_group_info.as_ref()) {
            let client_id = if proxy_pull_broadcast {
                request_header.proxy_forward_client_id.as_ref().cloned()
            } else {
                match consumer_group_info.as_ref().unwrap().find_channel_by_channel(channel) {
                    None => {
                        return -1;
                    }
                    Some(value) => Some(value.client_id().clone()),
                }
            };
            return self.broker_runtime_inner.broadcast_offset_manager().query_init_offset(
                topic,
                group,
                queue_id,
                client_id.as_ref().unwrap().as_str(),
                request_header.queue_offset,
                proxy_pull_broadcast,
            );
        }
        -1
    }

    pub fn execute_request_when_wakeup(
        &self,
        mut pull_message_processor: ArcMut<PullMessageProcessor<MS>>,
        channel: Channel,
        mut ctx: ConnectionHandlerContext,
        mut request: RemotingCommand,
    ) {
        let lock = Arc::clone(&self.write_message_lock);
        self.write_message_runtime.get_handle().spawn(async move {
            let broker_allow_flow_ctr_suspend =
                !(request.ext_fields().is_some() && request.ext_fields().unwrap().contains_key(NO_SUSPEND_KEY));
            let opaque = request.opaque();
            let response = pull_message_processor
                .process_request_inner(
                    RequestCode::from(request.code()),
                    channel,
                    ctx.clone(),
                    &mut request,
                    false,
                    broker_allow_flow_ctr_suspend,
                )
                .await;

            if let Ok(Some(response)) = response {
                let command = response.set_opaque(opaque).mark_response_type();

                let guard = lock.lock().await;
                ctx.write_response(command).await;
                drop(guard);
            }
        });
    }
}
pub(crate) fn is_broadcast(proxy_pull_broadcast: bool, consumer_group_info: Option<&ConsumerGroupInfo>) -> bool {
    proxy_pull_broadcast
        || consumer_group_info.is_some_and(|info| {
            matches!(info.get_message_model(), MessageModel::Broadcasting)
                && matches!(info.get_consume_type(), ConsumeType::ConsumePassively)
        })
}

#[cfg(test)]
mod tests {
    use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
    use rocketmq_remoting::protocol::heartbeat::consume_type::ConsumeType;
    use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

    use super::*;
    use crate::client::consumer_group_info::ConsumerGroupInfo;

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
}
