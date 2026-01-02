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
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rand::Rng;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_decoder;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_rust::ArcMut;

use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
use crate::consumer::pop_callback::PopCallback;
use crate::consumer::pull_callback::PullCallback;
use crate::consumer::pull_status::PullStatus;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::filter_message_context::FilterMessageContext;
use crate::hook::filter_message_hook::FilterMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;

#[derive(Clone)]
pub struct PullAPIWrapper {
    client_instance: ArcMut<MQClientInstance>,
    consumer_group: CheetahString,
    unit_mode: bool,
    pull_from_which_node_table: ArcMut<HashMap<MessageQueue, AtomicU64>>,
    connect_broker_by_user: bool,
    default_broker_id: u64,
    filter_message_hook_list: Vec<Arc<Box<dyn FilterMessageHook + Send + Sync>>>,
}

impl PullAPIWrapper {
    pub fn new(mq_client_factory: ArcMut<MQClientInstance>, consumer_group: CheetahString, unit_mode: bool) -> Self {
        Self {
            client_instance: mq_client_factory,
            consumer_group,
            unit_mode,
            pull_from_which_node_table: ArcMut::new(HashMap::with_capacity(64)),
            connect_broker_by_user: false,
            default_broker_id: mix_all::MASTER_ID,
            filter_message_hook_list: Vec::new(),
        }
    }

    pub fn register_filter_message_hook(
        &mut self,
        filter_message_hook_list: Vec<Arc<Box<dyn FilterMessageHook + Send + Sync>>>,
    ) {
        self.filter_message_hook_list = filter_message_hook_list;
    }

    #[inline]
    pub fn update_pull_from_which_node(&mut self, mq: &MessageQueue, broker_id: u64) {
        let atomic_u64 = self
            .pull_from_which_node_table
            .entry(mq.clone())
            .or_insert_with(|| AtomicU64::new(broker_id));
        atomic_u64.store(broker_id, std::sync::atomic::Ordering::Release);
    }

    pub fn has_hook(&self) -> bool {
        !self.filter_message_hook_list.is_empty()
    }

    pub fn process_pull_result(
        &mut self,
        message_queue: &MessageQueue,
        pull_result_ext: &mut PullResultExt,
        subscription_data: &SubscriptionData,
    ) {
        self.update_pull_from_which_node(message_queue, pull_result_ext.suggest_which_broker_id);
        if PullStatus::Found == pull_result_ext.pull_result.pull_status {
            let mut message_binary = pull_result_ext.message_binary.take().unwrap_or_default();
            let mut msg_vec = message_decoder::decodes_batch(
                &mut message_binary,
                self.client_instance.client_config.decode_read_body,
                self.client_instance.client_config.decode_decompress_body,
            );

            let mut need_decode_inner_message = false;
            for msg in &msg_vec {
                if MessageSysFlag::check(msg.sys_flag, MessageSysFlag::INNER_BATCH_FLAG)
                    && MessageSysFlag::check(msg.sys_flag, MessageSysFlag::NEED_UNWRAP_FLAG)
                {
                    need_decode_inner_message = true;
                    break;
                }
            }
            if need_decode_inner_message {
                let mut inner_msg_vec = Vec::with_capacity(msg_vec.len());
                for msg in msg_vec {
                    if MessageSysFlag::check(msg.sys_flag, MessageSysFlag::INNER_BATCH_FLAG)
                        && MessageSysFlag::check(msg.sys_flag, MessageSysFlag::NEED_UNWRAP_FLAG)
                    {
                        message_decoder::decode_messages_from(msg, &mut inner_msg_vec);
                    } else {
                        inner_msg_vec.push(msg);
                    }
                }
                msg_vec = inner_msg_vec;
            }
            // filter message
            let mut msg_list_filter_again =
                if !subscription_data.tags_set.is_empty() && !subscription_data.class_filter_mode {
                    let mut msg_vec_again = Vec::with_capacity(msg_vec.len());
                    for msg in msg_vec {
                        if let Some(ref tag) = msg.get_tags() {
                            if subscription_data.tags_set.contains(tag.as_str()) {
                                msg_vec_again.push(msg);
                            }
                        }
                    }
                    msg_vec_again
                } else {
                    msg_vec
                };
            if self.has_hook() {
                let context = FilterMessageContext {
                    unit_mode: self.unit_mode,
                    msg_list: &msg_list_filter_again,
                    ..Default::default()
                };
                self.execute_hook(&context);
            }

            for msg in &mut msg_list_filter_again {
                let tra_flag = msg
                    .get_property(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_TRANSACTION_PREPARED,
                    ))
                    .is_some_and(|v| v.parse().unwrap_or(false));
                if tra_flag {
                    if let Some(transaction_id) = msg.get_property(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                    )) {
                        msg.set_transaction_id(transaction_id);
                    }
                }
                MessageAccessor::put_property(
                    msg,
                    CheetahString::from_static_str(MessageConst::PROPERTY_MIN_OFFSET),
                    CheetahString::from_string(pull_result_ext.pull_result.min_offset.to_string()),
                );
                MessageAccessor::put_property(
                    msg,
                    CheetahString::from_static_str(MessageConst::PROPERTY_MAX_OFFSET),
                    CheetahString::from_string(pull_result_ext.pull_result.max_offset.to_string()),
                );
                msg.broker_name = CheetahString::from_string(message_queue.get_broker_name().to_string());
                msg.queue_id = message_queue.get_queue_id();
                if let Some(offset_delta) = pull_result_ext.offset_delta {
                    msg.queue_offset += offset_delta;
                }
            }
            pull_result_ext.pull_result.msg_found_list =
                Some(msg_list_filter_again.into_iter().map(ArcMut::new).collect::<Vec<_>>());
        }
    }

    pub fn execute_hook(&self, context: &FilterMessageContext) {
        for hook in &self.filter_message_hook_list {
            hook.filter_message(context);
        }
    }

    pub fn recalculate_pull_from_which_node(&self, mq: &MessageQueue) -> u64 {
        if self.connect_broker_by_user {
            return self.default_broker_id;
        }

        if let Some(atomic_u64) = self.pull_from_which_node_table.get(mq) {
            atomic_u64.load(std::sync::atomic::Ordering::Acquire)
        } else {
            mix_all::MASTER_ID
        }
    }

    /// Pulls messages from the broker asynchronously.
    ///
    /// # Arguments
    ///
    /// * `mq` - A reference to the `MessageQueue` from which to pull messages.
    /// * `sub_expression` - The subscription expression.
    /// * `expression_type` - The type of the subscription expression.
    /// * `sub_version` - The version of the subscription.
    /// * `offset` - The offset from which to start pulling messages.
    /// * `max_nums` - The maximum number of messages to pull.
    /// * `max_size_in_bytes` - The maximum size of messages to pull in bytes.
    /// * `sys_flag` - The system flag for the pull request.
    /// * `commit_offset` - The commit offset.
    /// * `broker_suspend_max_time_millis` - The maximum time in milliseconds for which the broker
    ///   can suspend the pull request.
    /// * `timeout_millis` - The timeout for the pull request in milliseconds.
    /// * `communication_mode` - The communication mode (e.g., sync, async).
    /// * `pull_callback` - The callback to execute when the pull request completes.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` with the `PullResultExt` if successful, or an
    /// `MQClientError` if an error occurs.
    pub async fn pull_kernel_impl<PCB>(
        &mut self,
        mq: &MessageQueue,
        sub_expression: CheetahString,
        expression_type: CheetahString,
        sub_version: i64,
        offset: i64,
        max_nums: i32,
        max_size_in_bytes: i32,
        sys_flag: i32,
        commit_offset: i64,
        broker_suspend_max_time_millis: u64,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        pull_callback: PCB,
    ) -> rocketmq_error::RocketMQResult<Option<PullResultExt>>
    where
        PCB: PullCallback + 'static,
    {
        let broker_name = self.client_instance.get_broker_name_from_message_queue(mq).await;
        let broker_id = self.recalculate_pull_from_which_node(mq);
        let mut find_broker_result = self
            .client_instance
            .find_broker_address_in_subscribe(&broker_name, broker_id, false)
            .await;

        if find_broker_result.is_none() {
            self.client_instance
                .update_topic_route_info_from_name_server_topic(mq.get_topic_cs())
                .await;
            let broker_name_again = self.client_instance.get_broker_name_from_message_queue(mq).await;
            let broker_id_again = self.recalculate_pull_from_which_node(mq);
            find_broker_result = self
                .client_instance
                .find_broker_address_in_subscribe(&broker_name_again, broker_id_again, false)
                .await;
        }

        if let Some(find_broker_result) = find_broker_result {
            {
                if !ExpressionType::is_tag_type(Some(expression_type.as_str()))
                    && find_broker_result.broker_version < RocketMqVersion::V4_1_0_SNAPSHOT as i32
                {
                    return Err(mq_client_err!(format!(
                        "The broker[{}],[{}] does not support consumer to filter message by tag[{}]",
                        mq.get_broker_name(),
                        find_broker_result.broker_version,
                        expression_type
                    )));
                }
            }

            let mut sys_flag_inner = sys_flag;
            if find_broker_result.slave {
                sys_flag_inner = PullSysFlag::clear_commit_offset_flag(sys_flag_inner as u32) as i32;
            }

            let request_header = PullMessageRequestHeader {
                consumer_group: self.consumer_group.clone(),
                topic: CheetahString::from_string(mq.get_topic().to_string()),
                queue_id: mq.get_queue_id(),
                queue_offset: offset,
                max_msg_nums: max_nums,
                sys_flag: sys_flag_inner,
                commit_offset,
                suspend_timeout_millis: broker_suspend_max_time_millis,
                subscription: Some(CheetahString::from_string(sub_expression.to_string())),
                sub_version,
                max_msg_bytes: Some(max_size_in_bytes),
                request_source: None,
                proxy_forward_client_id: None,
                expression_type: Some(CheetahString::from_string(expression_type.to_string())),
                topic_request: Some(TopicRequestHeader {
                    lo: None,
                    rpc: Some(RpcRequestHeader {
                        namespace: None,
                        namespaced: None,
                        broker_name: Some(CheetahString::from_string(mq.get_broker_name().to_string())),
                        oneway: None,
                    }),
                }),
            };

            let mut broker_addr = find_broker_result.broker_addr.clone();
            if PullSysFlag::has_class_filter_flag(sys_flag_inner as u32) {
                broker_addr = self
                    .compute_pull_from_which_filter_server(mq.get_topic_cs(), &broker_addr)
                    .await?;
            }

            MQClientAPIImpl::pull_message(
                self.client_instance.get_mq_client_api_impl(),
                broker_addr,
                request_header,
                timeout_millis,
                communication_mode,
                pull_callback,
            )
            .await
        } else {
            Err(mq_client_err!(format!(
                "The broker[{}] not exist",
                mq.get_broker_name(),
            )))
        }
    }

    async fn compute_pull_from_which_filter_server(
        &mut self,
        topic: &CheetahString,
        broker_addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        let topic_route_table = self.client_instance.topic_route_table.read().await;
        let topic_route_data = topic_route_table.get(topic);
        let vec = topic_route_data.unwrap().filter_server_table.get(broker_addr);
        if let Some(vec) = vec {
            return vec.get(random_num() as usize % vec.len()).map_or(
                Err(mq_client_err!(format!(
                    "Find Filter Server Failed, Broker Addr: {},topic:{}",
                    broker_addr, topic
                ))),
                |v| Ok(v.clone()),
            );
        }
        Err(mq_client_err!(format!(
            "Find Filter Server Failed, Broker Addr: {},topic:{}",
            broker_addr, topic
        )))
    }

    pub async fn pop_async<PC>(
        &mut self,
        mq: &MessageQueue,
        invisible_time: u64,
        max_nums: u32,
        consumer_group: CheetahString,
        timeout: u64,
        pop_callback: PC,
        poll: bool,
        init_mode: i32,
        order: bool,
        expression_type: CheetahString,
        expression: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PC: PopCallback + 'static,
    {
        let mut find_broker_result = self
            .client_instance
            .find_broker_address_in_subscribe(mq.get_broker_name(), mix_all::MASTER_ID, true)
            .await;
        if find_broker_result.is_none() {
            self.client_instance
                .update_topic_route_info_from_name_server_topic(mq.get_topic_cs())
                .await;
            find_broker_result = self
                .client_instance
                .find_broker_address_in_subscribe(mq.get_broker_name(), mix_all::MASTER_ID, true)
                .await;
        }
        if let Some(find_broker_result) = find_broker_result {
            let mut request_header = PopMessageRequestHeader {
                consumer_group,
                topic: mq.get_topic_cs().clone(),
                queue_id: mq.get_queue_id(),
                max_msg_nums: max_nums,
                invisible_time,
                init_mode,
                exp_type: Some(expression_type),
                exp: Some(expression),
                order: Some(order),
                topic_request_header: Some(TopicRequestHeader {
                    lo: None,
                    rpc: Some(RpcRequestHeader {
                        namespace: None,
                        namespaced: None,
                        broker_name: Some(CheetahString::from_string(mq.get_broker_name().to_string())),
                        oneway: None,
                    }),
                }),
                ..Default::default()
            };
            if poll {
                request_header.poll_time = timeout;
                request_header.born_time = get_current_millis();
            }
            self.client_instance
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .pop_message_async(
                    mq.get_broker_name(),
                    &find_broker_result.broker_addr,
                    request_header,
                    timeout,
                    pop_callback,
                )
                .await
        } else {
            Err(mq_client_err!(format!(
                "The broker[{}] not exist",
                mq.get_broker_name(),
            )))
        }
    }
}

pub fn random_num() -> i32 {
    let mut rng = rand::rng();
    let mut value = rng.random::<i32>();
    if value < 0 {
        value = value.abs();
        if value < 0 {
            value = 0;
        }
    }
    value
}
