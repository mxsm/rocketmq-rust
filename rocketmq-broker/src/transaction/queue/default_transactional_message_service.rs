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
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_client_rust::consumer::pull_result::PullResult;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::transaction::operation_result::OperationResult;
use crate::transaction::queue::get_result::GetResult;
use crate::transaction::queue::message_queue_op_context::MessageQueueOpContext;
use crate::transaction::queue::transactional_message_bridge::TransactionalMessageBridge;
use crate::transaction::queue::transactional_message_util::TransactionalMessageUtil;
use crate::transaction::queue::transactional_op_batch_service::TransactionalOpBatchService;
use crate::transaction::transaction_metrics::TransactionMetrics;
use crate::transaction::transactional_message_check_listener::TransactionalMessageCheckListener;
use crate::transaction::transactional_message_service::TransactionalMessageService;

const PULL_MSG_RETRY_NUMBER: i32 = 1;
const MAX_PROCESS_TIME_LIMIT: i32 = 60000;
const MAX_RETRY_TIMES_FOR_ESCAPE: i32 = 10;
const MAX_RETRY_COUNT_WHEN_HALF_NULL: i32 = 1;
const OP_MSG_PULL_NUMS: i32 = 32;
const SLEEP_WHILE_NO_OP: i32 = 1000;

pub struct DefaultTransactionalMessageService<MS: MessageStore> {
    transactional_message_bridge: TransactionalMessageBridge<MS>,
    delete_context: Arc<Mutex<HashMap<i32, MessageQueueOpContext>>>,
    transactional_op_batch_service: Option<TransactionalOpBatchService<MS>>,
    op_queue_map: Arc<RwLock<HashMap<MessageQueue, MessageQueue>>>,
    transaction_metrics: TransactionMetrics,
}

impl<MS> DefaultTransactionalMessageService<MS>
where
    MS: MessageStore,
{
    pub fn new(transactional_message_bridge: TransactionalMessageBridge<MS>) -> Self {
        Self {
            transactional_message_bridge,
            delete_context: Arc::new(Mutex::new(HashMap::new())),
            transactional_op_batch_service: None,
            op_queue_map: Arc::new(Default::default()),
            transaction_metrics: TransactionMetrics,
        }
    }

    pub async fn set_transactional_op_batch_service_start(
        &mut self,
        weak_this: WeakArcMut<DefaultTransactionalMessageService<MS>>,
    ) {
        let transactional_op_batch_service = TransactionalOpBatchService::new(
            self.transactional_message_bridge
                .broker_runtime_inner
                .broker_config_arc(),
            weak_this,
        );
        transactional_op_batch_service.start().await;
        self.transactional_op_batch_service = Some(transactional_op_batch_service);
    }

    fn get_half_message_by_offset(&self, offset: i64) -> OperationResult {
        let message_ext = self.transactional_message_bridge.look_message_by_offset(offset);

        if let Some(message_ext) = message_ext {
            OperationResult {
                prepare_message: Some(message_ext),
                response_remark: None,
                response_code: ResponseCode::Success,
            }
        } else {
            OperationResult {
                prepare_message: None,
                response_remark: Some("Find prepared transaction message failed".to_owned()),
                response_code: ResponseCode::SystemError,
            }
        }
    }

    pub async fn batch_send_op_message(&self) -> u64 {
        let start_time = get_current_millis();
        let broker_config = self.transactional_message_bridge.broker_runtime_inner.broker_config();
        let interval = broker_config.transaction_op_batch_interval;
        let max_size = broker_config.transaction_op_msg_max_size as usize;
        let mut over_size = false;
        let mut first_timestamp = start_time;
        let mut send_map = HashMap::<i32, Message>::new();
        let delete_context_mutex_guard = self.delete_context.lock().await;
        for (queue_id, mq_context) in delete_context_mutex_guard.iter() {
            if mq_context.get_total_size().await == 0
                || mq_context.is_empty().await
                || (mq_context.get_total_size().await < max_size as u32
                    && (start_time as i64 - mq_context.get_last_write_timestamp().await as i64) < interval as i64)
            {
                continue;
            }
            let op_message = self.get_op_message(*queue_id, None).await;
            if op_message.is_none() {
                continue;
            }
            send_map.insert(*queue_id, op_message.unwrap());
            first_timestamp = first_timestamp.min(mq_context.get_last_write_timestamp().await);
            if mq_context.get_total_size().await >= max_size as u32 {
                over_size = true;
            }
        }
        for (op_queue_id, op_message) in send_map {
            if !self
                .transactional_message_bridge
                .write_op(op_queue_id, op_message)
                .await
            {
                error!("Transaction batch op message write failed.");
            }
        }
        //wait for next batch remove
        let wakeup_timestamp = first_timestamp + interval;
        if !over_size && wakeup_timestamp > start_time {
            return wakeup_timestamp;
        }
        0
    }

    pub async fn get_op_message(&self, queue_id: i32, more_data: Option<String>) -> Option<Message> {
        let topic = TransactionalMessageUtil::build_op_topic();
        let mut delete_context = self.delete_context.lock().await;
        let mq_context = delete_context.get_mut(&queue_id)?;

        let more_data_length = if let Some(ref data) = more_data { data.len() } else { 0 };
        let mut length = more_data_length;
        let max_size = self
            .transactional_message_bridge
            .broker_runtime_inner
            .broker_config()
            .transaction_op_msg_max_size as usize;
        if length < max_size {
            let sz = mq_context.get_total_size().await as usize;
            if sz > max_size || length + sz > max_size {
                length = max_size + 100;
            } else {
                length += sz;
            }
        }

        let mut sb = String::with_capacity(length);

        if let Some(data) = more_data {
            sb.push_str(&data);
        }

        while !mq_context.is_empty().await {
            if sb.len() >= max_size {
                break;
            }
            {
                if let Ok(data) = mq_context.pull().await {
                    sb.push_str(&data);
                }
            }
        }

        if sb.is_empty() {
            return None;
        }

        Some(Message::with_tags(
            topic,
            TransactionalMessageUtil::REMOVE_TAG,
            sb.as_bytes(),
        ))
    }

    pub async fn shutdown(&mut self) {
        self.close().await
    }

    /// Internal check implementation
    async fn check_internal<Listener: TransactionalMessageCheckListener + Clone>(
        &mut self,
        transaction_timeout: u64,
        transaction_check_max: i32,
        listener: Listener,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC);

        //TopicValidator::RMQ_SYS_TRANS_HALF_TOPIC only one read and write queue
        let msg_queues = self.transactional_message_bridge.fetch_message_queues(&topic).await;

        if msg_queues.is_empty() {
            warn!("The queue of topic is empty: {}", topic);
            return Ok(());
        }

        debug!("Check topic={}, queues={:?}", topic, msg_queues);

        for message_queue in msg_queues {
            let start_time = get_current_millis() as i64;
            let op_queue = self.get_op_queue(&message_queue).await;

            let half_offset = self.transactional_message_bridge.fetch_consume_offset(&message_queue);
            let op_offset = self.transactional_message_bridge.fetch_consume_offset(&op_queue);

            info!(
                "Before check, the queue={:?} msgOffset={} opOffset={}",
                message_queue, half_offset, op_offset
            );

            if half_offset < 0 || op_offset < 0 {
                error!(
                    "MessageQueue: {:?} illegal offset read: {}, op offset: {}, skip this queue",
                    message_queue, half_offset, op_offset
                );
                continue;
            }

            let mut done_op_offset = Vec::new();
            //key: half message queue offset, value: op message queue offset
            let mut remove_map = HashMap::new();
            // key: op message queue offset, value: half message queue offsets
            let mut op_msg_map: HashMap<i64, HashSet<i64>> = HashMap::new();

            let pull_result = self
                .fill_op_remove_map(
                    &mut remove_map,
                    &op_queue,
                    op_offset,
                    half_offset,
                    &mut op_msg_map,
                    &mut done_op_offset,
                )
                .await?;

            if pull_result.is_none() {
                error!(
                    "The queue={:?} check msgOffset={} with opOffset={} failed, pullResult is null",
                    message_queue, half_offset, op_offset
                );
                continue;
            }

            // Process messages in the queue
            self.process_message_queue(
                &message_queue,
                &op_queue,
                half_offset,
                op_offset,
                start_time,
                transaction_timeout,
                transaction_check_max,
                &mut remove_map,
                &mut op_msg_map,
                &mut done_op_offset,
                pull_result,
                listener.clone(),
            )
            .await?;
        }

        Ok(())
    }

    /// Process messages in a specific queue
    async fn process_message_queue<Listener: TransactionalMessageCheckListener>(
        &mut self,
        message_queue: &MessageQueue,
        op_queue: &MessageQueue,
        half_offset: i64,
        op_offset: i64,
        start_time: i64,
        transaction_timeout: u64,
        transaction_check_max: i32,
        remove_map: &mut HashMap<i64, i64>,
        op_msg_map: &mut HashMap<i64, HashSet<i64>>,
        done_op_offset: &mut Vec<i64>,
        mut pull_result: Option<PullResult>,
        mut listener: Listener,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut get_message_null_count = 1;
        let mut new_offset = half_offset;
        let mut consume_half_offset = half_offset;
        let mut next_op_offset = pull_result.as_ref().map_or(0, |pr| pr.next_begin_offset());
        let mut put_in_queue_count = 0;
        let mut escape_fail_cnt = 0;
        let listener = &mut listener;

        loop {
            let current_time = get_current_millis() as i64;
            if current_time - start_time > MAX_PROCESS_TIME_LIMIT as i64 {
                info!(
                    "Queue={:?} process time reach max={}",
                    message_queue, MAX_PROCESS_TIME_LIMIT
                );
                break;
            }

            if let Some(removed_op_offset) = remove_map.remove(&consume_half_offset) {
                debug!("Half offset {} has been committed/rolled back", consume_half_offset);

                if let Some(op_msg_set) = op_msg_map.get_mut(&removed_op_offset) {
                    op_msg_set.remove(&consume_half_offset);
                    if op_msg_set.is_empty() {
                        op_msg_map.remove(&removed_op_offset);
                        done_op_offset.push(removed_op_offset);
                    }
                }
            } else {
                let get_result = self.get_half_msg(message_queue, consume_half_offset).await?;
                let mut msg_ext = match get_result.msg {
                    Some(msg) => msg,
                    None => {
                        if get_message_null_count > MAX_RETRY_COUNT_WHEN_HALF_NULL {
                            break;
                        }
                        get_message_null_count += 1;

                        if let Some(pr) = get_result.pull_result {
                            if *pr.pull_status() == PullStatus::NoNewMsg {
                                debug!(
                                    "No new msg, the miss offset={} in={:?}, continue check={}, pull result={}",
                                    consume_half_offset, message_queue, get_message_null_count, pr
                                );
                                break;
                            } else {
                                info!(
                                    "Illegal offset, the miss offset={} in={:?}, continue check={}, pull result={}",
                                    consume_half_offset, message_queue, get_message_null_count, pr
                                );
                                consume_half_offset = pr.next_begin_offset() as i64;
                                new_offset = consume_half_offset;
                                continue;
                            }
                        }
                        continue;
                    }
                };
                // Handle slave acting master scenario
                if self.should_escape_message() {
                    let msg_inner = TransactionalMessageBridge::<MS>::renew_half_message_inner(&msg_ext);
                    let is_success = self.transactional_message_bridge.escape_message(msg_inner).await;

                    if is_success {
                        escape_fail_cnt = 0;
                        new_offset = consume_half_offset + 1;
                        consume_half_offset += 1;
                    } else {
                        warn!(
                            "Escaping transactional message failed {} times! msgId(offsetId)={}, \
                             UNIQ_KEY(transactionId)={}",
                            escape_fail_cnt + 1,
                            msg_ext.msg_id(),
                            msg_ext
                                .get_user_property(&CheetahString::from_static_str(
                                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                                ))
                                .unwrap_or_default()
                        );

                        if escape_fail_cnt < MAX_RETRY_TIMES_FOR_ESCAPE {
                            escape_fail_cnt += 1;
                            let sleep_time = 100 * (2_i32.pow(escape_fail_cnt as u32));
                            tokio::time::sleep(Duration::from_millis(sleep_time as u64)).await;
                        } else {
                            escape_fail_cnt = 0;
                            new_offset = consume_half_offset + 1;
                            consume_half_offset += 1;
                        }
                    }
                    continue;
                }

                // Check if message should be discarded or skipped
                if self.need_discard(&mut msg_ext, transaction_check_max) || self.need_skip(&msg_ext) {
                    listener.resolve_discard_msg(msg_ext).await;
                    new_offset = consume_half_offset + 1;
                    consume_half_offset += 1;
                    continue;
                }

                // Check if message was just stored
                if msg_ext.store_timestamp() >= start_time {
                    debug!(
                        "Fresh stored. the miss offset={}, check it later, store={}",
                        consume_half_offset,
                        msg_ext.store_timestamp()
                    );
                    break;
                }

                // Handle immunity time logic
                let current_time = get_current_millis() as i64;
                let value_of_current_minus_born = current_time - msg_ext.born_timestamp();
                let mut check_immunity_time = transaction_timeout as i64;

                if let Some(immunity_time_str) = msg_ext.get_user_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
                )) {
                    check_immunity_time = self.get_immunity_time(&immunity_time_str, transaction_timeout as i64);
                    if value_of_current_minus_born < check_immunity_time
                        && self
                            .check_prepare_queue_offset(remove_map, done_op_offset, &msg_ext, &immunity_time_str)
                            .await?
                    {
                        new_offset = consume_half_offset + 1;
                        consume_half_offset += 1;
                        continue;
                    }
                } else if 0 <= value_of_current_minus_born && value_of_current_minus_born < check_immunity_time {
                    /* debug!(
                        "New arrived, the miss offset={}, check it later checkImmunity={}, born={}",
                        i,
                        check_immunity_time,
                        msg_ext.get_born_timestamp()
                    );*/
                    break;
                }

                // Determine if check is needed
                let op_msg = if let Some(ref inner) = pull_result {
                    inner.msg_found_list()
                } else {
                    &None
                };
                let is_need_check = self.is_check_needed(
                    op_msg,
                    value_of_current_minus_born,
                    check_immunity_time,
                    start_time,
                    transaction_timeout as i64,
                );
                if is_need_check {
                    if !self.put_back_half_msg_queue(&mut msg_ext, consume_half_offset).await {
                        continue;
                    }
                    put_in_queue_count += 1;

                    info!(
                        "Check transaction. real_topic={}, uniqKey={}, offset={}, commitLogOffset={}",
                        msg_ext
                            .get_user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                            .unwrap_or_default(),
                        msg_ext
                            .get_user_property(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                            ))
                            .unwrap_or_default(),
                        msg_ext.queue_offset(),
                        msg_ext.commit_log_offset()
                    );

                    listener.resolve_half_msg(msg_ext).await.unwrap_or_else(|err| {
                        error!("Failed to resolve half message, error: {}", err);
                    })
                } else {
                    // Pull more operation messages
                    next_op_offset = if let Some(pr) = pull_result.as_ref() {
                        pr.next_begin_offset()
                    } else {
                        next_op_offset
                    };

                    pull_result = self
                        .fill_op_remove_map(
                            remove_map,
                            op_queue,
                            next_op_offset as i64,
                            half_offset,
                            op_msg_map,
                            done_op_offset,
                        )
                        .await?;

                    if pull_result.is_none()
                        || matches!(
                            pull_result.as_ref().unwrap().pull_status(),
                            PullStatus::NoNewMsg | PullStatus::OffsetIllegal | PullStatus::NoMatchedMsg
                        )
                    {
                        tokio::time::sleep(Duration::from_millis(SLEEP_WHILE_NO_OP as u64)).await;
                    } else {
                        info!(
                            "The miss message offset:{}, pullOffsetOfOp:{}, miniOffset:{} get more opMsg.",
                            consume_half_offset, next_op_offset, half_offset
                        );
                    }
                    continue;
                }
            }

            new_offset = consume_half_offset + 1;
            consume_half_offset += 1;
        }

        // Update offsets
        if new_offset != half_offset {
            self.transactional_message_bridge
                .update_consume_offset(message_queue, new_offset);
        }

        let new_op_offset = self.calculate_op_offset(done_op_offset, op_offset);
        if new_op_offset != op_offset {
            self.transactional_message_bridge
                .update_consume_offset(op_queue, new_op_offset);
        }

        // Log final statistics
        let get_result = self.get_half_msg(message_queue, new_offset).await?;
        let pull_result = self.pull_op_msg(op_queue, new_op_offset, 1).await;

        let max_msg_offset = if let Some(ref pr) = get_result.pull_result {
            pr.max_offset()
        } else {
            new_offset as u64
        };
        let max_op_offset = if let Some(ref pr) = pull_result {
            pr.max_offset()
        } else {
            new_op_offset as u64
        };
        let msg_time = get_result
            .get_msg()
            .map(|msg| msg.store_timestamp())
            .unwrap_or_else(|| get_current_millis() as i64);

        info!(
            "After check, {:?} opOffset={} opOffsetDiff={} msgOffset={} msgOffsetDiff={} msgTime={} \
             msgTimeDelayInMs={} putInQueueCount={}",
            message_queue,
            new_op_offset,
            max_op_offset as i64 - new_op_offset,
            new_offset,
            max_msg_offset - new_offset as u64,
            msg_time,
            get_current_millis() as i64 - msg_time,
            put_in_queue_count
        );

        Ok(())
    }

    /// Put message back to half queue
    async fn put_back_half_msg_queue(&self, msg_ext: &mut MessageExt, offset: i64) -> bool {
        if let Some(put_message_result) = self.put_back_to_half_queue_return_result(msg_ext).await {
            if put_message_result.put_message_status() == PutMessageStatus::PutOk {
                if let Some(append_result) = put_message_result.append_message_result() {
                    msg_ext.set_queue_offset(append_result.logics_offset);
                    msg_ext.set_commit_log_offset(append_result.wrote_offset);
                    msg_ext.set_msg_id(append_result.msg_id.clone().unwrap_or("".to_string()).into());

                    debug!(
                        "Send check message, the offset={} restored in queueOffset={} commitLogOffset={} newMsgId={} \
                         realMsgId={} topic={}",
                        offset,
                        msg_ext.queue_offset(),
                        msg_ext.commit_log_offset(),
                        msg_ext.msg_id(),
                        msg_ext
                            .get_user_property(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                            ))
                            .unwrap_or_default(),
                        msg_ext.get_topic()
                    );
                    return true;
                }
            }
        }

        error!(
            "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, msgId: {}",
            msg_ext.get_topic(),
            msg_ext.queue_id(),
            msg_ext.msg_id()
        );
        false
    }

    /// Put message back to half queue and return result
    async fn put_back_to_half_queue_return_result(&self, message_ext: &MessageExt) -> Option<PutMessageResult> {
        let msg_inner = TransactionalMessageBridge::<MS>::renew_half_message_inner(message_ext);
        Some(
            self.transactional_message_bridge
                .put_message_return_result(msg_inner)
                .await,
        )
    }

    /// Put immunity message back to half queue
    async fn put_immunity_msg_back_to_half_queue(&mut self, message_ext: &MessageExt) -> bool {
        let msg_inner = TransactionalMessageBridge::<MS>::renew_immunity_half_message_inner(message_ext);
        self.transactional_message_bridge.put_message(msg_inner).await
    }

    /// Determine if message check is needed
    fn is_check_needed(
        &self,
        op_msg: &Option<Vec<ArcMut<MessageExt>>>,
        value_of_current_minus_born: i64,
        check_immunity_time: i64,
        start_time: i64,
        transaction_timeout: i64,
    ) -> bool {
        if let Some(ref messages) = op_msg {
            if let Some(last_msg) = messages.last() {
                return last_msg.born_timestamp() - start_time > transaction_timeout;
            }
        }

        op_msg.is_none() && value_of_current_minus_born > check_immunity_time || value_of_current_minus_born <= -1
    }

    /// Check prepare queue offset
    async fn check_prepare_queue_offset(
        &mut self,
        remove_map: &mut HashMap<i64, i64>,
        done_op_offset: &mut Vec<i64>,
        msg_ext: &MessageExt,
        check_immunity_time_str: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let prepare_queue_offset_str = msg_ext.get_user_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
        ));

        match prepare_queue_offset_str {
            None => Ok(self.put_immunity_msg_back_to_half_queue(msg_ext).await),
            Some(offset_str) => match offset_str.parse::<i64>() {
                Ok(-1) => Ok(false),
                Ok(prepare_queue_offset) => {
                    if let Some(tmp_op_offset) = remove_map.remove(&prepare_queue_offset) {
                        done_op_offset.push(tmp_op_offset);
                        info!(
                            "removeMap contain prepareQueueOffset. real_topic={}, uniqKey={}, immunityTime={}, \
                             offset={}",
                            msg_ext
                                .get_user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC))
                                .unwrap_or_default(),
                            msg_ext
                                .get_user_property(&CheetahString::from_static_str(
                                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
                                ))
                                .unwrap_or_default(),
                            check_immunity_time_str,
                            msg_ext.queue_offset()
                        );
                        Ok(true)
                    } else {
                        Ok(self.put_immunity_msg_back_to_half_queue(msg_ext).await)
                    }
                }
                Err(_) => Ok(false),
            },
        }
    }

    /// Get immunity time from string
    fn get_immunity_time(&self, check_immunity_time_str: &str, transaction_timeout: i64) -> i64 {
        match check_immunity_time_str.parse::<i64>() {
            Ok(-1) => transaction_timeout,
            Ok(time) => time * 1000,
            Err(_) => transaction_timeout,
        }
    }

    /// Calculate operation offset
    fn calculate_op_offset(&self, done_offset: &mut [i64], old_offset: i64) -> i64 {
        done_offset.sort();
        let mut new_offset = old_offset;

        for &offset in done_offset.iter() {
            if offset == new_offset {
                new_offset += 1;
            } else {
                break;
            }
        }

        new_offset
    }

    /// Check if message needs to be skipped
    fn need_skip(&self, msg_ext: &MessageExt) -> bool {
        let value_of_current_minus_born = get_current_millis() as i64 - msg_ext.born_timestamp();

        let file_reserved_time = self
            .transactional_message_bridge
            .broker_runtime_inner
            .message_store_config()
            .file_reserved_time as i64;

        if value_of_current_minus_born > file_reserved_time * 3600 * 1000 {
            info!(
                "Half message exceed file reserved time, so skip it. messageId {}, bornTime {}",
                msg_ext.msg_id(),
                msg_ext.born_timestamp()
            );
            return true;
        }
        false
    }

    /// Check if message needs to be discarded
    fn need_discard(&self, msg_ext: &mut MessageExt, transaction_check_max: i32) -> bool {
        let check_times_str = msg_ext.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_CHECK_TIMES,
        ));
        let mut check_time = 1;

        if let Some(times) = check_times_str {
            if let Ok(parsed_times) = times.parse::<i32>() {
                check_time = parsed_times;
                if check_time >= transaction_check_max {
                    return true;
                } else {
                    check_time += 1;
                }
            }
        }

        msg_ext.put_user_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_CHECK_TIMES),
            check_time.to_string().into(),
        );
        false
    }

    fn should_escape_message(&self) -> bool {
        self.transactional_message_bridge
            .broker_runtime_inner
            .broker_config()
            .enable_slave_acting_master
            && self
                .transactional_message_bridge
                .broker_runtime_inner
                .get_min_broker_id_in_group()
                == self
                    .transactional_message_bridge
                    .broker_runtime_inner
                    .broker_config()
                    .broker_identity
                    .broker_id
            && BrokerRole::Slave
                == self
                    .transactional_message_bridge
                    .broker_runtime_inner
                    .broker_config()
                    .broker_role
    }

    /// Get half message
    async fn get_half_msg(
        &self,
        message_queue: &MessageQueue,
        offset: i64,
    ) -> Result<GetResult, Box<dyn std::error::Error + Send + Sync>> {
        let mut get_result = GetResult::new();

        if let Some(result) = self.pull_half_msg(message_queue, offset, PULL_MSG_RETRY_NUMBER).await {
            if let Some(message_exts) = result.msg_found_list() {
                if !message_exts.is_empty() {
                    get_result.set_msg(Some(message_exts[0].as_ref().clone()));
                }
            }
            get_result.set_pull_result(Some(result));
        }

        Ok(get_result)
    }

    /// Pull half message
    async fn pull_half_msg(&self, mq: &MessageQueue, offset: i64, nums: i32) -> Option<PullResult> {
        self.transactional_message_bridge
            .get_half_message(mq.get_queue_id(), offset, nums)
            .await
    }

    /// Get operation queue for message queue
    async fn get_op_queue(&self, message_queue: &MessageQueue) -> MessageQueue {
        let mut op_queue_map = self.op_queue_map.write().await;

        op_queue_map
            .entry(message_queue.clone())
            .or_insert_with(|| {
                MessageQueue::from_parts(
                    CheetahString::from_static_str(TransactionalMessageUtil::build_op_topic()),
                    message_queue.get_broker_name(),
                    message_queue.get_queue_id(),
                )
            })
            .clone()
    }

    /// Read op message, parse op message, and fill removeMap
    ///
    /// # Arguments
    ///
    /// * `remove_map` -Half message to be removed, key:halfOffset, value: opOffset.
    /// * `op_queue` - Op message queue.
    /// * `pull_offset_of_op` -The beginning offset of op message queue.
    /// * `mini_offset` - The current consume offset of half message queue.
    /// * `op_msg_map` -Half message offset in op message
    /// * `done_op_offset` - Stored op messages that have been processed.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<PullResult>`:
    /// - `Some(PullResult)` if the operation messages were successfully pulled.
    /// - `None` if no operation messages were found or an error occurred.
    ///
    /// # Errors
    ///
    /// Returns an error if pulling operation messages fails or if there is an issue processing the
    /// messages.
    async fn fill_op_remove_map(
        &self,
        remove_map: &mut HashMap<i64, i64>,
        op_queue: &MessageQueue,
        pull_offset_of_op: i64,
        mini_offset: i64,
        op_msg_map: &mut HashMap<i64, HashSet<i64>>,
        done_op_offset: &mut Vec<i64>,
    ) -> Result<Option<PullResult>, Box<dyn std::error::Error + Send + Sync>> {
        let pull_result = self.pull_op_msg(op_queue, pull_offset_of_op, OP_MSG_PULL_NUMS).await;

        let Some(pull_result) = pull_result else {
            return Ok(None);
        };

        match pull_result.pull_status() {
            PullStatus::OffsetIllegal | PullStatus::NoMatchedMsg => {
                warn!(
                    "The miss op offset={} in queue={:?} is illegal, pullResult={}",
                    pull_offset_of_op, op_queue, pull_result
                );
                self.transactional_message_bridge
                    .update_consume_offset(op_queue, pull_result.next_begin_offset() as i64);
                return Ok(Some(pull_result));
            }
            PullStatus::NoNewMsg => {
                warn!(
                    "The miss op offset={} in queue={:?} is NO_NEW_MSG, pullResult={}",
                    pull_offset_of_op, op_queue, pull_result
                );
                return Ok(Some(pull_result));
            }
            _ => {}
        }

        let Some(op_msgs) = pull_result.msg_found_list() else {
            warn!(
                "The miss op offset={} in queue={:?} is empty, pullResult={}",
                pull_offset_of_op, op_queue, pull_result
            );
            return Ok(Some(pull_result));
        };

        for op_message_ext in op_msgs {
            let Some(body) = op_message_ext.get_body() else {
                error!(
                    "op message body is null. queueId={}, offset={}",
                    op_message_ext.queue_id(),
                    op_message_ext.queue_offset()
                );
                done_op_offset.push(op_message_ext.queue_offset());
                continue;
            };

            let mut set = HashSet::new();

            // queue_offset_body end with ","
            let queue_offset_body = String::from_utf8_lossy(body);

            debug!(
                "Topic: {} tags: {:?}, OpOffset: {}, HalfOffset: {}",
                op_message_ext.get_topic(),
                op_message_ext.get_tags(),
                op_message_ext.queue_offset(),
                queue_offset_body
            );

            if op_message_ext.get_tags() == Some(CheetahString::from_static_str(TransactionalMessageUtil::REMOVE_TAG)) {
                let offset_array: Vec<&str> = queue_offset_body
                    .split(TransactionalMessageUtil::OFFSET_SEPARATOR)
                    .collect();

                for offset_str in offset_array {
                    if offset_str.is_empty() {
                        continue;
                    }
                    if let Ok(offset_value) = offset_str.parse::<i64>() {
                        if offset_value < mini_offset {
                            continue;
                        }
                        remove_map.insert(offset_value, op_message_ext.queue_offset());
                        set.insert(offset_value);
                    }
                }
            } else {
                error!("Found a illegal tag in opMessageExt={:?}", op_message_ext);
            }

            if !set.is_empty() {
                op_msg_map.insert(op_message_ext.queue_offset(), set);
            } else {
                done_op_offset.push(op_message_ext.queue_offset());
            }
        }

        debug!("Remove map: {:?}", remove_map);
        debug!("Done op list: {:?}", done_op_offset);
        debug!("opMsg map: {:?}", op_msg_map);

        Ok(Some(pull_result))
    }

    /// Pull operation message
    async fn pull_op_msg(&self, mq: &MessageQueue, offset: i64, nums: i32) -> Option<PullResult> {
        self.transactional_message_bridge
            .get_op_message(mq.get_queue_id(), offset, nums)
            .await
    }
}

impl<MS> TransactionalMessageService for DefaultTransactionalMessageService<MS>
where
    MS: MessageStore + Send + Sync + 'static,
{
    async fn prepare_message(&mut self, message_inner: MessageExtBrokerInner) -> PutMessageResult {
        self.transactional_message_bridge.put_half_message(message_inner).await
    }

    async fn async_prepare_message(&mut self, message_inner: MessageExtBrokerInner) -> PutMessageResult {
        self.transactional_message_bridge.put_half_message(message_inner).await
    }

    async fn delete_prepare_message(&mut self, message_ext: &MessageExt) -> bool {
        let queue_id = message_ext.queue_id;
        let mut delete_context = self.delete_context.lock().await;
        let mq_context = delete_context
            .entry(queue_id)
            .or_insert(MessageQueueOpContext::new(get_current_millis(), 20000));
        let data = format!(
            "{}{}",
            message_ext.queue_offset,
            TransactionalMessageUtil::OFFSET_SEPARATOR
        );
        let len = data.len();
        let res = mq_context.offer(data.clone(), Duration::from_millis(100)).await;
        if res.is_ok() {
            let total_size = mq_context.total_size_add_and_get(len as u32).await;
            if total_size
                > self
                    .transactional_message_bridge
                    .broker_runtime_inner
                    .broker_config()
                    .transaction_op_msg_max_size as u32
            {
                if let Some(batch_service) = &self.transactional_op_batch_service {
                    batch_service.wakeup();
                } else {
                    error!("Transactional op batch service not initialized");
                }
            }
            return true;
        } else if let Some(batch_service) = &self.transactional_op_batch_service {
            batch_service.wakeup();
        } else {
            error!("Transactional op batch service not initialized");
        }

        let msg = self.get_op_message(queue_id, Some(data)).await;
        if self
            .transactional_message_bridge
            .write_op(queue_id, msg.expect("message is none"))
            .await
        {
            warn!("Force add remove op data. queueId={}", queue_id);
            true
        } else {
            error!(
                "Transaction op message write failed. messageId is {}, queueId is {}",
                message_ext.msg_id, message_ext.queue_id
            );
            false
        }
    }

    #[inline]
    fn commit_message(&mut self, request_header: &EndTransactionRequestHeader) -> OperationResult {
        self.get_half_message_by_offset(request_header.commit_log_offset as i64)
    }

    #[inline]
    fn rollback_message(&mut self, request_header: &EndTransactionRequestHeader) -> OperationResult {
        self.get_half_message_by_offset(request_header.commit_log_offset as i64)
    }

    async fn check<Listener: TransactionalMessageCheckListener + Clone>(
        &mut self,
        transaction_timeout: u64,
        transaction_check_max: i32,
        listener: Listener,
    ) {
        match self
            .check_internal(transaction_timeout, transaction_check_max, listener)
            .await
        {
            Ok(_) => {}
            Err(e) => error!("Check error: {}", e),
        }
    }

    fn open(&self) -> bool {
        true
    }

    async fn close(&self) {
        if let Some(batch_service) = &self.transactional_op_batch_service {
            batch_service.shutdown().await
        }
    }

    fn get_transaction_metrics(&self) -> &TransactionMetrics {
        unimplemented!("get_transaction_metrics")
    }

    fn set_transaction_metrics(&mut self, _transaction_metrics: TransactionMetrics) {
        unimplemented!("set_transaction_metrics")
    }
}
