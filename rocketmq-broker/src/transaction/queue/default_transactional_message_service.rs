/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::log_file::MessageStore;
use tokio::sync::Mutex;
use tracing::error;
use tracing::warn;

use crate::transaction::operation_result::OperationResult;
use crate::transaction::queue::message_queue_op_context::MessageQueueOpContext;
use crate::transaction::queue::transactional_message_bridge::TransactionalMessageBridge;
use crate::transaction::queue::transactional_message_util::TransactionalMessageUtil;
use crate::transaction::queue::transactional_op_batch_service::TransactionalOpBatchService;
use crate::transaction::transaction_metrics::TransactionMetrics;
use crate::transaction::transactional_message_service::TransactionalMessageService;

const PULL_MSG_RETRY_NUMBER: i32 = 1;
const MAX_PROCESS_TIME_LIMIT: i32 = 60000;
const MAX_RETRY_TIMES_FOR_ESCAPE: i32 = 10;
const MAX_RETRY_COUNT_WHEN_HALF_NULL: i32 = 1;
const OP_MSG_PULL_NUMS: i32 = 32;
const SLEEP_WHILE_NO_OP: i32 = 1000;

pub struct DefaultTransactionalMessageService<MS> {
    transactional_message_bridge: TransactionalMessageBridge<MS>,
    delete_context: Arc<Mutex<HashMap<i32, MessageQueueOpContext>>>,
    transactional_op_batch_service: TransactionalOpBatchService,
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
            transactional_op_batch_service: TransactionalOpBatchService::new(),
            transaction_metrics: TransactionMetrics,
        }
    }

    fn get_half_message_by_offset(&self, offset: i64) -> OperationResult {
        let message_ext = self
            .transactional_message_bridge
            .look_message_by_offset(offset);

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

    pub async fn get_op_message(
        &self,
        queue_id: i32,
        more_data: Option<String>,
    ) -> Option<Message> {
        let topic = TransactionalMessageUtil::build_op_topic();
        let delete_context = self.delete_context.lock().await;
        let mq_context = delete_context.get(&queue_id)?;

        let more_data_length = if let Some(ref data) = more_data {
            data.len()
        } else {
            0
        };
        let mut length = more_data_length;
        let max_size = self
            .transactional_message_bridge
            .broker_runtime_inner
            .broker_config()
            .transaction_op_msg_max_size as usize;
        if length < max_size {
            let sz = mq_context.get_total_size() as usize;
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

        while !mq_context.context_queue().is_empty().await {
            if sb.len() >= max_size {
                break;
            }
            {
                if let Some(data) = mq_context.context_queue().try_poll().await {
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
}

impl<MS> TransactionalMessageService for DefaultTransactionalMessageService<MS>
where
    MS: MessageStore + Send + Sync + 'static,
{
    async fn prepare_message(&mut self, message_inner: MessageExtBrokerInner) -> PutMessageResult {
        self.transactional_message_bridge
            .put_half_message(message_inner)
            .await
    }

    async fn async_prepare_message(
        &mut self,
        message_inner: MessageExtBrokerInner,
    ) -> PutMessageResult {
        self.transactional_message_bridge
            .put_half_message(message_inner)
            .await
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
        let res = mq_context
            .context_queue()
            .offer(data.clone(), Duration::from_millis(100))
            .await;
        if res {
            let total_size = mq_context.total_size_add_and_get(len as i32);
            if total_size
                > self
                    .transactional_message_bridge
                    .broker_runtime_inner
                    .broker_config()
                    .transaction_op_msg_max_size
            {
                self.transactional_op_batch_service.wakeup();
            }
            return true;
        } else {
            self.transactional_op_batch_service.wakeup();
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
    fn rollback_message(
        &mut self,
        request_header: &EndTransactionRequestHeader,
    ) -> OperationResult {
        self.get_half_message_by_offset(request_header.commit_log_offset as i64)
    }

    fn check(&self, _transaction_timeout: u64, _transaction_check_max: i32) {
        todo!()
    }

    fn open(&self) -> bool {
        true
    }

    fn close(&self) {
        //nothing to do
    }

    fn get_transaction_metrics(&self) -> &TransactionMetrics {
        unimplemented!("get_transaction_metrics")
    }

    fn set_transaction_metrics(&mut self, _transaction_metrics: TransactionMetrics) {
        unimplemented!("set_transaction_metrics")
    }
}
