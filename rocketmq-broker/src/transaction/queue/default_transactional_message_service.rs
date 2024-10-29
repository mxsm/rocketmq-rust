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

use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_store::base::message_result::PutMessageResult;
use rocketmq_store::log_file::MessageStore;
use tokio::sync::Mutex;

use crate::transaction::operation_result::OperationResult;
use crate::transaction::queue::message_queue_op_context::MessageQueueOpContext;
use crate::transaction::queue::transactional_message_bridge::TransactionalMessageBridge;
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

impl<MS> DefaultTransactionalMessageService<MS> {
    pub fn new(transactional_message_bridge: TransactionalMessageBridge<MS>) -> Self {
        Self {
            transactional_message_bridge,
            delete_context: Arc::new(Mutex::new(HashMap::new())),
            transactional_op_batch_service: TransactionalOpBatchService,
            transaction_metrics: TransactionMetrics,
        }
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

    fn delete_prepare_message(&mut self, _message_ext: MessageExtBrokerInner) -> bool {
        todo!()
    }

    fn commit_message(&mut self, _request_header: EndTransactionRequestHeader) -> OperationResult {
        todo!()
    }

    fn rollback_message(
        &mut self,
        _request_header: EndTransactionRequestHeader,
    ) -> OperationResult {
        todo!()
    }

    fn check(&self, _transaction_timeout: u64, _transaction_check_max: i32) {
        todo!()
    }

    fn open(&self) -> bool {
        todo!()
    }

    fn close(&self) {
        todo!()
    }

    fn get_transaction_metrics(&self) -> &TransactionMetrics {
        todo!()
    }

    fn set_transaction_metrics(&mut self, _transaction_metrics: TransactionMetrics) {
        todo!()
    }
}
