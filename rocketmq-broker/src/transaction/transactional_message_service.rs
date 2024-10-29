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
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_store::base::message_result::PutMessageResult;

use crate::transaction::operation_result::OperationResult;
use crate::transaction::transaction_metrics::TransactionMetrics;

#[trait_variant::make(TransactionalMessageService: Send)]
pub trait TransactionalMessageServiceLocal: Sync + 'static {
    async fn prepare_message(&mut self, message_inner: MessageExtBrokerInner) -> PutMessageResult;

    async fn async_prepare_message(
        &mut self,
        message_inner: MessageExtBrokerInner,
    ) -> PutMessageResult;

    fn delete_prepare_message(&mut self, message_ext: MessageExtBrokerInner) -> bool;

    fn commit_message(&mut self, request_header: EndTransactionRequestHeader) -> OperationResult;

    fn rollback_message(&mut self, request_header: EndTransactionRequestHeader) -> OperationResult;

    fn check(
        &self,
        transaction_timeout: u64,
        transaction_check_max: i32,
        // listener: AbstractTransactionalMessageCheckListener,
    );

    fn open(&self) -> bool;

    fn close(&self);

    fn get_transaction_metrics(&self) -> &TransactionMetrics;

    fn set_transaction_metrics(&mut self, transaction_metrics: TransactionMetrics);
}
