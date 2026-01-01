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

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_store::base::message_result::PutMessageResult;

use crate::transaction::operation_result::OperationResult;
use crate::transaction::transaction_metrics::TransactionMetrics;
use crate::transaction::transactional_message_check_listener::TransactionalMessageCheckListener;

/// Trait defining the local transactional message service.
/// This trait provides methods for preparing, committing, rolling back, and checking transactional
/// messages, as well as managing the state of the transactional message service.
#[trait_variant::make(TransactionalMessageService: Send)]
pub trait TransactionalMessageServiceLocal: Sync + 'static {
    /// Prepares a transactional message.
    ///
    /// # Arguments
    ///
    /// * `message_inner` - The inner message to be prepared.
    ///
    /// # Returns
    ///
    /// A `PutMessageResult` indicating the result of the message preparation.
    async fn prepare_message(&mut self, message_inner: MessageExtBrokerInner) -> PutMessageResult;

    /// Asynchronously prepares a transactional message.
    ///
    /// # Arguments
    ///
    /// * `message_inner` - The inner message to be prepared.
    ///
    /// # Returns
    ///
    /// A `PutMessageResult` indicating the result of the message preparation.
    async fn async_prepare_message(&mut self, message_inner: MessageExtBrokerInner) -> PutMessageResult;

    /// Deletes a prepared transactional message.
    ///
    /// # Arguments
    ///
    /// * `message_ext` - The external message to be deleted.
    ///
    /// # Returns
    ///
    /// A boolean indicating whether the message was successfully deleted.
    async fn delete_prepare_message(&mut self, message_ext: &MessageExt) -> bool;

    /// Commits a transactional message.
    ///
    /// # Arguments
    ///
    /// * `request_header` - The request header containing the transaction details.
    ///
    /// # Returns
    ///
    /// An `OperationResult` indicating the result of the commit operation.
    fn commit_message(&mut self, request_header: &EndTransactionRequestHeader) -> OperationResult;

    /// Rolls back a transactional message.
    ///
    /// # Arguments
    ///
    /// * `request_header` - The request header containing the transaction details.
    ///
    /// # Returns
    ///
    /// An `OperationResult` indicating the result of the rollback operation.
    fn rollback_message(&mut self, request_header: &EndTransactionRequestHeader) -> OperationResult;

    /// Checks the state of transactional messages.
    ///
    /// # Arguments
    ///
    /// * `transaction_timeout` - The timeout for the transaction.
    /// * `transaction_check_max` - The maximum number of transaction checks.
    async fn check<Listener: TransactionalMessageCheckListener + Clone>(
        &mut self,
        transaction_timeout: u64,
        transaction_check_max: i32,
        listener: Listener,
    );

    /// Opens the transactional message service.
    ///
    /// # Returns
    ///
    /// A boolean indicating whether the service was successfully opened.
    fn open(&self) -> bool;

    /// Closes the transactional message service.
    async fn close(&self);

    /// Gets the transaction metrics.
    ///
    /// # Returns
    ///
    /// A reference to the `TransactionMetrics`.
    fn get_transaction_metrics(&self) -> &TransactionMetrics;

    /// Sets the transaction metrics.
    ///
    /// # Arguments
    ///
    /// * `transaction_metrics` - The transaction metrics to be set.
    fn set_transaction_metrics(&mut self, transaction_metrics: TransactionMetrics);
}
