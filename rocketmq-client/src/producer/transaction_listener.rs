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

use std::any::Any;
use std::sync::Arc;

use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageTrait;

use crate::producer::local_transaction_state::LocalTransactionState;

/// Listener for handling transactional message operations.
///
/// This trait defines the callback interface for transactional message processing,
/// allowing applications to integrate local transaction execution with distributed
/// message transactions.
///
/// Implementations must be thread-safe as they may be invoked concurrently from
/// different threads or async tasks.
///
/// # Examples
///
/// ```ignore
/// use rocketmq_client_rust::producer::transaction_listener::TransactionListener;
/// use rocketmq_client_rust::producer::local_transaction_state::LocalTransactionState;
/// use rocketmq_common::common::message::MessageTrait;
/// use rocketmq_common::common::message::message_ext::MessageExt;
/// use std::any::Any;
///
/// struct OrderTransactionListener;
///
/// impl TransactionListener for OrderTransactionListener {
///     fn execute_local_transaction(
///         &self,
///         msg: &dyn MessageTrait,
///         arg: Option<&(dyn Any + Send + Sync)>,
///     ) -> LocalTransactionState {
///         // Execute local database transaction
///         if insert_order_into_db(msg).is_ok() {
///             LocalTransactionState::CommitMessage
///         } else {
///             LocalTransactionState::RollbackMessage
///         }
///     }
///
///     fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState {
///         // Check transaction status from database
///         if order_exists_in_db(msg) {
///             LocalTransactionState::CommitMessage
///         } else {
///             LocalTransactionState::Unknown
///         }
///     }
/// }
/// ```
pub trait TransactionListener: Send + Sync + 'static {
    /// Executes the local transaction when sending a transactional message.
    ///
    /// This method is invoked after the half message is successfully sent to the broker.
    /// The implementation should execute the local business transaction and return
    /// the transaction state to determine whether the message should be committed or rolled back.
    ///
    /// # Parameters
    ///
    /// * `msg` - The message being sent
    /// * `arg` - Optional user-defined argument passed from the send operation
    ///
    /// # Returns
    ///
    /// The local transaction state indicating whether to commit, rollback, or defer the decision:
    /// - `CommitMessage` - Commit the transaction and make the message visible to consumers
    /// - `RollbackMessage` - Roll back the transaction and discard the message
    /// - `Unknown` - Transaction state is uncertain, broker will check later
    fn execute_local_transaction(
        &self,
        msg: &dyn MessageTrait,
        arg: Option<&(dyn Any + Send + Sync)>,
    ) -> LocalTransactionState;

    /// Checks the status of a previously executed local transaction.
    ///
    /// This method is invoked by the broker when it needs to verify the state of a
    /// transaction whose initial state was `Unknown` or when the transaction check
    /// timeout is reached.
    ///
    /// The implementation should query the local transaction state (e.g., from a database)
    /// and return the current status.
    ///
    /// # Parameters
    ///
    /// * `msg` - The message whose transaction status needs to be checked
    ///
    /// # Returns
    ///
    /// The current state of the local transaction:
    /// - `CommitMessage` - The local transaction was committed successfully
    /// - `RollbackMessage` - The local transaction failed or was rolled back
    /// - `Unknown` - Transaction state cannot be determined at this time
    fn check_local_transaction(&self, msg: &MessageExt) -> LocalTransactionState;
}

/// Thread-safe shared reference to a [`TransactionListener`].
///
/// This type alias provides a convenient way to share transaction listeners across
/// threads using atomic reference counting. It uses `Arc<dyn TransactionListener>`
/// instead of `Arc<Box<dyn TransactionListener>>` to avoid double heap allocation
/// and minimize pointer indirection overhead.
///
/// # Performance
///
/// Using this alias instead of `Arc<Box<dyn TransactionListener>>` provides:
/// - One fewer heap allocation per instance
/// - One fewer pointer dereference per method call
/// - Reduced memory overhead (saves approximately 16 bytes per instance)
///
/// # Examples
///
/// ```ignore
/// use rocketmq_client_rust::producer::transaction_listener::{TransactionListener, ArcTransactionListener};
/// use std::sync::Arc;
///
/// struct MyListener;
/// impl TransactionListener for MyListener { /* ... */ }
///
/// // Create a shared reference
/// let listener: ArcTransactionListener = Arc::new(MyListener);
///
/// // Clone for sharing across threads
/// let listener_clone = listener.clone();
/// ```
pub type ArcTransactionListener = Arc<dyn TransactionListener>;
