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

use crate::producer::send_result::SendResult;

/// Callback function type for asynchronous message sending operations.
///
/// This type represents a callback that will be invoked when an asynchronous send operation
/// completes, either successfully or with an error. Following the original Java RocketMQ API
/// design, the callback receives two `Option` parameters to indicate success or failure.
///
/// # Parameters
///
/// * First parameter: `Option<&SendResult>` - Contains the send result on success, `None` on
///   failure
/// * Second parameter: `Option<&dyn std::error::Error>` - Contains the error on failure, `None` on
///   success
///
/// # Callback Contract
///
/// Implementations must handle two mutually exclusive states:
/// - **Success**: `(Some(result), None)` - The message was sent successfully
/// - **Failure**: `(None, Some(error))` - The send operation failed
///
/// The state `(Some(_), Some(_))` and `(None, None)` should never occur in normal operation.
///
/// # Thread Safety
///
/// The callback must be `Send + Sync` as it may be invoked from different threads in the
/// async runtime.
///
/// # Examples
///
/// Basic usage with error handling:
///
/// ```rust,ignore
/// use rocketmq_client::producer::SendMessageCallback;
/// use std::sync::Arc;
///
/// let callback: SendMessageCallback = Arc::new(|result, error| {
///     match (result, error) {
///         (Some(send_result), None) => {
///             println!("Message sent successfully: {:?}", send_result);
///         }
///         (None, Some(err)) => {
///             eprintln!("Failed to send message: {}", err);
///         }
///         _ => {
///             eprintln!("Invalid callback state");
///         }
///     }
/// });
///
/// producer.send_with_callback(message, callback).await?;
/// ```
///
/// With state capture:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Ordering};
///
/// let success_count = Arc::new(AtomicUsize::new(0));
/// let failure_count = Arc::new(AtomicUsize::new(0));
///
/// let success_cnt = Arc::clone(&success_count);
/// let failure_cnt = Arc::clone(&failure_count);
///
/// let callback = Arc::new(move |result, error| {
///     match (result, error) {
///         (Some(_), None) => {
///             success_cnt.fetch_add(1, Ordering::Relaxed);
///         }
///         (None, Some(_)) => {
///             failure_cnt.fetch_add(1, Ordering::Relaxed);
///         }
///         _ => {}
///     }
/// });
/// ```
pub type SendMessageCallback = Arc<dyn Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync>;

/// Alternative Result-based callback type for more idiomatic Rust code.
///
/// This callback type uses `Result<SendResult, E>` instead of separate `Option` parameters,
/// providing a more Rust-idiomatic API while maintaining the same functionality.
///
/// # Type Parameters
///
/// * `E` - The error type, typically `Box<dyn std::error::Error>`
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_client::producer::SendResultCallback;
/// use std::sync::Arc;
///
/// let callback: SendResultCallback<Box<dyn std::error::Error>> =
///     Arc::new(|result| match result {
///         Ok(send_result) => {
///             println!("Success: {:?}", send_result);
///         }
///         Err(error) => {
///             eprintln!("Error: {}", error);
///         }
///     });
/// ```
///
/// Note: This type is provided for future API evolution. The primary callback type
/// remains `SendMessageCallback` to maintain compatibility with the existing codebase.
pub type SendResultCallback<E> = Arc<dyn Fn(Result<&SendResult, E>) + Send + Sync>;

/// Trait-based callback interface for message sending operations.
///
/// This trait provides an object-oriented callback interface similar to Java's `SendCallback`.
/// It allows implementing custom callback logic as a type rather than a closure.
///
/// # Note
///
/// This trait is currently not actively used in the codebase. The preferred callback mechanism
/// is the `SendMessageCallback` function type, which is more flexible and idiomatic in Rust.
/// This trait is provided for API completeness and potential future extensions.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_client::producer::{SendCallback, SendResult};
///
/// struct LoggingSendCallback;
///
/// impl SendCallback for LoggingSendCallback {
///     fn on_success(&self, send_result: &SendResult) {
///         println!("Message sent: {:?}", send_result);
///     }
///
///     fn on_exception(&self, error: &dyn std::error::Error) {
///         eprintln!("Send failed: {}", error);
///     }
/// }
/// ```
pub trait SendCallback: Send + Sync + 'static {
    /// Called when the message is sent successfully.
    ///
    /// # Parameters
    ///
    /// * `send_result` - The result of the send operation containing message ID, queue information,
    ///   and other metadata
    fn on_success(&self, send_result: &SendResult);

    /// Called when the send operation fails.
    ///
    /// # Parameters
    ///
    /// * `error` - The error that caused the send operation to fail. Can be any error type
    ///   implementing the standard `Error` trait.
    fn on_exception(&self, error: &dyn std::error::Error);
}
