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

//! Callback abstractions for asynchronous message send operations.
//!
//! This module provides the [`SendCallback`] trait and related types for handling
//! results of asynchronous message send operations. Callbacks can be implemented
//! directly as trait implementations or provided as closures through automatic
//! blanket implementation.

use std::sync::Arc;

use crate::producer::send_result::SendResult;

/// Callback trait for handling asynchronous message send results.
///
/// Implementors of this trait can respond to successful message delivery or
/// send failures. Closures that match the expected signature automatically
/// implement this trait through blanket implementation.
///
/// # Examples
///
/// Using a closure as callback:
///
/// ```rust,ignore
/// producer.send_with_callback(message, |result, error| {
///     match (result, error) {
///         (Some(send_result), None) => {
///             println!("Sent: {:?}", send_result.msg_id);
///         }
///         (None, Some(err)) => {
///             eprintln!("Failed: {}", err);
///         }
///         _ => {}
///     }
/// }).await?;
/// ```
///
/// Implementing the trait:
///
/// ```rust,ignore
/// struct LoggingCallback;
///
/// impl SendCallback for LoggingCallback {
///     fn on_success(&self, send_result: &SendResult) {
///         log::info!("Message sent: {:?}", send_result.msg_id);
///     }
///
///     fn on_exception(&self, error: &dyn std::error::Error) {
///         log::error!("Send failed: {}", error);
///     }
/// }
/// ```
pub trait SendCallback: Send + Sync {
    /// Invoked when a message is successfully sent.
    ///
    /// # Arguments
    ///
    /// * `send_result` - Contains the message ID, queue information, and send metadata.
    fn on_success(&self, send_result: &SendResult);

    /// Invoked when a message send operation fails.
    ///
    /// # Arguments
    ///
    /// * `error` - The error that caused the send failure.
    fn on_exception(&self, error: &dyn std::error::Error);
}

// Blanket implementation allowing closures to act as callbacks.
//
// Closures receive `Option<&SendResult>` and `Option<&dyn Error>` parameters,
// where exactly one is `Some` and the other is `None`.
impl<F> SendCallback for F
where
    F: Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync,
{
    fn on_success(&self, send_result: &SendResult) {
        self(Some(send_result), None)
    }

    fn on_exception(&self, error: &dyn std::error::Error) {
        self(None, Some(error))
    }
}

/// Type alias for dynamically dispatched send callbacks.
///
/// Use this type when storing callbacks in struct fields or when the callback
/// type cannot be determined at compile time. For function parameters, prefer
/// generic bounds over `SendCallback` to enable monomorphization.
///
/// # Examples
///
/// ```rust,ignore
/// struct ProducerState {
///     pending: Vec<ArcSendCallback>,
/// }
///
/// impl ProducerState {
///     fn add<CB: SendCallback + 'static>(&mut self, callback: CB) {
///         self.pending.push(Arc::new(callback));
///     }
/// }
/// ```
pub type ArcSendCallback = Arc<dyn SendCallback>;

/// Legacy type alias for backward compatibility.
///
/// This type is deprecated in favor of the trait-based approach.
/// Use `ArcSendCallback` for storage or generic `SendCallback` bound for parameters.
///
/// # Migration
///
/// ```rust,ignore
/// // Old style (still works)
/// let callback: SendMessageCallback = Arc::new(|result, error| { /* ... */ });
///
/// // New style (recommended)
/// let callback: ArcSendCallback = Arc::new(|result, error| { /* ... */ });
///
/// // Or use generic parameter (best performance)
/// fn my_function<CB: SendCallback + 'static>(callback: CB) { /* ... */ }
/// ```
#[deprecated(since = "0.8.0", note = "Use ArcSendCallback or generic SendCallback bound instead")]
pub type SendMessageCallback = Arc<dyn Fn(Option<&SendResult>, Option<&dyn std::error::Error>) + Send + Sync>;
