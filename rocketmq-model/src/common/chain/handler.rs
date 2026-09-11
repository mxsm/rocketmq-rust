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

//! Handler trait for chain of responsibility pattern.
//!
//! This module provides the `Handler` trait which defines the interface for
//! processing requests in a chain of responsibility pattern.

use super::HandlerChain;

/// Handler trait for chain of responsibility pattern.
///
/// Implementors of this trait can process requests of type `T` and return
/// results of type `R`, with the ability to delegate to the next handler
/// in the chain.
///
/// # Type Parameters
///
/// * `T` - The type of the request/input to be handled
/// * `R` - The type of the response/output returned by the handler
///
/// # Examples
///
/// ```
/// use rocketmq_model::common::chain::{Handler, HandlerChain};
///
/// struct NonNegative;
/// impl Handler<i32, i32> for NonNegative {
///     fn handle(&self, request: i32, chain: &HandlerChain<i32, i32>) -> Option<i32> {
///         if request >= 0 {
///             Some(request)
///         } else {
///             chain.handle(request)
///         }
///     }
/// }
///
/// let mut chain = HandlerChain::create().add_next(Box::new(NonNegative));
/// assert_eq!(chain.handle(3), Some(3));
/// chain.reset();
/// assert_eq!(chain.handle(-1), None);
/// ```
pub trait Handler<T, R> {
    /// Handle a request and optionally delegate to the next handler in the chain.
    ///
    /// # Arguments
    ///
    /// * `t` - The request/input to be handled
    /// * `chain` - The handler chain for delegating to subsequent handlers
    ///
    /// # Returns
    ///
    /// An `Option<R>` containing the response, or `None` if no response is produced
    ///
    /// The chain advances its cursor before invoking this method. Calling
    /// [`HandlerChain::handle`] delegates to the next handler; returning directly
    /// stops processing for this call. Later calls continue at the current cursor
    /// until [`HandlerChain::reset`] is called explicitly.
    ///
    /// See [`HandlerChain`] for a complete delegation example.
    fn handle(&self, t: T, chain: &HandlerChain<T, R>) -> Option<R>;
}
