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
/// ```rust,ignore
/// use rocketmq_common::common::chain::{Handler, HandlerChain};
///
/// struct MyHandler;
///
/// impl Handler<Request, Response> for MyHandler {
///     fn handle(&self, request: Request, chain: &HandlerChain<Request, Response>) -> Option<Response> {
///         // Process the request
///         println!("Processing request: {:?}", request);
///         
///         // Optionally delegate to next handler in chain
///         chain.handle(request)
///     }
/// }
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
    /// # Examples
    ///
    /// ```rust,ignore
    /// fn handle(&self, request: Request, chain: &HandlerChain<Request, Response>) -> Option<Response> {
    ///     // Pre-processing logic
    ///     let modified_request = preprocess(request);
    ///     
    ///     // Delegate to next handler
    ///     let response = chain.handle(modified_request);
    ///     
    ///     // Post-processing logic
    ///     response.map(|r| postprocess(r))
    /// }
    /// ```
    fn handle(&self, t: T, chain: &HandlerChain<T, R>) -> Option<R>;
}
