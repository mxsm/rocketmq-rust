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

//! Chain of responsibility pattern implementation.
//!
//! This module provides traits and types for implementing the chain of
//! responsibility design pattern in RocketMQ.

use std::cell::Cell;

pub mod handler;

pub use handler::Handler;

/// Handler chain for managing a sequence of handlers.
///
/// This struct manages a collection of handlers and executes them in sequence.
/// Each handler can process the request and decide whether to continue to the
/// next handler in the chain.
///
/// # Type Parameters
///
/// * `T` - The type of the request/input
/// * `R` - The type of the response/output
///
/// # Examples
///
/// ```
/// use rocketmq_model::common::chain::{Handler, HandlerChain};
///
/// struct Increment;
/// struct Double;
///
/// impl Handler<i32, i32> for Increment {
///     fn handle(&self, request: i32, chain: &HandlerChain<i32, i32>) -> Option<i32> {
///         chain.handle(request + 1)
///     }
/// }
///
/// impl Handler<i32, i32> for Double {
///     fn handle(&self, request: i32, _chain: &HandlerChain<i32, i32>) -> Option<i32> {
///         Some(request * 2)
///     }
/// }
///
/// let chain = HandlerChain::create()
///     .add_next(Box::new(Increment))
///     .add_next(Box::new(Double));
/// assert_eq!(chain.handle(3), Some(8));
/// assert_eq!(chain.handle(3), None);
/// ```
///
/// The cursor advances before each handler is called. Handlers delegate explicitly
/// through [`Self::handle`]; returning a response without delegation stops that call.
/// Later calls continue at the current cursor. Independent requests require an
/// explicit [`Self::reset`] when reusing the chain.
pub struct HandlerChain<T, R> {
    /// List of handlers in the chain
    handlers: Vec<Box<dyn Handler<T, R>>>,

    /// Current position in the handler chain.
    /// Uses Cell to achieve interior mutability, avoiding unsafe code.
    current_index: Cell<usize>,
}

impl<T, R> HandlerChain<T, R> {
    /// Create a new empty handler chain.
    ///
    /// # Returns
    ///
    /// A new `HandlerChain` instance with no handlers
    ///
    /// # Examples
    ///
    /// ```
    /// use rocketmq_model::common::chain::HandlerChain;
    ///
    /// let chain = HandlerChain::<i32, i32>::create();
    /// assert!(chain.is_empty());
    /// assert_eq!(chain.handle(1), None);
    /// ```
    pub fn create() -> Self {
        Self {
            handlers: Vec::new(),
            current_index: Cell::new(0),
        }
    }

    /// Add a handler to the end of the chain.
    ///
    /// This method allows for builder-style chaining by returning `self`.
    ///
    /// # Arguments
    ///
    /// * `handler` - A boxed handler to add to the chain
    ///
    /// # Returns
    ///
    /// Self for method chaining
    ///
    /// See [`HandlerChain`] for a complete example with delegating and terminal handlers.
    pub fn add_next(mut self, handler: Box<dyn Handler<T, R>>) -> Self {
        self.handlers.push(handler);
        self
    }

    /// Process the request through the handler chain.
    ///
    /// This method executes handlers in sequence. Each handler can process
    /// the request and optionally call this method again to continue to the
    /// next handler in the chain.
    ///
    /// # Arguments
    ///
    /// * `t` - The request to process
    ///
    /// # Returns
    ///
    /// An `Option<R>` containing the response, or `None` if no handlers
    /// are left or no response is produced
    ///
    /// The cursor advances before invoking the selected handler. Returning a response
    /// without calling `chain.handle` leaves later handlers for a subsequent call.
    /// The cursor is not reset automatically, even if a handler returns `None`.
    /// See [`HandlerChain`] for delegation and [`Self::reset`] for reuse.
    pub fn handle(&self, t: T) -> Option<R> {
        let index = self.current_index.get();
        if index < self.handlers.len() {
            // Update index (interior mutability)
            self.current_index.set(index + 1);

            // Now we can safely call the handler because current_index uses Cell
            // No need to mutably borrow entire self
            self.handlers[index].handle(t, self)
        } else {
            None
        }
    }

    /// Reset the chain to start from the beginning.
    ///
    /// This allows reusing the same chain for multiple requests.
    ///
    /// # Examples
    ///
    /// ```
    /// use rocketmq_model::common::chain::{Handler, HandlerChain};
    ///
    /// struct Echo;
    /// impl Handler<i32, i32> for Echo {
    ///     fn handle(&self, request: i32, _chain: &HandlerChain<i32, i32>) -> Option<i32> {
    ///         Some(request)
    ///     }
    /// }
    ///
    /// let mut chain = HandlerChain::create().add_next(Box::new(Echo));
    /// assert_eq!(chain.handle(7), Some(7));
    /// assert_eq!(chain.handle(8), None);
    /// chain.reset();
    /// assert_eq!(chain.handle(8), Some(8));
    /// ```
    pub fn reset(&mut self) {
        self.current_index.set(0);
    }

    /// Get the number of handlers in the chain.
    ///
    /// # Returns
    ///
    /// The number of handlers
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Check if the chain is empty.
    ///
    /// # Returns
    ///
    /// `true` if there are no handlers, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

impl<T, R> Default for HandlerChain<T, R> {
    fn default() -> Self {
        Self::create()
    }
}
