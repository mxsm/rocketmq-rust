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
/// ```rust,ignore
/// use rocketmq_common::common::chain::{Handler, HandlerChain};
///
/// let chain = HandlerChain::<Request, Response>::create()
///     .add_next(Box::new(ValidationHandler))
///     .add_next(Box::new(AuthenticationHandler))
///     .add_next(Box::new(ProcessingHandler));
///
/// let response = chain.handle(request);
/// ```
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
    /// ```rust,ignore
    /// let chain = HandlerChain::<Request, Response>::create();
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
    /// # Examples
    ///
    /// ```rust,ignore
    /// let chain = HandlerChain::create()
    ///     .add_next(Box::new(FirstHandler))
    ///     .add_next(Box::new(SecondHandler));
    /// ```
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
    /// # Implementation Note
    ///
    /// Uses Cell to store current_index, achieving interior mutability. This allows
    /// modifying the index while holding an immutable borrow of self, avoiding
    /// borrow conflicts and unsafe code.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let mut chain = HandlerChain::create()
    ///     .add_next(Box::new(MyHandler));
    ///
    /// if let Some(response) = chain.handle(request) {
    ///     println!("Got response: {:?}", response);
    /// }
    /// ```
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
    /// ```rust,ignore
    /// let mut chain = HandlerChain::create()
    ///     .add_next(Box::new(MyHandler));
    ///
    /// chain.handle(request1);
    /// chain.reset();
    /// chain.handle(request2); // Start from first handler again
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
