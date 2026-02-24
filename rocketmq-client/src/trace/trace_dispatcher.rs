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

use crate::base::access_channel::AccessChannel;

/// The type of tracing operation being performed.
///
/// This enum distinguishes between producer-side and consumer-side tracing,
/// allowing the trace dispatcher to handle different types of message flow tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// Tracing for message production/sending operations.
    Produce,
    /// Tracing for message consumption/receiving operations.
    Consume,
}

/// A dispatcher that manages the collection and transmission of message tracing data.
///
/// The `TraceDispatcher` trait defines the interface for components that collect
/// message tracing information and send it to a trace storage backend. It supports
/// both producer and consumer side tracing, enabling end-to-end message flow tracking
/// in RocketMQ applications.
///
/// # Purpose
///
/// Message tracing provides visibility into:
/// - Message production: send time, broker, topic, tags, keys
/// - Message consumption: consume time, status, retry times
/// - Message routing: which brokers and queues handled the message
///
/// # Implementation Notes
///
/// Implementations must be thread-safe (`Send + Sync`) as they may be accessed
/// concurrently from multiple producer or consumer threads. The dispatcher typically
/// batches trace data and sends it asynchronously to minimize performance impact.
///
/// # Lifecycle
///
/// 1. Create and configure the dispatcher
/// 2. Call [`start`](TraceDispatcher::start) to initialize and connect
/// 3. Call [`append`](TraceDispatcher::append) to add trace contexts
/// 4. Call [`flush`](TraceDispatcher::flush) periodically or before shutdown
/// 5. Call [`shutdown`](TraceDispatcher::shutdown) for graceful cleanup
pub trait TraceDispatcher: Any {
    /// Starts the trace dispatcher and establishes connection to the trace backend.
    ///
    /// This method initializes the dispatcher's internal state and establishes
    /// any necessary connections to name servers or trace storage systems.
    ///
    /// # Parameters
    ///
    /// * `name_srv_addr` - The name server address for service discovery
    /// * `access_channel` - The access channel configuration for network communication
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the dispatcher started successfully, or an error if
    /// initialization or connection failed.
    ///
    /// # Notes
    ///
    /// This method should be called once before any trace data is appended.
    /// Calling it multiple times may result in undefined behavior.
    fn start(&self, name_srv_addr: &str, access_channel: AccessChannel) -> rocketmq_error::RocketMQResult<()>;

    /// Appends a trace context to the dispatcher's queue for later transmission.
    ///
    /// This method adds trace data to an internal buffer. The data is typically
    /// sent in batches to minimize network overhead and performance impact.
    ///
    /// # Parameters
    ///
    /// * `ctx` - The trace context containing message metadata. The actual type depends on whether
    ///   this is producer or consumer tracing (e.g., `TraceContext` for producers, `ConsumeContext`
    ///   for consumers).
    ///
    /// # Returns
    ///
    /// Returns `true` if the context was successfully queued, `false` if the
    /// queue is full or the dispatcher is not ready.
    ///
    /// # Notes
    ///
    /// This method should be non-blocking and fast to avoid impacting message
    /// production or consumption performance.
    fn append(&self, ctx: &dyn std::any::Any) -> bool;

    /// Flushes all pending trace data to the trace backend.
    ///
    /// This method forces immediate transmission of all buffered trace contexts,
    /// bypassing any batching delay. It's typically called during shutdown or
    /// at regular intervals to ensure trace data is not lost.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if all pending data was successfully flushed, or an error
    /// if transmission failed.
    ///
    /// # Notes
    ///
    /// This operation may block until all data is sent. For production systems,
    /// consider implementing a timeout mechanism.
    fn flush(&self) -> rocketmq_error::RocketMQResult<()>;

    /// Shuts down the trace dispatcher and releases all resources.
    ///
    /// This method performs graceful shutdown by flushing any remaining trace data,
    /// closing network connections, and cleaning up internal state.
    ///
    /// # Notes
    ///
    /// After calling this method, the dispatcher should not be used again.
    /// Any subsequent calls to other methods may result in errors or panics.
    fn shutdown(&self);

    /// Returns a reference to this dispatcher as an `Any` trait object.
    ///
    /// This method enables runtime type inspection and downcasting to concrete
    /// dispatcher implementations when needed.
    ///
    /// # Returns
    ///
    /// A reference to `self` as `&dyn Any`.
    fn as_any(&self) -> &dyn Any;

    /// Returns a mutable reference to this dispatcher as an `Any` trait object.
    ///
    /// This method enables runtime type inspection and mutable downcasting to
    /// concrete dispatcher implementations when needed.
    ///
    /// # Returns
    ///
    /// A mutable reference to `self` as `&mut dyn Any`.
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

/// Type alias for an atomically reference-counted trace dispatcher.
///
/// This is the standard way to share a [`TraceDispatcher`] implementation across multiple
/// threads or components. The `Arc` provides thread-safe reference counting.
///
/// # Thread Safety
///
/// `ArcTraceDispatcher` is both `Send` and `Sync`, allowing it to be safely shared
/// across thread boundaries and accessed concurrently from multiple threads.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
/// use crate::trace::trace_dispatcher::{TraceDispatcher, ArcTraceDispatcher};
/// use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher;
///
/// // Create a dispatcher
/// let dispatcher = AsyncTraceDispatcher::new("ProducerGroup", Type::Produce, "TraceTopic", None);
/// let arc_dispatcher: ArcTraceDispatcher = Arc::new(dispatcher);
///
/// // Share across threads
/// let dispatcher_clone = arc_dispatcher.clone();
/// ```
pub type ArcTraceDispatcher = Arc<dyn TraceDispatcher + Send + Sync>;
