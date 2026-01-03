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

//! # RocketMQ Processor Registry V2
//!
//! ## Architecture: Core + Plugin + GAT
//!
//! This module implements a zero-cost, three-tier processor architecture:
//! - **Core Layer**: Static dispatch via enum + GAT (zero boxing, inline-friendly)
//! - **Plugin Layer**: Dynamic dispatch for runtime extensibility (BoxFuture allowed)
//! - **Unified Dispatcher**: Core-first routing with plugin fallback
//!
//! ## Design Principles
//! 1. Hot path (Core) = zero heap allocation
//! 2. Cold path (Plugin) = dynamic but isolated
//! 3. GAT eliminates unnecessary `Box<dyn Future>` in trait definitions
//! 4. Preserves async/await ergonomics without sacrificing performance

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use rocketmq_error::RocketMQResult;

use crate::net::channel::Channel;
use crate::protocol::remoting_command::RemotingCommand;
use crate::runtime::connection_handler_context::ConnectionHandlerContext;

/// Core processor trait using GAT to avoid `Box<dyn Future>`.
///
/// ## Why GAT?
/// - Traditional `async fn` in traits desugar to `-> impl Future`, which is NOT object-safe
/// - `#[async_trait]` macro forces `Box<dyn Future>` for every call (heap allocation)
/// - GAT allows expressing "the future's lifetime depends on `&self`" without boxing
///
/// ## Type Signature Breakdown
/// ```ignore
/// type Fut<'a>: Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send
/// where Self: 'a;
/// ```
/// - `'a`: The lifetime of the borrow from `self`
/// - `where Self: 'a`: The future may capture references from `self`
/// - `+ Send`: Required for spawning across threads in Tokio
///
/// ## Compared to async-trait
/// ```ignore
/// // async-trait (forced boxing):
/// #[async_trait]
/// trait Processor {
///     async fn process(&mut self, ...) -> Result<..>
///     // Desugars to: -> Pin<Box<dyn Future<...> + Send>>
/// }
///
/// // GAT (zero-cost when used with enums):
/// trait Processor {
///     type Fut<'a>: Future<...> + Send where Self: 'a;
///     fn process<'a>(&'a mut self, ...) -> Self::Fut<'a>;
/// }
/// ```
pub trait RequestProcessorV2 {
    /// Associated future type with lifetime bound
    type Fut<'a>: Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send
    where
        Self: 'a;

    /// Process request without forcing heap allocation
    ///
    /// # Lifetime Explanation
    /// - `'a` ties the future's lifetime to the borrow of `&'a mut self`
    /// - This allows the future to capture `self` without requiring `'static`
    /// - The compiler can stack-allocate the future state machine
    fn process_request<'a>(
        &'a mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a>;

    /// Optional hook for request rejection (non-async)
    fn reject_request(&self, _code: i32) -> (bool, Option<RemotingCommand>) {
        (false, None)
    }
}

/// Core processor registry implemented as an enum.
///
/// ## Why Enum Instead of dyn?
/// 1. **Static Dispatch**: `match` arms compile to direct function calls (inlinable)
/// 2. **No vtable**: Compiler knows exact type at every call site
/// 3. **Stack Allocation**: Enum is sized, can live on stack
/// 4. **Zero Runtime Overhead**: Equivalent to hand-written dispatch code
///
/// ## Performance Implications
/// - Branch prediction-friendly (tight match arm layout)
/// - LLVM can inline across match arms
/// - No indirect jumps (unlike vtable dispatch)
///
/// ## Trade-offs
/// - Adding new processors requires recompiling (vs runtime plugin registration)
/// - Enum size = size of largest variant (acceptable for core processors)
#[derive(Clone)]
pub enum CoreProcessor<SendProc, PullProc, AdminProc> {
    /// Send message processor (high frequency)
    Send(SendProc),
    /// Pull message processor (high frequency)
    Pull(PullProc),
    /// Admin operations (moderate frequency)
    Admin(AdminProc),
}

impl<SendProc, PullProc, AdminProc> RequestProcessorV2 for CoreProcessor<SendProc, PullProc, AdminProc>
where
    SendProc: RequestProcessorV2,
    PullProc: RequestProcessorV2,
    AdminProc: RequestProcessorV2,
{
    /// The future type is an enum of the constituent processor futures
    /// This avoids any boxing - the compiler generates a state machine enum
    type Fut<'a>
        = CoreProcessorFuture<'a, SendProc, PullProc, AdminProc>
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        match self {
            CoreProcessor::Send(p) => CoreProcessorFuture::Send(p.process_request(channel, ctx, request)),
            CoreProcessor::Pull(p) => CoreProcessorFuture::Pull(p.process_request(channel, ctx, request)),
            CoreProcessor::Admin(p) => CoreProcessorFuture::Admin(p.process_request(channel, ctx, request)),
        }
    }

    fn reject_request(&self, code: i32) -> (bool, Option<RemotingCommand>) {
        match self {
            CoreProcessor::Send(p) => p.reject_request(code),
            CoreProcessor::Pull(p) => p.reject_request(code),
            CoreProcessor::Admin(p) => p.reject_request(code),
        }
    }
}

/// Future enum for core processor dispatch
///
/// This is the concrete type returned by `CoreProcessor::process_request`.
/// The Rust compiler automatically generates an optimal state machine for this enum.
pub enum CoreProcessorFuture<'a, SendProc, PullProc, AdminProc>
where
    SendProc: RequestProcessorV2 + 'a,
    PullProc: RequestProcessorV2 + 'a,
    AdminProc: RequestProcessorV2 + 'a,
{
    Send(SendProc::Fut<'a>),
    Pull(PullProc::Fut<'a>),
    Admin(AdminProc::Fut<'a>),
}

impl<'a, SendProc, PullProc, AdminProc> Future for CoreProcessorFuture<'a, SendProc, PullProc, AdminProc>
where
    SendProc: RequestProcessorV2 + 'a,
    PullProc: RequestProcessorV2 + 'a,
    AdminProc: RequestProcessorV2 + 'a,
{
    type Output = RocketMQResult<Option<RemotingCommand>>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        // SAFETY: We never move out of the pinned data
        unsafe {
            match self.get_unchecked_mut() {
                CoreProcessorFuture::Send(fut) => Pin::new_unchecked(fut).poll(cx),
                CoreProcessorFuture::Pull(fut) => Pin::new_unchecked(fut).poll(cx),
                CoreProcessorFuture::Admin(fut) => Pin::new_unchecked(fut).poll(cx),
            }
        }
    }
}

/// Dynamic processor type for runtime-registered plugins.
///
/// ## Why BoxFuture Here?
/// - Plugins are registered at runtime (unknown types at compile time)
/// - Must erase concrete types → requires trait objects
/// - `Pin<Box<...>>` is necessary for self-referential futures in trait objects
///
/// ## HRTB (`for<'a>`) Explained
/// ```ignore
/// for<'a> Fn(...) -> Pin<Box<dyn Future<Output = ...> + Send + 'a>>
/// ```
/// - "For any lifetime 'a chosen by the caller..."
/// - Allows the closure to be called with borrowed data of any lifetime
/// - Without HRTB, you'd need to specify a concrete lifetime upfront
///
/// ## Why Not GAT Here?
/// GAT requires knowing the concrete type at compile time. Plugins are:
/// - Registered via closure/function pointers
/// - Type-erased through `Box<dyn Fn(...)>`
/// - Inherently dynamic
type DynProcessor = Box<
    dyn for<'a> Fn(
            Channel,
            ConnectionHandlerContext,
            &'a mut RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a>>
        + Send
        + Sync,
>;

/// Plugin registry for runtime-registered processors.
///
/// ## Use Cases
/// - Experimental features (can be toggled on/off)
/// - Third-party extensions
/// - A/B testing different processor implementations
/// - Hot-reloading without recompile
///
/// ## Performance Characteristics
/// - Cold path: not on critical message processing path
/// - Acceptable overhead: HashMap lookup + vtable call + heap allocation
/// - Isolated: doesn't pollute hot path (Core) performance
pub struct PluginProcessorRegistry {
    processors: HashMap<i32, DynProcessor>,
}

impl PluginProcessorRegistry {
    pub fn new() -> Self {
        Self {
            processors: HashMap::new(),
        }
    }

    /// Register a plugin processor using a simple async closure.
    ///
    /// ## Ergonomics for Plugin Authors
    /// ```ignore
    /// registry.register(RequestCode::Custom, |channel, ctx, cmd| async move {
    ///     // Plugin logic here
    ///     Ok(Some(response))
    /// });
    /// ```
    ///
    /// The closure is automatically boxed and lifetime-erased.
    pub fn register<F, Fut>(&mut self, request_code: i32, processor: F)
    where
        F: Fn(Channel, ConnectionHandlerContext, &mut RemotingCommand) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'static,
    {
        let boxed: DynProcessor = Box::new(move |channel, ctx, request| Box::pin(processor(channel, ctx, request)));
        self.processors.insert(request_code, boxed);
    }

    /// Process a request using a plugin processor.
    pub async fn process_request(
        &self,
        request_code: i32,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> Option<RocketMQResult<Option<RemotingCommand>>> {
        if let Some(processor) = self.processors.get(&request_code) {
            Some(processor(channel, ctx, request).await)
        } else {
            None
        }
    }

    pub fn contains(&self, request_code: i32) -> bool {
        self.processors.contains_key(&request_code)
    }
}

impl Default for PluginProcessorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Unified processor dispatcher with tiered routing.
///
/// ## Dispatch Strategy
/// 1. **Core Lookup**: O(1) HashMap + match (hot path)
/// 2. **Plugin Lookup**: O(1) HashMap lookup (cold path)
/// 3. **Error Handling**: Unsupported request code
///
/// ## Performance Guarantee
/// - Core processors: zero allocation, static dispatch
/// - Plugin processors: only heap allocate when plugin path is taken
/// - Clear separation prevents cross-contamination
pub struct ProcessorDispatcher<SendProc, PullProc, AdminProc> {
    /// Core processor registry (request_code → variant mapping)
    core_mapping: HashMap<i32, CoreProcessorVariant>,
    /// Individual core processors
    send_processor: SendProc,
    pull_processor: PullProc,
    admin_processor: AdminProc,
    /// Plugin processor registry
    plugin_registry: PluginProcessorRegistry,
}

/// Helper enum to map request codes to core processor variants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreProcessorVariant {
    Send,
    Pull,
    Admin,
}

impl<SendProc, PullProc, AdminProc> ProcessorDispatcher<SendProc, PullProc, AdminProc>
where
    SendProc: RequestProcessorV2,
    PullProc: RequestProcessorV2,
    AdminProc: RequestProcessorV2,
{
    pub fn new(send_processor: SendProc, pull_processor: PullProc, admin_processor: AdminProc) -> Self {
        Self {
            core_mapping: HashMap::new(),
            send_processor,
            pull_processor,
            admin_processor,
            plugin_registry: PluginProcessorRegistry::new(),
        }
    }

    /// Register a request code to a core processor variant
    pub fn register_core(&mut self, request_code: i32, variant: CoreProcessorVariant) {
        self.core_mapping.insert(request_code, variant);
    }

    /// Register a plugin processor
    pub fn register_plugin<F, Fut>(&mut self, request_code: i32, processor: F)
    where
        F: Fn(Channel, ConnectionHandlerContext, &mut RemotingCommand) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'static,
    {
        self.plugin_registry.register(request_code, processor);
    }

    /// Dispatch request with tiered routing.
    ///
    /// ## Flow
    /// ```text
    /// Request Code
    ///     │
    ///     ├─ In Core Mapping? ──Yes──> Core Processor (enum match)
    ///     │                                    │
    ///     │                                    └─> Return
    ///     │
    ///     └─ No ──> Plugin Registry Lookup
    ///                     │
    ///                     ├─ Found? ──Yes──> Plugin Processor (dyn call)
    ///                     │                        │
    ///                     │                        └─> Return
    ///                     │
    ///                     └─ No ──> UnsupportedRequestCode Error
    /// ```
    pub async fn dispatch(
        &mut self,
        request_code: i32,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // Try core processor (hot path)
        if let Some(variant) = self.core_mapping.get(&request_code) {
            return match variant {
                CoreProcessorVariant::Send => self.send_processor.process_request(channel, ctx, request).await,
                CoreProcessorVariant::Pull => self.pull_processor.process_request(channel, ctx, request).await,
                CoreProcessorVariant::Admin => self.admin_processor.process_request(channel, ctx, request).await,
            };
        }

        // Try plugin processor (cold path)
        if let Some(result) = self
            .plugin_registry
            .process_request(request_code, channel, ctx, request)
            .await
        {
            return result;
        }

        // Unsupported request code
        Err(rocketmq_error::RocketMQError::broker_operation_failed(
            "ProcessorDispatcher",
            -1,
            format!("Unsupported request code: {}", request_code),
        ))
    }
}

/// Example: Send message processor
pub struct SendMessageProcessorExample {
    // processor state
}

impl Default for SendMessageProcessorExample {
    fn default() -> Self {
        Self::new()
    }
}

impl SendMessageProcessorExample {
    pub fn new() -> Self {
        Self {}
    }

    async fn process_internal(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // Business logic here
        Ok(None)
    }
}

impl RequestProcessorV2 for SendMessageProcessorExample {
    type Fut<'a>
        = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        self.process_internal(channel, ctx, request)
    }
}

/// Example: Pull message processor
pub struct PullMessageProcessorExample {
    // processor state
}

impl Default for PullMessageProcessorExample {
    fn default() -> Self {
        Self::new()
    }
}

impl PullMessageProcessorExample {
    pub fn new() -> Self {
        Self {}
    }

    async fn process_internal(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // Business logic here
        Ok(None)
    }
}

impl RequestProcessorV2 for PullMessageProcessorExample {
    type Fut<'a>
        = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        self.process_internal(channel, ctx, request)
    }
}

/// Example: Admin processor
pub struct AdminProcessorExample {
    // processor state
}

impl Default for AdminProcessorExample {
    fn default() -> Self {
        Self::new()
    }
}

impl AdminProcessorExample {
    pub fn new() -> Self {
        Self {}
    }

    async fn process_internal(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        // Business logic here
        Ok(None)
    }
}

impl RequestProcessorV2 for AdminProcessorExample {
    type Fut<'a>
        = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        self.process_internal(channel, ctx, request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Demonstrates the complete usage pattern
    #[tokio::test]
    async fn test_processor_dispatcher() {
        // 1. Create core processor instances
        let send_processor = SendMessageProcessorExample::new();
        let pull_processor = PullMessageProcessorExample::new();
        let admin_processor = AdminProcessorExample::new();

        // 2. Create dispatcher with individual processors
        let mut dispatcher = ProcessorDispatcher::new(send_processor, pull_processor, admin_processor);

        // 3. Register core processors
        dispatcher.register_core(10, CoreProcessorVariant::Send); // SendMessage
        dispatcher.register_core(11, CoreProcessorVariant::Pull); // PullMessage
        dispatcher.register_core(50, CoreProcessorVariant::Admin); // Admin

        // 4. Register plugin processors
        dispatcher.register_plugin(9999, |_channel, _ctx, _request| async move {
            // Custom plugin logic
            Ok(Some(RemotingCommand::create_response_command()))
        });

        // 5. Dispatch requests
        // (In real code, you'd have actual Channel, ConnectionHandlerContext, RemotingCommand)
        // This demonstrates the API surface
    }
}
