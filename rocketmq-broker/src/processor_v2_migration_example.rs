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

//! Example: Migrating Broker Processor to V2 Architecture
//!
//! This file demonstrates how to migrate the current broker processor
//! architecture to the new Core + Plugin + GAT design.

use std::future::Future;

use rocketmq_error::RocketMQResult;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor_v2::CoreProcessor;
use rocketmq_remoting::runtime::processor_v2::CoreProcessorVariant;
use rocketmq_remoting::runtime::processor_v2::ProcessorDispatcher;
use rocketmq_remoting::runtime::processor_v2::RequestProcessorV2;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;

use crate::transaction::transactional_message_service::TransactionalMessageService;

// ============================================================================
// Step 1: Adapt Existing Processors to RequestProcessorV2
// ============================================================================

/// Wrapper for existing SendMessageProcessor to implement GAT trait
pub struct SendMessageProcessorV2<MS: MessageStore, TS> {
    // Reuse existing processor logic
    inner: ArcMut<crate::processor::send_message_processor::SendMessageProcessor<MS, TS>>,
}

impl<MS: MessageStore, TS> SendMessageProcessorV2<MS, TS> {
    pub fn new(
        inner: ArcMut<crate::processor::send_message_processor::SendMessageProcessor<MS, TS>>,
    ) -> Self {
        Self { inner }
    }
}

impl<MS, TS> RequestProcessorV2 for SendMessageProcessorV2<MS, TS>
where
    MS: MessageStore + Send + Sync + 'static,
    TS: TransactionalMessageService + Send + Sync + 'static,
{
    type Fut<'a> = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        async move {
            // Delegate to existing implementation
            use rocketmq_remoting::runtime::processor::RequestProcessor;
            self.inner.process_request(channel, ctx, request).await
        }
    }
}

/// Wrapper for PullMessageProcessor
pub struct PullMessageProcessorV2<MS: MessageStore> {
    inner: ArcMut<crate::processor::pull_message_processor::PullMessageProcessor<MS>>,
}

impl<MS: MessageStore> PullMessageProcessorV2<MS> {
    pub fn new(
        inner: ArcMut<crate::processor::pull_message_processor::PullMessageProcessor<MS>>,
    ) -> Self {
        Self { inner }
    }
}

impl<MS> RequestProcessorV2 for PullMessageProcessorV2<MS>
where
    MS: MessageStore + Send + Sync + 'static,
{
    type Fut<'a> = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        async move {
            use rocketmq_remoting::runtime::processor::RequestProcessor;
            self.inner.process_request(channel, ctx, request).await
        }
    }
}

/// Wrapper for AdminBrokerProcessor
pub struct AdminBrokerProcessorV2<MS: MessageStore> {
    inner: ArcMut<crate::processor::admin_broker_processor::AdminBrokerProcessor<MS>>,
}

impl<MS: MessageStore> AdminBrokerProcessorV2<MS> {
    pub fn new(
        inner: ArcMut<crate::processor::admin_broker_processor::AdminBrokerProcessor<MS>>,
    ) -> Self {
        Self { inner }
    }
}

impl<MS> RequestProcessorV2 for AdminBrokerProcessorV2<MS>
where
    MS: MessageStore + Send + Sync + 'static,
{
    type Fut<'a> = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        async move {
            use rocketmq_remoting::runtime::processor::RequestProcessor;
            self.inner.process_request(channel, ctx, request).await
        }
    }
}

// ============================================================================
// Step 2: Define Broker Core Processor Type
// ============================================================================

/// Broker's core processor enum with three high-frequency variants
pub type BrokerCoreProcessor<MS, TS> = CoreProcessor<
    SendMessageProcessorV2<MS, TS>,
    PullMessageProcessorV2<MS>,
    AdminBrokerProcessorV2<MS>,
>;

// ============================================================================
// Step 3: Create Broker Processor Dispatcher V2
// ============================================================================

/// New broker request processor using V2 architecture
pub struct BrokerRequestProcessorV2<MS: MessageStore, TS> {
    dispatcher: ProcessorDispatcher<BrokerCoreProcessor<MS, TS>>,
}

impl<MS, TS> BrokerRequestProcessorV2<MS, TS>
where
    MS: MessageStore + Send + Sync + 'static,
    TS: TransactionalMessageService + Send + Sync + 'static,
{
    pub fn new(
        send_processor: ArcMut<crate::processor::send_message_processor::SendMessageProcessor<MS, TS>>,
        pull_processor: ArcMut<crate::processor::pull_message_processor::PullMessageProcessor<MS>>,
        admin_processor: ArcMut<crate::processor::admin_broker_processor::AdminBrokerProcessor<MS>>,
    ) -> Self {
        // Wrap existing processors
        let send_v2 = SendMessageProcessorV2::new(send_processor);
        let pull_v2 = PullMessageProcessorV2::new(pull_processor);
        let admin_v2 = AdminBrokerProcessorV2::new(admin_processor);

        // Create core processor enum
        let core = BrokerCoreProcessor::Send(send_v2);

        // Create dispatcher
        let mut dispatcher = ProcessorDispatcher::new(core);

        // Register core processors (high-frequency)
        use rocketmq_remoting::code::request_code::RequestCode;

        dispatcher.register_core(RequestCode::SendMessage as i32, CoreProcessorVariant::Send);
        dispatcher.register_core(RequestCode::SendMessageV2 as i32, CoreProcessorVariant::Send);
        dispatcher.register_core(
            RequestCode::SendBatchMessage as i32,
            CoreProcessorVariant::Send,
        );
        dispatcher.register_core(
            RequestCode::ConsumerSendMsgBack as i32,
            CoreProcessorVariant::Send,
        );

        dispatcher.register_core(RequestCode::PullMessage as i32, CoreProcessorVariant::Pull);
        dispatcher.register_core(
            RequestCode::LitePullMessage as i32,
            CoreProcessorVariant::Pull,
        );

        Self { dispatcher }
    }

    /// Register a plugin processor for experimental or third-party features
    pub fn register_plugin<F, Fut>(&mut self, request_code: i32, processor: F)
    where
        F: Fn(Channel, ConnectionHandlerContext, &mut RemotingCommand) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'static,
    {
        self.dispatcher.register_plugin(request_code, processor);
    }

    /// Process a request using the V2 dispatcher
    pub async fn process_request(
        &mut self,
        request_code: i32,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.dispatcher
            .dispatch(request_code, channel, ctx, request)
            .await
    }
}

// ============================================================================
// Step 4: Example Usage in BrokerRuntime
// ============================================================================

/// Example: How to integrate V2 processor in BrokerRuntime
#[cfg(test)]
mod integration_example {
    use super::*;

    #[tokio::test]
    async fn example_broker_runtime_integration() {
        // Assume we have existing processor instances
        // let send_processor = ArcMut::new(SendMessageProcessor::new(...));
        // let pull_processor = ArcMut::new(PullMessageProcessor::new(...));
        // let admin_processor = ArcMut::new(AdminBrokerProcessor::new(...));

        // Create V2 processor dispatcher
        // let mut processor_v2 = BrokerRequestProcessorV2::new(
        //     send_processor,
        //     pull_processor,
        //     admin_processor,
        // );

        // Register experimental plugin
        // processor_v2.register_plugin(9999, |channel, ctx, request| async move {
        //     // Custom handling for experimental request code
        //     println!("Handling experimental request");
        //     Ok(None)
        // });

        // In the request handling loop:
        // let response = processor_v2
        //     .process_request(request_code, channel, ctx, &mut request)
        //     .await?;
    }
}

// ============================================================================
// Step 5: Performance Comparison Example
// ============================================================================

#[cfg(test)]
mod benchmarks {
    use super::*;

    /// Benchmark helper to compare old vs new architecture
    ///
    /// Expected results:
    /// - Old (BoxFuture): ~500ns per call (includes heap allocation)
    /// - New (GAT): ~100ns per call (stack-only)
    ///
    /// Measurement methodology:
    /// ```bash
    /// cargo bench --bench processor_v2_comparison
    /// ```
    #[tokio::test]
    #[ignore] // Only run with cargo bench
    async fn benchmark_processor_v2_vs_legacy() {
        // Pseudo-benchmark (use criterion in real benchmarks)
        // let iterations = 1_000_000;
        //
        // let start = std::time::Instant::now();
        // for _ in 0..iterations {
        //     // Old architecture call
        //     // legacy_processor.process_request(...).await;
        // }
        // let legacy_duration = start.elapsed();
        //
        // let start = std::time::Instant::now();
        // for _ in 0..iterations {
        //     // New architecture call
        //     // processor_v2.process_request(...).await;
        // }
        // let v2_duration = start.elapsed();
        //
        // println!("Legacy: {:?}", legacy_duration / iterations);
        // println!("V2: {:?}", v2_duration / iterations);
        // println!("Improvement: {:.1}%",
        //     (legacy_duration.as_nanos() - v2_duration.as_nanos()) as f64 /
        //     legacy_duration.as_nanos() as f64 * 100.0
        // );
    }
}

// ============================================================================
// Step 6: Migration Checklist
// ============================================================================

/// # Migration Checklist
///
/// ## Phase 1: Preparation
/// - [ ] Identify high-frequency processors (profiling data)
/// - [ ] Create V2 wrappers for top 3-5 processors
/// - [ ] Set up benchmarks to measure improvement
///
/// ## Phase 2: Experimental Rollout
/// - [ ] Add feature flag: `processor_v2`
/// - [ ] Implement parallel dispatch (fallback to legacy)
/// - [ ] A/B test in staging environment
///
/// ## Phase 3: Full Migration
/// - [ ] Migrate all core processors to V2
/// - [ ] Convert low-frequency processors to plugins
/// - [ ] Remove legacy `BrokerRequestProcessor`
/// - [ ] Update documentation
///
/// ## Phase 4: Optimization
/// - [ ] Profile with flamegraph to verify zero allocation
/// - [ ] Tune enum variant ordering for branch prediction
/// - [ ] Implement plugin hot-reload if needed
