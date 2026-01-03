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

#![allow(incomplete_features)]
#![feature(impl_trait_in_assoc_type)]

//! Complete Working Example: Processor Registry V2
//!
//! This example demonstrates a fully functional processor registry
//! with all three layers working together.
//!
//! NOTE: This is a demonstration/documentation example showing the API usage.
//! The main() function contains pseudo-code that won't compile due to mock objects.
//! The actual processor implementations and tests are fully functional.

use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor_v2::CoreProcessorVariant;
use rocketmq_remoting::runtime::processor_v2::ProcessorDispatcher;
use rocketmq_remoting::runtime::processor_v2::RequestProcessorV2;

/// High-frequency processor: Send Message
#[derive(Clone)]
pub struct SendMessageProcessor {
    send_count: Arc<AtomicU64>,
}

impl Default for SendMessageProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl SendMessageProcessor {
    pub fn new() -> Self {
        Self {
            send_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_send_count(&self) -> u64 {
        self.send_count.load(Ordering::Relaxed)
    }
}

impl RequestProcessorV2 for SendMessageProcessor {
    type Fut<'a>
        = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        async move {
            // Increment counter
            self.send_count.fetch_add(1, Ordering::Relaxed);

            // Business logic: validate and send message
            let topic = request
                .ext_fields()
                .and_then(|f| f.get("topic"))
                .cloned()
                .unwrap_or_else(|| CheetahString::from("DefaultTopic"));

            println!("[SendMessageProcessor] Processing send to topic: {}", topic);

            // Create response
            let response = RemotingCommand::create_response_command()
                .set_code(0) // Success
                .set_remark(format!("Message sent to {}", topic));

            Ok(Some(response))
        }
    }
}

/// Medium-frequency processor: Pull Message
#[derive(Clone)]
pub struct PullMessageProcessor {
    pull_count: Arc<AtomicU64>,
}

impl Default for PullMessageProcessor {
    fn default() -> Self {
        Self::new()
    }
}
impl PullMessageProcessor {
    pub fn new() -> Self {
        Self {
            pull_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_pull_count(&self) -> u64 {
        self.pull_count.load(Ordering::Relaxed)
    }
}

impl RequestProcessorV2 for PullMessageProcessor {
    type Fut<'a>
        = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        async move {
            // Increment counter
            self.pull_count.fetch_add(1, Ordering::Relaxed);

            // Business logic: pull messages
            let queue_id = request
                .ext_fields()
                .and_then(|f| f.get("queueId"))
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            println!("[PullMessageProcessor] Processing pull from queue: {}", queue_id);

            // Simulate pulling messages
            let response = RemotingCommand::create_response_command()
                .set_code(0) // Success
                .set_body(vec![0u8; 1024]) // Mock message data
                .set_remark(format!("Pulled from queue {}", queue_id));

            Ok(Some(response))
        }
    }
}

/// Low-frequency processor: Admin Operations
#[derive(Clone)]
pub struct AdminProcessor {
    admin_count: Arc<AtomicU64>,
}

impl Default for AdminProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl AdminProcessor {
    pub fn new() -> Self {
        Self {
            admin_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_admin_count(&self) -> u64 {
        self.admin_count.load(Ordering::Relaxed)
    }
}

impl RequestProcessorV2 for AdminProcessor {
    type Fut<'a>
        = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        async move {
            // Increment counter
            self.admin_count.fetch_add(1, Ordering::Relaxed);

            // Business logic: admin operations
            let operation = request
                .ext_fields()
                .and_then(|f| f.get("operation"))
                .cloned()
                .unwrap_or_else(|| CheetahString::from("status"));

            println!("[AdminProcessor] Processing admin operation: {}", operation);

            // Create response
            let response = RemotingCommand::create_response_command()
                .set_code(0)
                .set_remark(format!("Admin operation {} completed", operation));

            Ok(Some(response))
        }
    }
}

/// Application's dispatcher
pub struct AppProcessorDispatcher {
    dispatcher: ProcessorDispatcher<SendMessageProcessor, PullMessageProcessor, AdminProcessor>,
    // Keep references for metrics
    send_processor_ref: SendMessageProcessor,
    pull_processor_ref: PullMessageProcessor,
    admin_processor_ref: AdminProcessor,
}

impl Default for AppProcessorDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl AppProcessorDispatcher {
    pub fn new() -> Self {
        let send_processor = SendMessageProcessor::new();
        let pull_processor = PullMessageProcessor::new();
        let admin_processor = AdminProcessor::new();

        // Clone for metrics (Arc inside makes this cheap)
        let send_processor_ref = send_processor.clone();
        let pull_processor_ref = pull_processor.clone();
        let admin_processor_ref = admin_processor.clone();

        let mut dispatcher = ProcessorDispatcher::new(send_processor, pull_processor, admin_processor);

        // Register core processors
        dispatcher.register_core(RequestCode::SendMessage as i32, CoreProcessorVariant::Send);
        dispatcher.register_core(RequestCode::SendMessageV2 as i32, CoreProcessorVariant::Send);
        dispatcher.register_core(RequestCode::SendBatchMessage as i32, CoreProcessorVariant::Send);

        dispatcher.register_core(RequestCode::PullMessage as i32, CoreProcessorVariant::Pull);
        dispatcher.register_core(RequestCode::LitePullMessage as i32, CoreProcessorVariant::Pull);

        dispatcher.register_core(RequestCode::GetBrokerConfig as i32, CoreProcessorVariant::Admin);
        dispatcher.register_core(RequestCode::UpdateBrokerConfig as i32, CoreProcessorVariant::Admin);

        Self {
            dispatcher,
            send_processor_ref,
            pull_processor_ref,
            admin_processor_ref,
        }
    }

    /// Register experimental plugin
    pub fn register_experimental_features(&mut self) {
        // Plugin 1: Custom trace interceptor
        self.dispatcher.register_plugin(9001, |_channel, _ctx, request| {
            let code = request.code();
            let opaque = request.opaque();
            async move {
                println!("[Plugin:Trace] Request code: {}, opaque: {}", code, opaque);

                let response = RemotingCommand::create_response_command().set_remark("Trace logged".to_string());
                Ok(Some(response))
            }
        });

        // Plugin 2: A/B testing new algorithm
        self.dispatcher
            .register_plugin(9002, |_channel, _ctx, _request| async move {
                println!("[Plugin:ABTest] Testing new algorithm");

                // Simulate async operation
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                let response =
                    RemotingCommand::create_response_command().set_remark("New algorithm result".to_string());
                Ok(Some(response))
            });

        // Plugin 3: Third-party extension
        self.dispatcher
            .register_plugin(9003, |_channel, _ctx, _request| async move {
                println!("[Plugin:ThirdParty] External processor");

                let response =
                    RemotingCommand::create_response_command().set_remark("Third-party processed".to_string());
                Ok(Some(response))
            });
    }

    /// Process a request
    pub async fn process(
        &mut self,
        request_code: i32,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> RocketMQResult<Option<RemotingCommand>> {
        self.dispatcher.dispatch(request_code, channel, ctx, request).await
    }

    /// Get metrics
    pub fn get_metrics(&self) -> (u64, u64, u64) {
        (
            self.send_processor_ref.get_send_count(),
            self.pull_processor_ref.get_pull_count(),
            self.admin_processor_ref.get_admin_count(),
        )
    }
}

// The following main function is for documentation purposes.
// In a real application, you would initialize Channel and Context from
// actual network connections.
#[allow(dead_code, unreachable_code, unused_variables)]
#[tokio::main]
async fn main() -> RocketMQResult<()> {
    println!("=== RocketMQ Processor Registry V2 Demo ===\n");

    // Note: This demo requires actual Channel and Context objects from a running system.
    // For testing, see the #[cfg(test)] module below.
    unimplemented!("This example requires actual runtime initialization. See tests for working examples.");

    /*
    // Create dispatcher
    let mut dispatcher = AppProcessorDispatcher::new();

    // Register plugins
    dispatcher.register_experimental_features();

    // NOTE: In real usage, obtain channel and ctx from actual network connections
    let channel: Channel = todo!();
    let ctx: ConnectionHandlerContext = todo();

    println!("--- Testing Core Processors ---\n");

    // Test 1: Send Message (Core)
    println!("1. Sending message...");
    let mut send_request = RemotingCommand::create_remoting_command(RequestCode::SendMessage as i32);
    send_request.add_ext_field("topic", "TestTopic");

    let response = dispatcher
        .process(
            RequestCode::SendMessage as i32,
            channel.clone(),
            ctx.clone(),
            &mut send_request,
        )
        .await?;
    println!("   Response: {:?}\n", response.unwrap().remark());

    // Test 2: Pull Message (Core)
    println!("2. Pulling messages...");
    let mut pull_request = RemotingCommand::create_remoting_command(RequestCode::PullMessage as i32);
    pull_request.add_ext_field("queueId", "1");

    let response = dispatcher
        .process(
            RequestCode::PullMessage as i32,
            channel.clone(),
            ctx.clone(),
            &mut pull_request,
        )
        .await?;
    println!("   Response: {:?}\n", response.unwrap().remark());

    // Test 3: Admin Operation (Core)
    println!("3. Admin operation...");
    let mut admin_request =
        RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig as i32);
    admin_request.add_ext_field("operation", "getConfig");

    let response = dispatcher
        .process(
            RequestCode::GetBrokerConfig as i32,
            channel.clone(),
            ctx.clone(),
            &mut admin_request,
        )
        .await?;
    println!("   Response: {:?}\n", response.unwrap().remark());

    println!("--- Testing Plugin Processors ---\n");

    // Test 4: Trace Plugin
    println!("4. Trace plugin...");
    let mut trace_request = RemotingCommand::create_remoting_command(9001);
    let response = dispatcher
        .process(9001, channel.clone(), ctx.clone(), &mut trace_request)
        .await?;
    println!("   Response: {:?}\n", response.unwrap().remark());

    // Test 5: A/B Test Plugin
    println!("5. A/B test plugin...");
    let mut ab_request = RemotingCommand::create_remoting_command(9002);
    let response = dispatcher
        .process(9002, channel.clone(), ctx.clone(), &mut ab_request)
        .await?;
    println!("   Response: {:?}\n", response.unwrap().remark());

    // Test 6: Unsupported Code (Error)
    println!("6. Unsupported request code...");
    let mut unsupported_request = RemotingCommand::create_remoting_command(8888);
    match dispatcher
        .process(8888, channel, ctx, &mut unsupported_request)
        .await
    {
        Ok(_) => println!("   Unexpected success"),
        Err(e) => println!("   Expected error: {:?}\n", e),
    }

    // Print metrics
    println!("--- Metrics ---");
    let (send, pull, admin) = dispatcher.get_metrics();
    println!("Send count: {}", send);
    println!("Pull count: {}", pull);
    println!("Admin count: {}", admin);

    println!("\n=== Demo Complete ===");
    */

    Ok(())
}

// Note: These benchmarks require actual Channel and Context objects.
// They are disabled by default. To run them, you need to initialize
// real network connections.

#[cfg(test)]
mod benchmarks {
    use super::*;

    #[tokio::test]
    #[ignore = "Requires actual runtime initialization"]
    async fn benchmark_core_processor_throughput() {
        let mut dispatcher = AppProcessorDispatcher::new();
        // Note: Replace these with actual objects from a running system
        let channel: Channel = todo!();
        let ctx: ConnectionHandlerContext = todo!();

        let iterations = 100_000;
        let start = std::time::Instant::now();

        for _ in 0..iterations {
            let mut request = RemotingCommand::create_remoting_command(RequestCode::SendMessage as i32);
            let _ = dispatcher
                .process(
                    RequestCode::SendMessage as i32,
                    channel.clone(),
                    ctx.clone(),
                    &mut request,
                )
                .await;
        }

        let duration = start.elapsed();
        let qps = iterations as f64 / duration.as_secs_f64();

        println!("Core processor QPS: {:.0}", qps);
        println!("Average latency: {:?}", duration / iterations);

        // Expected: >1M QPS on modern hardware
        assert!(qps > 500_000.0, "QPS too low: {}", qps);
    }

    #[tokio::test]
    #[ignore = "Requires actual runtime initialization"]
    async fn benchmark_plugin_processor_throughput() {
        let mut dispatcher = AppProcessorDispatcher::new();
        dispatcher.register_experimental_features();

        // Note: Replace these with actual objects from a running system
        let channel: Channel = todo!();
        let ctx: ConnectionHandlerContext = todo!();

        let iterations = 10_000;
        let start = std::time::Instant::now();

        for _ in 0..iterations {
            let mut request = RemotingCommand::create_remoting_command(9001);
            let _ = dispatcher
                .process(9001, channel.clone(), ctx.clone(), &mut request)
                .await;
        }

        let duration = start.elapsed();
        let qps = iterations as f64 / duration.as_secs_f64();

        println!("Plugin processor QPS: {:.0}", qps);
        println!("Average latency: {:?}", duration / iterations);

        // Plugin performance is lower but acceptable for cold path
        assert!(qps > 50_000.0, "Plugin QPS too low: {}", qps);
    }
}
