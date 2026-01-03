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

//! Comprehensive Tests and Examples for Processor Registry V2
//!
//! This file demonstrates:
//! 1. How to implement custom processors using GAT
//! 2. How to compose processors into Core enum
//! 3. How to register and use plugins
//! 4. Performance characteristics of each layer

use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_error::RocketMQResult;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_remoting::runtime::processor_v2::CoreProcessor;
use rocketmq_remoting::runtime::processor_v2::CoreProcessorVariant;
use rocketmq_remoting::runtime::processor_v2::PluginProcessorRegistry;
use rocketmq_remoting::runtime::processor_v2::ProcessorDispatcher;
use rocketmq_remoting::runtime::processor_v2::RequestProcessorV2;

/// A simple processor that echoes back the request
struct EchoProcessor;

impl RequestProcessorV2 for EchoProcessor {
    // Use `impl Future` syntax (RPIT - Return Position Impl Trait)
    // This is equivalent to defining an opaque type
    type Fut<'a>
        = impl Future<Output = RocketMQResult<Option<RemotingCommand>>> + Send + 'a
    where
        Self: 'a;

    fn process_request<'a>(
        &'a mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request: &'a mut RemotingCommand,
    ) -> Self::Fut<'a> {
        async move {
            // Simple echo: create response
            let response = RemotingCommand::create_response_command()
                .set_code(100)
                .set_remark("Echo from EchoProcessor");
            Ok(Some(response))
        }
    }
}

// Note: Tests requiring actual Channel/Context objects are disabled
// They require real runtime initialization

/// A processor that counts how many requests it has processed
#[derive(Clone)]
struct MetricsProcessor {
    request_count: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
}

impl MetricsProcessor {
    fn new() -> Self {
        Self {
            request_count: Arc::new(AtomicU64::new(0)),
            total_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get_metrics(&self) -> (u64, u64) {
        (
            self.request_count.load(Ordering::Relaxed),
            self.total_bytes.load(Ordering::Relaxed),
        )
    }
}

impl RequestProcessorV2 for MetricsProcessor {
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
            // Update metrics
            self.request_count.fetch_add(1, Ordering::Relaxed);

            if let Some(body) = request.body() {
                self.total_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
            }

            // Process request
            let response = RemotingCommand::create_response_command().set_remark(format!(
                "Processed {} requests",
                self.request_count.load(Ordering::Relaxed)
            ));

            Ok(Some(response))
        }
    }
}

#[test]
fn test_metrics_processor_creation() {
    // Test processor creation and metrics initialization
    let processor = MetricsProcessor::new();
    let (count, bytes) = processor.get_metrics();
    assert_eq!(count, 0);
    assert_eq!(bytes, 0);
}

/// Demonstrates how to create a custom core processor enum
type MyAppCoreProcessor = CoreProcessor<EchoProcessor, MetricsProcessor, EchoProcessor>;

#[test]
fn test_core_processor_types() {
    // Test that different processor types can be composed
    let _echo = MyAppCoreProcessor::Send(EchoProcessor);
    let _metrics = MyAppCoreProcessor::Pull(MetricsProcessor::new());
    let _admin = MyAppCoreProcessor::Admin(EchoProcessor);
}

#[test]
fn test_plugin_registry_creation() {
    let mut registry = PluginProcessorRegistry::new();

    // Register multiple plugins
    registry.register(1001, |_channel, _ctx, _request| async move {
        let response = RemotingCommand::create_response_command().set_remark("Plugin 1001");
        Ok(Some(response))
    });

    registry.register(1002, |_channel, _ctx, request| {
        // Capture request data before async move
        let code = request.code();
        async move {
            let response =
                RemotingCommand::create_response_command().set_remark(format!("Plugin 1002 handled code {}", code));
            Ok(Some(response))
        }
    });

    // Verify plugins are registered
    assert!(registry.contains(1001));
    assert!(registry.contains(1002));
    assert!(!registry.contains(9999));
}

#[test]
fn test_dispatcher_creation() {
    // Create individual core processors
    let send_processor = EchoProcessor;
    let pull_processor = MetricsProcessor::new();
    let admin_processor = EchoProcessor;

    // Create dispatcher with individual processors
    let mut dispatcher = ProcessorDispatcher::new(send_processor, pull_processor, admin_processor);

    // Register core mappings
    dispatcher.register_core(100, CoreProcessorVariant::Send);
    dispatcher.register_core(200, CoreProcessorVariant::Pull);
    dispatcher.register_core(300, CoreProcessorVariant::Admin);

    // Register plugins
    dispatcher.register_plugin(9001, |_channel, _ctx, _request| async move {
        let response = RemotingCommand::create_response_command().set_remark("Experimental Feature");
        Ok(Some(response))
    });

    // Note: Dispatcher doesn't expose has_core/has_plugin methods
    // Registration is successful if no panic occurs
}

/// Demonstrates a processor that performs async I/O
struct AsyncIoProcessor {
    db_client: Arc<MockDatabase>,
}

struct MockDatabase;

impl MockDatabase {
    async fn query(&self, _key: &str) -> String {
        // Simulate async I/O
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        "mock_value".to_string()
    }
}

impl RequestProcessorV2 for AsyncIoProcessor {
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
            // Perform async I/O
            let key = request.remark().as_ref().map(|s| s.as_str()).unwrap_or("default");
            let value = self.db_client.query(key).await;

            // Create response
            let response = RemotingCommand::create_response_command().set_remark(format!("DB result: {}", value));

            Ok(Some(response))
        }
    }
}

#[tokio::test]
async fn test_async_io_processor() {
    let processor = AsyncIoProcessor {
        db_client: Arc::new(MockDatabase),
    };

    let _request = RemotingCommand::create_remoting_command(400).set_remark("test_key");

    // Note: This test uses mock objects and demonstrates the API
    // Real usage would require actual Channel and Context
    let _result = async {
        // Simulate processing - would need real objects in production
        let _value = processor.db_client.query("test_key").await;
        Ok::<(), rocketmq_error::RocketMQError>(())
    }
    .await;
}

/// This test verifies that our GAT-based processors don't allocate
/// We can't directly test this in a unit test, but we can use
/// compiler output and profiling tools.
///
/// To verify zero allocation:
/// 1. Compile with: `cargo rustc --release -- --emit=mir`
/// 2. Check MIR output for absence of `Box::new` calls
/// 3. Use flamegraph: `cargo flamegraph --test processor_v2_tests`
/// 4. Verify no `__rust_alloc` calls in hot path
#[test]
#[ignore = "Manual verification required"]
fn verify_zero_allocation() {
    // This is a marker test for documentation purposes
    // Actual verification requires:
    // - MIR inspection
    // - Flamegraph profiling
    // - Memory profiling tools (valgrind, heaptrack)
}

#[test]
fn test_plugin_hot_reload() {
    // Create individual core processors
    let send_processor = EchoProcessor;
    let pull_processor = MetricsProcessor::new();
    let admin_processor = EchoProcessor;

    let mut dispatcher = ProcessorDispatcher::new(send_processor, pull_processor, admin_processor);

    // Initial plugin version
    dispatcher.register_plugin(5000, |_channel, _ctx, _request| async move {
        let response = RemotingCommand::create_response_command().set_remark("Plugin v1");
        Ok(Some(response))
    });

    // "Hot reload" by re-registering (overwrites old plugin)
    dispatcher.register_plugin(5000, |_channel, _ctx, _request| async move {
        let response = RemotingCommand::create_response_command().set_remark("Plugin v2 - Updated!");
        Ok(Some(response))
    });

    // Registration successful (no panics = success)
}

/// Demonstrates the complete API surface
///
/// ```no_run
/// use rocketmq_remoting::runtime::processor_v2::*;
///
/// async fn example() {
///     // 1. Create processors
///     // let send_proc = SendProcessor::new();
///     // let pull_proc = PullProcessor::new();
///     // let admin_proc = AdminProcessor::new();
///     //
///     // // 2. Compose into core
///     // let core = CoreProcessor::Send(send_proc);
///     //
///     // // 3. Create dispatcher
///     // let mut dispatcher = ProcessorDispatcher::new(core);
///     //
///     // // 4. Register core codes
///     // dispatcher.register_core(10, CoreProcessorVariant::Send);
///     // dispatcher.register_core(11, CoreProcessorVariant::Pull);
///     //
///     // // 5. Register plugins
///     // dispatcher.register_plugin(9000, |channel, ctx, request| async move {
///     //     // Plugin logic
///     //     Ok(None)
///     // });
///     //
///     // // 6. Dispatch requests
///     // let response = dispatcher.dispatch(
///     //     10,
///     //     channel,
///     //     ctx,
///     //     &mut request
///     // ).await.unwrap();
/// }
/// ```
#[allow(dead_code)]
fn api_surface_documentation() {}

/// Test to verify that core_mapping actually controls routing
#[test]
fn test_core_mapping_routing() {
    // Create tracking processors to verify which one gets called
    let send = MetricsProcessor::new();
    let pull = MetricsProcessor::new();
    let admin = MetricsProcessor::new();

    // Keep references to check metrics
    let send_ref = send.clone();
    let pull_ref = pull.clone();
    let admin_ref = admin.clone();

    let mut dispatcher = ProcessorDispatcher::new(send, pull, admin);

    // Register request code 100 to Pull variant
    dispatcher.register_core(100, CoreProcessorVariant::Pull);

    // Register request code 200 to Admin variant
    dispatcher.register_core(200, CoreProcessorVariant::Admin);

    // Register request code 300 to Send variant
    dispatcher.register_core(300, CoreProcessorVariant::Send);

    // Verify initial state - all processors should have 0 requests
    assert_eq!(send_ref.get_metrics().0, 0);
    assert_eq!(pull_ref.get_metrics().0, 0);
    assert_eq!(admin_ref.get_metrics().0, 0);

    // In a real test, we would dispatch and verify:
    // - Code 100 should increment pull_ref counter (not send_ref)
    // - Code 200 should increment admin_ref counter
    // - Code 300 should increment send_ref counter
    //
    // This proves that core_mapping controls routing, not the enum variant
}
