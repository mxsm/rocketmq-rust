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

//! Example: Using Admin Builder and RAII patterns
//!
//! This example demonstrates:
//! - Builder pattern for admin configuration
//! - RAII automatic resource cleanup
//! - Error handling with new patterns

use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_tools::core::admin::create_admin_with_guard;
use rocketmq_tools::core::admin::AdminBuilder;
use rocketmq_tools::core::topic::TopicService;
use rocketmq_tools::core::RocketMQResult;

/// Example 1: Simple usage with helper function
async fn example_simple() -> RocketMQResult<()> {
    println!("=== Example 1: Simple Usage ===");

    // Create admin with default instance name
    let mut admin = create_admin_with_guard("127.0.0.1:9876").await?;

    // Use the admin client
    let clusters = TopicService::get_topic_cluster_list(&mut admin, "TestTopic").await?;

    println!("Topic clusters: {:?}", clusters.clusters);

    // Admin automatically cleaned up here
    Ok(())
}

/// Example 2: Builder pattern with custom configuration
async fn example_builder() -> RocketMQResult<()> {
    println!("\n=== Example 2: Builder Pattern ===");

    let admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .instance_name("example-admin")
        .timeout_millis(5000)
        .build_with_guard()
        .await?;

    // Get topic route info
    let route = admin.examine_topic_route_info("TestTopic".into()).await?;

    if let Some(route_data) = route {
        println!("Queue data count: {}", route_data.queue_datas.len());
        println!("Broker data count: {}", route_data.broker_datas.len());
    } else {
        println!("No route data found for TestTopic");
    }

    Ok(())
}

/// Example 3: Multiple NameServers with fallback
async fn example_multiple_namesrv() -> RocketMQResult<()> {
    println!("\n=== Example 3: Multiple NameServers ===");

    // Try primary NameServers
    let result = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876;127.0.0.1:9877")
        .build_with_guard()
        .await;

    let _admin = match result {
        Ok(admin) => {
            println!("Connected to primary NameServer");
            admin
        }
        Err(e) => {
            eprintln!("Primary NameServer failed: {e}");
            eprintln!("Falling back to backup...");

            // Fallback to backup
            AdminBuilder::new()
                .namesrv_addr("127.0.0.1:9878")
                .build_with_guard()
                .await?
        }
    };

    println!("Admin client ready");

    Ok(())
}

/// Example 4: Early return with automatic cleanup
async fn example_early_return(topic: &str) -> RocketMQResult<()> {
    println!("\n=== Example 4: Early Return ===");

    let admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .build_with_guard()
        .await?;

    // Get topic route
    let route = admin.examine_topic_route_info(topic.into()).await?;

    // Early return if no route data - admin still cleaned up!
    let Some(route_data) = route else {
        println!("Topic '{}' not found, exiting early", topic);
        return Ok(());
    };

    // Early return if no queues - admin still cleaned up!
    if route_data.queue_datas.is_empty() {
        println!("Topic '{}' has no queues, exiting early", topic);
        return Ok(());
    }

    println!("Topic '{}' has {} queues", topic, route_data.queue_datas.len());

    // Process queues...
    for queue in &route_data.queue_datas {
        println!(
            "  Broker: {}, ReadQueueNums: {}, WriteQueueNums: {}",
            queue.broker_name, queue.read_queue_nums, queue.write_queue_nums
        );
    }

    Ok(())
}

/// Example 5: Explicit shutdown for logging
async fn example_explicit_shutdown() -> RocketMQResult<()> {
    println!("\n=== Example 5: Explicit Shutdown ===");

    let admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .instance_name("explicit-shutdown-example")
        .build_with_guard()
        .await?;

    // Use admin...
    println!("Admin client started successfully");

    // Explicit shutdown with logging
    println!("Shutting down admin client...");
    admin.shutdown().await;
    println!("Shutdown complete");

    Ok(())
}

/// Example 6: Dynamic configuration from environment
async fn example_from_environment() -> RocketMQResult<()> {
    println!("\n=== Example 6: Environment Configuration ===");

    use std::env;

    // Read from environment with defaults
    let namesrv_addr = env::var("NAMESRV_ADDR").unwrap_or_else(|_| "127.0.0.1:9876".to_string());

    let instance_name = env::var("INSTANCE_NAME").unwrap_or_else(|_| "env-admin".to_string());

    println!("NameServer: {}", namesrv_addr);
    println!("Instance: {}", instance_name);

    let _admin = AdminBuilder::new()
        .namesrv_addr(namesrv_addr)
        .instance_name(instance_name)
        .build_with_guard()
        .await?;

    println!("Admin configured from environment");

    Ok(())
}

/// Example 7: Without RAII (manual cleanup)
async fn example_manual_cleanup() -> RocketMQResult<()> {
    println!("\n=== Example 7: Manual Cleanup ===");

    // Use build_and_start() instead of build_with_guard()
    let mut admin = AdminBuilder::new()
        .namesrv_addr("127.0.0.1:9876")
        .build_and_start() // Returns DefaultMQAdminExt directly
        .await?;

    // Use admin...
    println!("Admin started");

    // Manual shutdown required
    admin.shutdown().await;
    println!("Manually shut down");

    Ok(())
}

#[tokio::main]
async fn main() -> RocketMQResult<()> {
    println!("RocketMQ Admin Builder & RAII Examples\n");
    println!("Note: These examples require a running RocketMQ NameServer");
    println!("======================================================\n");

    // Run examples (comment out if NameServer not available)
    if let Err(e) = example_simple().await {
        eprintln!("Example 1 failed: {e}");
    }

    if let Err(e) = example_builder().await {
        eprintln!("Example 2 failed: {e}");
    }

    if let Err(e) = example_multiple_namesrv().await {
        eprintln!("Example 3 failed: {e}");
    }

    if let Err(e) = example_early_return("TestTopic").await {
        eprintln!("Example 4 failed: {e}");
    }

    if let Err(e) = example_explicit_shutdown().await {
        eprintln!("Example 5 failed: {e}");
    }

    if let Err(e) = example_from_environment().await {
        eprintln!("Example 6 failed: {e}");
    }

    if let Err(e) = example_manual_cleanup().await {
        eprintln!("Example 7 failed: {e}");
    }

    println!("\n======================================================");
    println!("Examples completed!");

    Ok(())
}
