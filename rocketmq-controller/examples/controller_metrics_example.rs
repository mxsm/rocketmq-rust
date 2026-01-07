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

//! Integration example for ControllerMetricsManager
//!
//! This example demonstrates:
//! - Initializing the metrics manager
//! - Recording various types of metrics
//! - Simulating controller operations with metrics tracking

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_controller::config::ControllerConfig;
use rocketmq_controller::metrics::controller_metrics_manager::ControllerMetricsManager;
use rocketmq_controller::metrics::*;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    println!("=== RocketMQ Controller Metrics Example ===\n");

    // 1. Initialize controller configuration
    println!("1. Initializing controller configuration...");
    let config = Arc::new(ControllerConfig::new_node(1, "127.0.0.1:60109".parse().unwrap()));

    // 2. Initialize metrics manager (singleton)
    println!("2. Initializing metrics manager...");
    let metrics_manager = ControllerMetricsManager::get_instance(config.clone());
    println!("    Metrics manager initialized\n");

    // 3. Simulate role change
    println!("3. Simulating role changes...");
    println!("   - Initial role: FOLLOWER (2)");
    ControllerMetricsManager::record_role_change(2, 0); // UNKNOWN -> FOLLOWER
    sleep(Duration::from_millis(100)).await;

    println!("   - Becoming CANDIDATE (1)");
    ControllerMetricsManager::record_role_change(1, 2); // FOLLOWER -> CANDIDATE
    sleep(Duration::from_millis(500)).await;

    println!("   - Elected as LEADER (3)");
    ControllerMetricsManager::record_role_change(3, 1); // CANDIDATE -> LEADER
    println!("    Role changes recorded\n");

    // 4. Simulate controller requests
    println!("4. Simulating controller requests...");
    simulate_controller_requests(&metrics_manager).await;
    println!("    Controller requests simulated\n");

    // 5. Simulate DLedger operations
    println!("5. Simulating DLedger operations...");
    simulate_dledger_operations(&metrics_manager).await;
    println!("    DLedger operations simulated\n");

    // 6. Simulate elections
    println!("6. Simulating elections...");
    simulate_elections(&metrics_manager).await;
    println!("    Elections simulated\n");

    println!("=== Example completed ===");
    println!("\nNote: In production, metrics would be exported to:");
    println!("  - OTLP gRPC endpoint (e.g., OpenTelemetry Collector)");
    println!("  - Prometheus HTTP endpoint");
    println!("  - Application logs");
}

/// Simulate various controller request scenarios
async fn simulate_controller_requests(manager: &Arc<ControllerMetricsManager>) {
    let request_types = vec![
        "controller_register_broker",
        "controller_broker_heartbeat",
        "controller_elect_master",
        "controller_get_replica_info",
    ];

    for request_type in request_types {
        // Simulate successful request
        let start = Instant::now();
        sleep(Duration::from_micros(rand::random::<u64>() % 5000 + 100)).await;
        let latency = start.elapsed().as_micros() as u64;

        manager.inc_request_total(request_type, RequestHandleStatus::Success);
        manager.record_request_latency(request_type, latency);

        println!("   - {} (SUCCESS): {}µs", request_type, latency);

        // Simulate occasional failures
        if rand::random_f32() < 0.2 {
            manager.inc_request_total(request_type, RequestHandleStatus::Failed);
            println!("   - {} (FAILED)", request_type);
        }

        sleep(Duration::from_millis(50)).await;
    }
}

/// Simulate DLedger append operations
async fn simulate_dledger_operations(manager: &Arc<ControllerMetricsManager>) {
    for i in 0..10 {
        let start = Instant::now();

        // Simulate append operation
        sleep(Duration::from_micros(rand::random::<u64>() % 2000 + 500)).await;

        let latency = start.elapsed().as_micros() as u64;
        let success = rand::random_f32() > 0.1; // 90% success rate

        let status = if success {
            DLedgerOperationStatus::Success
        } else {
            DLedgerOperationStatus::Failed
        };

        manager.inc_dledger_op_total(DLedgerOperation::Append, status);
        manager.record_dledger_op_latency(DLedgerOperation::Append, latency);

        println!("   - Append operation #{} ({:?}): {}µs", i + 1, status, latency);

        sleep(Duration::from_millis(50)).await;
    }
}

/// Simulate election scenarios
async fn simulate_elections(manager: &Arc<ControllerMetricsManager>) {
    // Scenario 1: New master elected
    println!("   - Election: New master elected");
    manager.inc_election_total(ElectionResult::NewMasterElected);
    sleep(Duration::from_millis(200)).await;

    // Scenario 2: Keep current master (several times)
    for i in 0..3 {
        println!("   - Election #{}: Keep current master", i + 1);
        manager.inc_election_total(ElectionResult::KeepCurrentMaster);
        sleep(Duration::from_millis(100)).await;
    }

    // Scenario 3: No master elected (split vote)
    println!("   - Election: No master elected (split vote)");
    manager.inc_election_total(ElectionResult::NoMasterElected);
}

/// Mock rand module for demonstration
mod rand {
    use std::cell::Cell;

    thread_local! {
        static SEED: Cell<u64> = const{Cell::new(12345)};
    }

    pub fn random<T>() -> T
    where
        T: From<u32>,
    {
        SEED.with(|s| {
            let val = s.get();
            s.set(val.wrapping_mul(1103515245).wrapping_add(12345));
            T::from((val % 10000) as u32)
        })
    }

    pub fn random_f32() -> f32 {
        SEED.with(|s| {
            let val = s.get();
            s.set(val.wrapping_mul(1103515245).wrapping_add(12345));
            (val % 10000) as f32 / 10000.0
        })
    }
}
