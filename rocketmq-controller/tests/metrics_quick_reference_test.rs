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

//! Quick reference test demonstrating all metric recording APIs

#[cfg(test)]
mod quick_reference_tests {
    use std::sync::Arc;
    use std::time::Instant;

    use rocketmq_controller::config::ControllerConfig;
    use rocketmq_controller::metrics::controller_metrics_manager::ControllerMetricsManager;
    use rocketmq_controller::metrics::*;

    #[test]
    fn test_initialization() {
        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let _metrics = ControllerMetricsManager::get_instance(config);
    }

    #[test]
    fn test_role_recording() {
        ControllerMetricsManager::record_role_change(2, 0);
        ControllerMetricsManager::record_role_change(3, 2);
        ControllerMetricsManager::record_role_change(2, 3);
    }

    #[test]
    fn test_request_metrics() {
        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let metrics = ControllerMetricsManager::get_instance(config);

        metrics.inc_request_total("controller_register_broker", RequestHandleStatus::Success);
        metrics.record_request_latency("controller_register_broker", 1500);

        ControllerMetricsManager::inc_request_total_static("controller_elect_master", RequestHandleStatus::Failed);
        ControllerMetricsManager::record_request_latency_static("controller_elect_master", 2500);
    }

    #[test]
    fn test_dledger_metrics() {
        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let metrics = ControllerMetricsManager::get_instance(config);

        metrics.inc_dledger_op_total(DLedgerOperation::Append, DLedgerOperationStatus::Success);
        metrics.record_dledger_op_latency(DLedgerOperation::Append, 800);

        ControllerMetricsManager::inc_dledger_op_total_static(DLedgerOperation::Append, DLedgerOperationStatus::Failed);
        ControllerMetricsManager::record_dledger_op_latency_static(DLedgerOperation::Append, 1200);
    }

    #[test]
    fn test_election_metrics() {
        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let metrics = ControllerMetricsManager::get_instance(config);

        metrics.inc_election_total(ElectionResult::NewMasterElected);
        metrics.inc_election_total(ElectionResult::KeepCurrentMaster);

        ControllerMetricsManager::inc_election_total_static(ElectionResult::NoMasterElected);
    }

    #[test]
    fn test_request_with_timing() {
        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let metrics = ControllerMetricsManager::get_instance(config);

        let start = Instant::now();
        std::thread::sleep(std::time::Duration::from_micros(100));
        let latency_us = start.elapsed().as_micros() as u64;

        metrics.inc_request_total("test_request", RequestHandleStatus::Success);
        metrics.record_request_latency("test_request", latency_us);

        assert!(latency_us >= 100);
    }

    #[test]
    fn test_all_request_statuses() {
        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let metrics = ControllerMetricsManager::get_instance(config);

        metrics.inc_request_total("test", RequestHandleStatus::Success);
        metrics.inc_request_total("test", RequestHandleStatus::Failed);
        metrics.inc_request_total("test", RequestHandleStatus::Timeout);
    }

    #[test]
    fn test_all_dledger_statuses() {
        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let metrics = ControllerMetricsManager::get_instance(config);

        metrics.inc_dledger_op_total(DLedgerOperation::Append, DLedgerOperationStatus::Success);
        metrics.inc_dledger_op_total(DLedgerOperation::Append, DLedgerOperationStatus::Failed);
        metrics.inc_dledger_op_total(DLedgerOperation::Append, DLedgerOperationStatus::Timeout);
    }

    #[test]
    fn test_all_election_results() {
        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let metrics = ControllerMetricsManager::get_instance(config);

        metrics.inc_election_total(ElectionResult::NewMasterElected);
        metrics.inc_election_total(ElectionResult::KeepCurrentMaster);
        metrics.inc_election_total(ElectionResult::NoMasterElected);
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let config = Arc::new(ControllerConfig::default().with_node_info(1, "127.0.0.1:9876".parse().unwrap()));
        let _metrics = ControllerMetricsManager::get_instance(config);

        let handles: Vec<_> = (0..10)
            .map(|i| {
                thread::spawn(move || {
                    for _ in 0..100 {
                        ControllerMetricsManager::inc_request_total_static(
                            &format!("request_{}", i),
                            RequestHandleStatus::Success,
                        );
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
