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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_rust::task::service_task::ServiceContext;
use rocketmq_rust::task::service_task::ServiceTask;
use rocketmq_rust::task::ServiceManager;

use crate::config::message_store_config::MessageStoreConfig;

pub struct FlowMonitor {
    server_manager: ServiceManager<FlowMonitorInner>,
}

impl FlowMonitor {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        let inner = FlowMonitorInner::new(message_store_config);
        let server_manager = ServiceManager::new(inner);
        FlowMonitor { server_manager }
    }

    pub async fn start(&self) -> rocketmq_error::RocketMQResult<()> {
        self.server_manager.start().await
    }

    pub async fn shutdown(&self) {
        self.server_manager.shutdown().await.unwrap();
    }
    pub async fn shutdown_with_interrupt(&self, interrupt: bool) {
        self.server_manager.shutdown_with_interrupt(interrupt).await.unwrap();
    }

    pub fn get_transferred_byte_in_second(&self) -> i64 {
        self.server_manager.as_ref().get_transferred_byte_in_second()
    }
    pub fn can_transfer_max_byte_num(&self) -> i32 {
        self.server_manager.as_ref().can_transfer_max_byte_num()
    }

    pub fn add_byte_count_transferred(&self, count: i64) {
        self.server_manager.as_ref().add_byte_count_transferred(count);
    }

    pub fn max_transfer_byte_in_second(&self) -> usize {
        self.server_manager.as_ref().max_transfer_byte_in_second()
    }
}

struct FlowMonitorInner {
    window: rocketmq_store_local::ha::flow::FlowControlWindow,
}

impl FlowMonitorInner {
    pub fn new(message_store_config: Arc<MessageStoreConfig>) -> Self {
        FlowMonitorInner {
            window: rocketmq_store_local::ha::flow::FlowControlWindow::new(
                message_store_config.ha_flow_control_enable,
                message_store_config.max_ha_transfer_byte_in_second,
            ),
        }
    }

    pub fn calculate_speed(&self) {
        self.window.roll_window();
    }

    pub fn can_transfer_max_byte_num(&self) -> i32 {
        self.window.available_bytes()
    }

    pub fn add_byte_count_transferred(&self, count: i64) {
        self.window.record_transferred(count);
    }

    pub fn get_transferred_byte_in_second(&self) -> i64 {
        self.window.transferred_bytes_per_second()
    }

    pub fn max_transfer_byte_in_second(&self) -> usize {
        self.window.max_bytes_per_second()
    }
}

impl ServiceTask for FlowMonitorInner {
    fn get_service_name(&self) -> String {
        String::from("FlowMonitor")
    }

    async fn run(&self, context: &ServiceContext) {
        while !context.is_stopped() {
            context.wait_for_running(Duration::from_millis(1000)).await;
            self.calculate_speed();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn flow_monitor_starts_successfully() {
        let config = Arc::new(MessageStoreConfig::default());
        let monitor = FlowMonitor::new(config);
        monitor.start().await.expect("flow monitor should start");
    }

    #[tokio::test]
    async fn flow_monitor_shuts_down_successfully() {
        let config = Arc::new(MessageStoreConfig::default());
        let monitor = FlowMonitor::new(config);
        monitor.start().await.expect("flow monitor should start");
        monitor.shutdown().await;
    }

    #[test]
    fn calculate_speed_updates_transferred_byte_in_second() {
        let config = Arc::new(MessageStoreConfig::default());
        let inner = FlowMonitorInner::new(config);
        inner.add_byte_count_transferred(100);
        inner.calculate_speed();
        assert_eq!(inner.get_transferred_byte_in_second(), 100);
    }

    #[test]
    fn can_transfer_max_byte_num_returns_correct_value_when_flow_control_enabled() {
        let config = MessageStoreConfig {
            ha_flow_control_enable: true,
            max_ha_transfer_byte_in_second: 200,
            ..Default::default()
        };
        let inner = FlowMonitorInner::new(Arc::new(config));
        inner.add_byte_count_transferred(150);
        assert_eq!(inner.can_transfer_max_byte_num(), 50);
    }

    #[test]
    fn can_transfer_max_byte_num_returns_max_value_when_flow_control_disabled() {
        let config = MessageStoreConfig {
            ha_flow_control_enable: false,
            ..Default::default()
        };
        let inner = FlowMonitorInner::new(Arc::new(config));
        assert_eq!(inner.can_transfer_max_byte_num(), i32::MAX);
    }
}
