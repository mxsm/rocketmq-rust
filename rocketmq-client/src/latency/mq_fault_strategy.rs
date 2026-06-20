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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_rust::ArcMut;

use crate::base::client_config::ClientConfig;
use crate::latency::latency_fault_tolerance::LatencyFaultTolerance;
use crate::latency::latency_fault_tolerance_impl::LatencyFaultToleranceImpl;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultResolver;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultServiceDetector;
use crate::producer::producer_impl::queue_filter::QueueFilter;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;

pub struct MQFaultStrategy {
    latency_fault_tolerance: ArcMut<LatencyFaultToleranceImpl<DefaultResolver, DefaultServiceDetector>>,
    send_latency_fault_enable: AtomicBool,
    start_detector_enable: AtomicBool,
    latency_max: Vec<u64>,
    not_available_duration: Vec<u64>,
    reachable_filter: Box<dyn QueueFilter>,
    available_filter: Box<dyn QueueFilter>,
}

impl MQFaultStrategy {
    pub const DEFAULT_LATENCY_MAX: [u64; 7] = [50, 100, 550, 1800, 3000, 5000, 15000];
    pub const DEFAULT_NOT_AVAILABLE_DURATION: [u64; 7] = [0, 0, 2000, 5000, 6000, 10000, 30000];
    /// Latency penalty applied when a broker is isolated (10 seconds).
    const ISOLATION_LATENCY_MS: u64 = 10_000;

    pub fn new(client_config: &ClientConfig) -> Self {
        let mut tolerance_impl = LatencyFaultToleranceImpl::new();
        tolerance_impl.set_detect_interval(client_config.detect_interval);
        tolerance_impl.set_detect_timeout(client_config.detect_timeout);
        tolerance_impl.set_start_detector_enable(client_config.start_detector_enable);
        let latency_fault_tolerance = ArcMut::new(tolerance_impl);
        Self {
            latency_fault_tolerance: ArcMut::clone(&latency_fault_tolerance),
            send_latency_fault_enable: AtomicBool::new(client_config.send_latency_enable),
            start_detector_enable: AtomicBool::new(client_config.start_detector_enable),
            latency_max: Self::DEFAULT_LATENCY_MAX.to_vec(),
            not_available_duration: Self::DEFAULT_NOT_AVAILABLE_DURATION.to_vec(),
            reachable_filter: Box::new(ReachableFilter {
                latency_fault_tolerance: ArcMut::clone(&latency_fault_tolerance),
            }),
            available_filter: Box::new(AvailableFilter {
                latency_fault_tolerance,
            }),
        }
    }

    pub fn start_detector(&mut self) {
        LatencyFaultTolerance::start_detector(self.latency_fault_tolerance.clone());
    }

    pub(crate) fn set_resolve(&mut self, resolver: DefaultResolver) {
        self.latency_fault_tolerance.set_resolver(resolver);
    }

    pub(crate) fn set_service_detector(&mut self, service_detector: DefaultServiceDetector) {
        self.latency_fault_tolerance.set_service_detector(service_detector);
    }

    pub fn shutdown(&mut self) {
        self.latency_fault_tolerance.shutdown();
    }

    pub async fn shutdown_async(&mut self) -> bool {
        self.latency_fault_tolerance.shutdown_detector().await
    }

    pub fn is_start_detector_enable(&self) -> bool {
        self.start_detector_enable.load(Ordering::Relaxed)
    }

    pub fn set_start_detector_enable(&mut self, start_detector_enable: bool) {
        self.start_detector_enable
            .store(start_detector_enable, Ordering::Relaxed);
        self.latency_fault_tolerance
            .set_start_detector_enable(start_detector_enable);
    }

    pub fn is_send_latency_fault_enable(&self) -> bool {
        self.send_latency_fault_enable.load(Ordering::Relaxed)
    }

    pub fn select_one_message_queue(
        &self,
        tp_info: &TopicPublishInfo,
        last_broker_name: Option<&CheetahString>,
        reset_index: bool,
    ) -> Option<MessageQueue> {
        let broker_filter = BrokerFilter::with_last_broker_name(last_broker_name.cloned());

        if self.send_latency_fault_enable.load(Ordering::Relaxed) {
            if reset_index {
                tp_info.reset_index();
            }

            let filter = &[self.available_filter.as_ref(), &broker_filter as &dyn QueueFilter];
            if let Some(mq) = tp_info.select_one_message_queue_filters(filter) {
                return Some(mq);
            }

            let filter = &[self.reachable_filter.as_ref(), &broker_filter as &dyn QueueFilter];
            if let Some(mq) = tp_info.select_one_message_queue_filters(filter) {
                return Some(mq);
            }

            return tp_info.select_one_message_queue_filters(&[]);
        }

        if let Some(mq) = tp_info.select_one_message_queue_filters(&[&broker_filter]) {
            return Some(mq);
        }
        tp_info.select_one_message_queue_filters(&[])
    }

    pub fn get_latency_max(&self) -> &[u64] {
        &self.latency_max
    }

    pub fn set_latency_max(&mut self, latency_max: impl Into<Vec<u64>>) {
        self.latency_max = latency_max.into();
    }

    pub fn get_not_available_duration(&self) -> &[u64] {
        &self.not_available_duration
    }

    pub fn set_not_available_duration(&mut self, not_available_duration: impl Into<Vec<u64>>) {
        self.not_available_duration = not_available_duration.into();
    }

    pub async fn update_fault_item(
        &self,
        broker_name: CheetahString,
        current_latency: u64,
        isolation: bool,
        reachable: bool,
    ) {
        if self.send_latency_fault_enable.load(Ordering::Relaxed) {
            let effective_latency = if isolation {
                Self::ISOLATION_LATENCY_MS
            } else {
                current_latency
            };
            let duration = self.compute_not_available_duration(effective_latency);
            self.latency_fault_tolerance
                .update_fault_item(broker_name, current_latency, duration, reachable)
                .await;
        }
    }

    fn compute_not_available_duration(&self, current_latency: u64) -> u64 {
        let len = self.latency_max.len().min(self.not_available_duration.len());
        for i in (0..len).rev() {
            if current_latency >= self.latency_max[i] {
                return self.not_available_duration[i];
            }
        }
        0
    }

    #[inline]
    pub fn set_send_latency_fault_enable(&mut self, send_latency_fault_enable: bool) {
        self.send_latency_fault_enable
            .store(send_latency_fault_enable, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BrokerFilter {
    last_broker_name: Option<CheetahString>,
}

impl BrokerFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_last_broker_name(last_broker_name: Option<CheetahString>) -> Self {
        Self { last_broker_name }
    }

    pub fn last_broker_name(&self) -> Option<&CheetahString> {
        self.last_broker_name.as_ref()
    }

    pub fn get_last_broker_name(&self) -> Option<&CheetahString> {
        self.last_broker_name()
    }

    pub fn set_last_broker_name(&mut self, last_broker_name: impl Into<CheetahString>) {
        self.last_broker_name = Some(last_broker_name.into());
    }

    pub fn clear_last_broker_name(&mut self) {
        self.last_broker_name = None;
    }
}

impl QueueFilter for BrokerFilter {
    fn filter(&self, message_queue: &MessageQueue) -> bool {
        if let Some(last_broker_name) = &self.last_broker_name {
            message_queue.broker_name() != last_broker_name
        } else {
            true
        }
    }
}

struct ReachableFilter {
    latency_fault_tolerance: ArcMut<LatencyFaultToleranceImpl<DefaultResolver, DefaultServiceDetector>>,
}

impl QueueFilter for ReachableFilter {
    fn filter(&self, message_queue: &MessageQueue) -> bool {
        self.latency_fault_tolerance.is_reachable(message_queue.broker_name())
    }
}

struct AvailableFilter {
    latency_fault_tolerance: ArcMut<LatencyFaultToleranceImpl<DefaultResolver, DefaultServiceDetector>>,
}

impl QueueFilter for AvailableFilter {
    fn filter(&self, message_queue: &MessageQueue) -> bool {
        self.latency_fault_tolerance.is_available(message_queue.broker_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_filter_matches_java_last_broker_name_rule() {
        let mq = MessageQueue::from_parts("topic", "broker-a", 0);
        let mut filter = BrokerFilter::new();

        assert!(filter.filter(&mq));
        assert!(filter.get_last_broker_name().is_none());

        filter.set_last_broker_name("broker-a");
        assert_eq!(
            filter.get_last_broker_name(),
            Some(&CheetahString::from_static_str("broker-a"))
        );
        assert!(!filter.filter(&mq));

        filter.set_last_broker_name("broker-b");
        assert!(filter.filter(&mq));

        filter.clear_last_broker_name();
        assert!(filter.filter(&mq));
    }

    #[test]
    fn mq_fault_strategy_copies_detector_config_like_java_constructor() {
        let mut client_config = ClientConfig::default();
        client_config.set_detect_interval(12_345);
        client_config.set_detect_timeout(321);
        client_config.set_start_detector_enable(true);

        let strategy = MQFaultStrategy::new(&client_config);
        let (detect_interval, detect_timeout) = strategy.latency_fault_tolerance.detector_config_for_test();

        assert_eq!(detect_interval, 12_345);
        assert_eq!(detect_timeout, 321);
        assert!(strategy.is_start_detector_enable());
        assert!(strategy.latency_fault_tolerance.is_start_detector_enable());
    }
}
