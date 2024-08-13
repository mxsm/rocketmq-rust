/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::cell::RefCell;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use rocketmq_common::common::message::message_queue::MessageQueue;

use crate::base::client_config::ClientConfig;
use crate::latency::latency_fault_tolerance::LatencyFaultTolerance;
use crate::latency::latency_fault_tolerance_impl::LatencyFaultToleranceImpl;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;
use crate::producer::producer_impl::queue_filter::QueueFilter;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;

thread_local! {
    static THREAD_BROKER_FILTER: RefCell<BrokerFilter> = RefCell::new(BrokerFilter::default());
}

pub struct MQFaultStrategy {
    latency_fault_tolerance: Arc<Mutex<dyn LatencyFaultTolerance<String>>>,
    send_latency_fault_enable: AtomicBool,
    start_detector_enable: AtomicBool,
    latency_max: &'static [u64],
    not_available_duration: &'static [u64],
    reachable_filter: Box<dyn QueueFilter>,
    available_filter: Box<dyn QueueFilter>,
}

impl MQFaultStrategy {
    pub fn new(client_config: &ClientConfig) -> Self {
        let mut tolerance_impl = LatencyFaultToleranceImpl::new();
        tolerance_impl.set_start_detector_enable(client_config.start_detector_enable);
        let latency_fault_tolerance = Arc::new(Mutex::new(tolerance_impl));
        Self {
            latency_fault_tolerance: latency_fault_tolerance.clone(),
            send_latency_fault_enable: AtomicBool::new(client_config.send_latency_enable),
            start_detector_enable: AtomicBool::new(client_config.start_detector_enable),
            latency_max: &[50, 100, 550, 1800, 3000, 5000, 15000],
            not_available_duration: &[0, 0, 2000, 5000, 6000, 10000, 30000],
            reachable_filter: Box::new(ReachableFilter {
                latency_fault_tolerance: latency_fault_tolerance.clone(),
            }),
            available_filter: Box::new(AvailableFilter {
                latency_fault_tolerance,
            }),
        }
    }

    pub fn start_detector(&mut self) {}

    pub fn set_resolver(&mut self, resolver: Box<dyn Resolver>) {
        let mut tolerance = self.latency_fault_tolerance.lock();
        tolerance.set_resolver(resolver);
    }

    pub fn set_service_detector(&mut self, service_detector: Box<dyn ServiceDetector>) {
        let mut tolerance = self.latency_fault_tolerance.lock();
        tolerance.set_service_detector(service_detector);
    }

    pub fn is_start_detector_enable(&self) -> bool {
        unimplemented!("not implemented")
    }

    pub fn select_one_message_queue(
        &self,
        tp_info: &TopicPublishInfo,
        last_broker_name: Option<&str>,
        reset_index: bool,
    ) -> Option<MessageQueue> {
        THREAD_BROKER_FILTER.with(|filer| {
            filer.borrow_mut().last_broker_name = last_broker_name.map(|s| s.to_string());
        });
        if self.send_latency_fault_enable.load(Ordering::Relaxed) {
            if reset_index {
                tp_info.reset_index();
            }
            let broker_filter = THREAD_BROKER_FILTER.with_borrow(|f| f.clone());
            let filter = &[self.available_filter.as_ref(), &broker_filter];
            let mut mq = tp_info.select_one_message_queue(filter);
            if mq.is_some() {
                return mq;
            }
            let filter = &[self.reachable_filter.as_ref(), &broker_filter];
            mq = tp_info.select_one_message_queue(filter);
            if mq.is_some() {
                return mq;
            }
            return tp_info.select_one_message_queue(&[]);
        }
        let broker_filter = THREAD_BROKER_FILTER.with_borrow(|f| f.clone());
        let mq = tp_info.select_one_message_queue(&[&broker_filter]);
        if mq.is_some() {
            return mq;
        }
        tp_info.select_one_message_queue(&[])
    }

    pub fn get_latency_max(&self) -> &'static [u64] {
        self.latency_max
    }

    pub fn get_not_available_duration(&self) -> &'static [u64] {
        self.not_available_duration
    }

    pub fn update_fault_item(
        &self,
        broker_name: &str,
        current_latency: u64,
        isolation: bool,
        reachable: bool,
    ) {
        if self.send_latency_fault_enable.load(Ordering::Relaxed) {
            let duration = self.compute_not_available_duration(if isolation {
                10000
            } else {
                current_latency
            });
            self.latency_fault_tolerance.lock().update_fault_item(
                broker_name.to_string(),
                current_latency,
                duration,
                reachable,
            );
        }
    }

    fn compute_not_available_duration(&self, current_latency: u64) -> u64 {
        for i in (0..self.latency_max.len()).rev() {
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

#[derive(Default, Clone)]
struct BrokerFilter {
    last_broker_name: Option<String>,
}

impl QueueFilter for BrokerFilter {
    fn filter(&self, message_queue: &MessageQueue) -> bool {
        if let Some(last_broker_name) = &self.last_broker_name {
            message_queue.get_broker_name() != last_broker_name
        } else {
            true
        }
    }
}

struct ReachableFilter {
    latency_fault_tolerance: Arc<Mutex<dyn LatencyFaultTolerance<String>>>,
}

impl QueueFilter for ReachableFilter {
    fn filter(&self, message_queue: &MessageQueue) -> bool {
        let tolerance = self.latency_fault_tolerance.lock();
        tolerance.is_reachable(&message_queue.get_broker_name().to_string())
    }
}

struct AvailableFilter {
    latency_fault_tolerance: Arc<Mutex<dyn LatencyFaultTolerance<String>>>,
}

impl QueueFilter for AvailableFilter {
    fn filter(&self, message_queue: &MessageQueue) -> bool {
        let tolerance = self.latency_fault_tolerance.lock();
        tolerance.is_available(&message_queue.get_broker_name().to_string())
    }
}
