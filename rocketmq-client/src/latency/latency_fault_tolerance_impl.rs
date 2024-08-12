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
use std::collections::HashMap;

use crate::latency::latency_fault_tolerance::LatencyFaultTolerance;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;

pub struct LatencyFaultToleranceImpl {
    fault_item_table: parking_lot::Mutex<HashMap<String, FaultItem>>,
    detect_timeout: i32,
    detect_interval: i32,
    which_item_worst: ThreadLocalIndex,
    start_detector_enable: AtomicBool,
    resolver: Option<Box<dyn Resolver>>,
    service_detector: Option<Box<dyn ServiceDetector>>,
}

impl LatencyFaultToleranceImpl {
    pub fn new(/*fetcher: impl Resolver, service_detector: impl ServiceDetector*/) -> Self {
        Self {
            resolver: None,
            service_detector: None,
            fault_item_table: Default::default(),
            detect_timeout: 200,
            detect_interval: 2000,
            which_item_worst: Default::default(),
            start_detector_enable: AtomicBool::new(false),
        }
    }
}

impl LatencyFaultTolerance<String> for LatencyFaultToleranceImpl {
    fn update_fault_item(
        &mut self,
        name: String,
        current_latency: u64,
        not_available_duration: u64,
        reachable: bool,
    ) {
        let mut table = self.fault_item_table.lock();
        let fault_item = table
            .entry(name.clone())
            .or_insert_with(|| FaultItem::new(name.clone()));

        fault_item.set_current_latency(current_latency);
        fault_item.update_not_available_duration(not_available_duration);
        fault_item.set_reachable(reachable);

        if !reachable {
            info!(
                "{} is unreachable, it will not be used until it's reachable",
                name
            );
        }
    }

    fn is_available(&self, name: &String) -> bool {
        let fault_item_table = self.fault_item_table.lock();
        if let Some(fault_item) = fault_item_table.get(name) {
            return fault_item.is_available();
        }
        true
    }

    fn is_reachable(&self, name: &String) -> bool {
        todo!()
    }

    fn remove(&mut self, name: &String) {
        todo!()
    }

    fn pick_one_at_least(&self) -> String {
        todo!()
    }

    fn start_detector(&self) {
        todo!()
    }

    fn shutdown(&self) {}

    fn detect_by_one_round(&self) {
        todo!()
    }

    fn set_detect_timeout(&mut self, detect_timeout: u32) {
        todo!()
    }

    fn set_detect_interval(&mut self, detect_interval: u32) {
        todo!()
    }

    fn set_start_detector_enable(&mut self, start_detector_enable: bool) {
        self.start_detector_enable
            .store(start_detector_enable, std::sync::atomic::Ordering::Relaxed);
    }

    fn is_start_detector_enable(&self) -> bool {
        todo!()
    }

    fn set_resolver(&mut self, resolver: Box<dyn Resolver>) {
        self.resolver = Some(resolver);
    }

    fn set_service_detector(&mut self, service_detector: Box<dyn ServiceDetector>) {
        self.service_detector = Some(service_detector);
    }
}

use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;

use rocketmq_common::TimeUtils::get_current_millis;
use tracing::info;

use crate::common::thread_local_index::ThreadLocalIndex;

#[derive(Debug)]
pub struct FaultItem {
    name: String,
    current_latency: std::sync::atomic::AtomicU64,
    start_timestamp: std::sync::atomic::AtomicU64,
    check_stamp: std::sync::atomic::AtomicU64,
    reachable_flag: std::sync::atomic::AtomicBool,
}

impl FaultItem {
    pub fn new(name: String) -> Self {
        FaultItem {
            name,
            current_latency: std::sync::atomic::AtomicU64::new(0),
            start_timestamp: std::sync::atomic::AtomicU64::new(0),
            check_stamp: std::sync::atomic::AtomicU64::new(0),
            reachable_flag: std::sync::atomic::AtomicBool::new(true),
        }
    }

    pub fn update_not_available_duration(&self, not_available_duration: u64) {
        let now = get_current_millis();
        if not_available_duration > 0
            && now + not_available_duration
                > self
                    .start_timestamp
                    .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.start_timestamp.store(
                now + not_available_duration,
                std::sync::atomic::Ordering::Relaxed,
            );
            println!(
                "{} will be isolated for {} ms.",
                self.name, not_available_duration
            );
        }
    }

    pub fn set_reachable(&self, reachable_flag: bool) {
        self.reachable_flag
            .store(reachable_flag, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_check_stamp(&self, check_stamp: u64) {
        self.check_stamp
            .store(check_stamp, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn is_available(&self) -> bool {
        let now = get_current_millis();
        now >= self
            .start_timestamp
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn is_reachable(&self) -> bool {
        self.reachable_flag
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_current_latency(&self, latency: u64) {
        self.current_latency
            .store(latency, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_current_latency(&self) -> u64 {
        self.current_latency
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_start_timestamp(&self) -> u64 {
        self.start_timestamp
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Eq for FaultItem {}

impl Ord for FaultItem {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.is_available() != other.is_available() {
            if self.is_available() {
                return Ordering::Less;
            }
            if other.is_available() {
                return Ordering::Greater;
            }
        }

        match self
            .current_latency
            .load(std::sync::atomic::Ordering::Relaxed)
            .cmp(
                &other
                    .current_latency
                    .load(std::sync::atomic::Ordering::Relaxed),
            ) {
            Ordering::Equal => (),
            ord => return ord,
        }

        match self
            .start_timestamp
            .load(std::sync::atomic::Ordering::Relaxed)
            .cmp(
                &other
                    .start_timestamp
                    .load(std::sync::atomic::Ordering::Relaxed),
            ) {
            Ordering::Equal => (),
            ord => return ord,
        }

        Ordering::Equal
    }
}

impl PartialEq<Self> for FaultItem {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self
                .current_latency
                .load(std::sync::atomic::Ordering::Relaxed)
                == other
                    .current_latency
                    .load(std::sync::atomic::Ordering::Relaxed)
            && self
                .start_timestamp
                .load(std::sync::atomic::Ordering::Relaxed)
                == other
                    .start_timestamp
                    .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl PartialOrd for FaultItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for FaultItem {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.current_latency
            .load(std::sync::atomic::Ordering::Relaxed)
            .hash(state);
        self.start_timestamp
            .load(std::sync::atomic::Ordering::Relaxed)
            .hash(state);
    }
}

impl std::fmt::Display for FaultItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FaultItem{{ name='{}', current_latency={}, start_timestamp={}, reachable_flag={} }}",
            self.name,
            self.get_current_latency(),
            self.get_start_timestamp(),
            self.is_reachable()
        )
    }
}
