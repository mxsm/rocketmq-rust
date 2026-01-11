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

use std::any::Any;
use std::collections::HashSet;

use dashmap::DashMap;

use crate::latency::latency_fault_tolerance::LatencyFaultTolerance;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;

pub struct LatencyFaultToleranceImpl<R, S> {
    fault_item_table: DashMap<CheetahString, FaultItem>,
    detect_timeout: u32,
    detect_interval: u32,
    start_detector_enable: AtomicBool,
    resolver: Option<R>,
    service_detector: Option<S>,
}

impl<R, S> LatencyFaultToleranceImpl<R, S> {
    pub fn new(/*fetcher: impl Resolver, service_detector: impl ServiceDetector*/) -> Self {
        Self {
            resolver: None,
            service_detector: None,
            fault_item_table: Default::default(),
            detect_timeout: 200,
            detect_interval: 2000,
            start_detector_enable: AtomicBool::new(false),
        }
    }
}

impl<R, S> LatencyFaultTolerance<CheetahString, R, S> for LatencyFaultToleranceImpl<R, S>
where
    R: Resolver,
    S: ServiceDetector,
{
    async fn update_fault_item(
        &mut self,
        name: CheetahString,
        current_latency: u64,
        not_available_duration: u64,
        reachable: bool,
    ) {
        let entry = self
            .fault_item_table
            .entry(name.clone())
            .or_insert_with(|| FaultItem::new(name.clone()));

        entry.set_current_latency(current_latency);
        entry.update_not_available_duration(not_available_duration);
        entry.set_reachable(reachable);

        if !reachable {
            info!("{} is unreachable, it will not be used until it's reachable", name);
        }
    }

    fn is_available(&self, name: &CheetahString) -> bool {
        if let Some(fault_item) = self.fault_item_table.get(name) {
            return fault_item.is_available();
        }
        true
    }

    fn is_reachable(&self, name: &CheetahString) -> bool {
        if let Some(fault_item) = self.fault_item_table.get(name) {
            return fault_item.is_reachable();
        }
        true
    }

    async fn remove(&mut self, name: &CheetahString) {
        self.fault_item_table.remove(name);
    }

    async fn pick_one_at_least(&self) -> Option<CheetahString> {
        let mut reachable_names: Vec<CheetahString> = Vec::new();
        for entry in self.fault_item_table.iter() {
            if entry.value().reachable_flag.load(std::sync::atomic::Ordering::Acquire) {
                reachable_names.push(entry.key().clone());
            }
        }

        if !reachable_names.is_empty() {
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();
            reachable_names.shuffle(&mut rng);
            return Some(reachable_names[0].clone());
        }
        None
    }

    fn start_detector(this: ArcMut<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                if !this.start_detector_enable.load(std::sync::atomic::Ordering::Relaxed) {
                    continue;
                }

                this.detect_by_one_round().await;
            }
        });
    }

    fn shutdown(&self) {}

    async fn detect_by_one_round(&self) {
        let mut remove_set = HashSet::new();
        for entry in self.fault_item_table.iter() {
            let (name, fault_item) = (entry.key(), entry.value());
            if get_current_millis() as i64 - (fault_item.check_stamp.load(std::sync::atomic::Ordering::Relaxed) as i64)
                < 0
            {
                continue;
            }
            fault_item.check_stamp.store(
                get_current_millis() + self.detect_interval as u64,
                std::sync::atomic::Ordering::Release,
            );
            let broker_addr = self.resolver.as_ref().unwrap().resolve(fault_item.name.as_ref()).await;
            if broker_addr.is_none() {
                remove_set.insert(name.clone());
                continue;
            }
            if self.service_detector.is_none() {
                continue;
            }
            let service_ok = self
                .service_detector
                .as_ref()
                .unwrap()
                .detect(broker_addr.unwrap().as_str(), self.detect_timeout as u64)
                .await;
            if service_ok && fault_item.reachable_flag.load(std::sync::atomic::Ordering::Acquire) {
                info!("{} is reachable now, then it can be used.", name);
                fault_item
                    .reachable_flag
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        for name in remove_set {
            self.fault_item_table.remove(&name);
        }
    }

    fn set_detect_timeout(&mut self, detect_timeout: u32) {
        self.detect_timeout = detect_timeout;
    }

    fn set_detect_interval(&mut self, detect_interval: u32) {
        self.detect_interval = detect_interval;
    }

    fn set_start_detector_enable(&mut self, start_detector_enable: bool) {
        self.start_detector_enable
            .store(start_detector_enable, std::sync::atomic::Ordering::Relaxed);
    }

    fn is_start_detector_enable(&self) -> bool {
        self.start_detector_enable.load(std::sync::atomic::Ordering::Acquire)
    }

    fn set_resolver(&mut self, resolver: R) {
        self.resolver = Some(resolver);
    }

    fn set_service_detector(&mut self, service_detector: S) {
        self.service_detector = Some(service_detector);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;

use cheetah_string::CheetahString;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::ArcMut;
use tracing::info;

#[derive(Debug)]
pub struct FaultItem {
    name: CheetahString,
    current_latency: std::sync::atomic::AtomicU64,
    start_timestamp: std::sync::atomic::AtomicU64,
    check_stamp: std::sync::atomic::AtomicU64,
    reachable_flag: std::sync::atomic::AtomicBool,
}

impl FaultItem {
    pub fn new(name: CheetahString) -> Self {
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
            && now + not_available_duration > self.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
        {
            self.start_timestamp
                .store(now + not_available_duration, std::sync::atomic::Ordering::Relaxed);
            info!("{} will be isolated for {} ms.", self.name, not_available_duration);
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
        now >= self.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn is_reachable(&self) -> bool {
        self.reachable_flag.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_current_latency(&self, latency: u64) {
        self.current_latency
            .store(latency, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_current_latency(&self) -> u64 {
        self.current_latency.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_start_timestamp(&self) -> u64 {
        self.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
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
            .cmp(&other.current_latency.load(std::sync::atomic::Ordering::Relaxed))
        {
            Ordering::Equal => (),
            ord => return ord,
        }

        match self
            .start_timestamp
            .load(std::sync::atomic::Ordering::Relaxed)
            .cmp(&other.start_timestamp.load(std::sync::atomic::Ordering::Relaxed))
        {
            Ordering::Equal => (),
            ord => return ord,
        }

        Ordering::Equal
    }
}

impl PartialEq<Self> for FaultItem {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.current_latency.load(std::sync::atomic::Ordering::Relaxed)
                == other.current_latency.load(std::sync::atomic::Ordering::Relaxed)
            && self.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
                == other.start_timestamp.load(std::sync::atomic::Ordering::Relaxed)
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
