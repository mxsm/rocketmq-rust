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
use crate::latency::latency_fault_tolerance::LatencyFaultTolerance;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;

pub struct LatencyFaultToleranceImpl {
    resolver: Box<dyn Resolver>,
    service_detector: Box<dyn ServiceDetector>,
}

impl LatencyFaultToleranceImpl {
    pub fn new(fetcher: impl Resolver, service_detector: impl ServiceDetector) -> Self {
        Self {
            resolver: Box::new(fetcher),
            service_detector: Box::new(service_detector),
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
        todo!()
    }

    fn is_available(&self, name: &String) -> bool {
        todo!()
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

    fn shutdown(&self) {
        todo!()
    }

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
        todo!()
    }

    fn is_start_detector_enable(&self) -> bool {
        todo!()
    }
}
