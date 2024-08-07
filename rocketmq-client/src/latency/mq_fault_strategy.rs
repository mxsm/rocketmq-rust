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
use crate::base::client_config::ClientConfig;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;

pub struct MQFaultStrategy {}

impl MQFaultStrategy {
    pub fn new(
        client_config: ClientConfig,
        /*fetcher: impl Resolver,
        service_detector: impl ServiceDetector,*/
    ) -> Self {
        Self {}
    }

    pub fn start_detector(&mut self) {}

    pub fn set_resolver(&mut self, resolver: impl Resolver) {}

    pub fn set_service_detector(&mut self, service_detector: impl ServiceDetector) {}

    pub fn is_start_detector_enable(&self) -> bool {
        unimplemented!("not implemented")
    }
}
