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

use std::time::Instant;

pub(crate) struct RequestMetricsGuard {
    start: Instant,
}

impl RequestMetricsGuard {
    #[inline]
    pub(crate) fn start(request_bytes: u64) -> Self {
        rocketmq_observability::metrics::remoting::record_requests_total(1);
        record_network_bytes(request_bytes);

        Self { start: Instant::now() }
    }
}

impl Drop for RequestMetricsGuard {
    fn drop(&mut self) {
        rocketmq_observability::metrics::remoting::record_request_latency(self.start.elapsed().as_millis() as u64);
    }
}

#[inline]
pub(crate) fn record_network_bytes(bytes: u64) {
    if bytes > 0 {
        rocketmq_observability::metrics::remoting::record_network_bytes(bytes);
    }
}
