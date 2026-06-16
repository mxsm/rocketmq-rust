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

use std::time::Duration;

pub(crate) fn record_route_request(elapsed: Duration) {
    #[cfg(feature = "observability")]
    {
        rocketmq_observability::metrics::namesrv::record_route_request_total(1);
        rocketmq_observability::metrics::namesrv::record_route_request_latency(duration_millis_u64(elapsed));
    }

    #[cfg(not(feature = "observability"))]
    let _ = elapsed;
}

pub(crate) fn record_broker_registration(active_brokers: usize) {
    #[cfg(feature = "observability")]
    {
        rocketmq_observability::metrics::namesrv::record_broker_registrations(1);
        rocketmq_observability::metrics::namesrv::record_active_brokers(active_brokers as u64);
    }

    #[cfg(not(feature = "observability"))]
    let _ = active_brokers;
}

pub(crate) fn record_active_brokers(active_brokers: usize) {
    #[cfg(feature = "observability")]
    rocketmq_observability::metrics::namesrv::record_active_brokers(active_brokers as u64);

    #[cfg(not(feature = "observability"))]
    let _ = active_brokers;
}

#[cfg(feature = "observability")]
#[inline]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().clamp(0, u128::from(u64::MAX)) as u64
}
