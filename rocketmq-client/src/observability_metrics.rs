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

pub(crate) fn record_send(elapsed: Duration) {
    #[cfg(feature = "observability-metrics")]
    {
        rocketmq_observability::metrics::client::record_send_total(1);
        rocketmq_observability::metrics::client::record_send_latency(duration_millis_u64(elapsed));
    }

    #[cfg(not(feature = "observability-metrics"))]
    let _ = elapsed;
}

pub(crate) fn record_rebalance() {
    #[cfg(feature = "observability-metrics")]
    rocketmq_observability::metrics::client::record_rebalance_total(1);
}

pub(crate) fn record_consume(message_count: usize, latency_ms: u64) {
    #[cfg(feature = "observability-metrics")]
    {
        rocketmq_observability::metrics::client::record_consume_total(message_count as u64);
        rocketmq_observability::metrics::client::record_consume_latency(latency_ms);
    }

    #[cfg(not(feature = "observability-metrics"))]
    let _ = (message_count, latency_ms);
}

#[cfg(feature = "observability-metrics")]
#[inline]
fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().clamp(0, u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn metrics_helpers_are_noops_without_global_meter() {
        record_send(Duration::from_millis(1));
        record_rebalance();
        record_consume(1, 2);
    }
}
