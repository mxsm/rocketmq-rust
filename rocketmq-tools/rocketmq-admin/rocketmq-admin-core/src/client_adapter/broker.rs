// Copyright 2026 The RocketMQ Rust Authors
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

use cheetah_string::CheetahString;
use rocketmq_client_rust::MQAdminExt;
use rocketmq_protocol::protocol::body::kv_table::KVTable;

use crate::client_adapter::lifecycle::AdminSession;
use crate::core::broker::BrokerAdmin;
use crate::core::broker::BrokerSummary;
use crate::core::broker::ListBrokersRequest;
use crate::core::broker::ListBrokersResult;
use crate::core::broker::ProbeBrokerRuntimeRequest;
use crate::core::broker::ProbeBrokerRuntimeResult;
use crate::core::AdminError;
use crate::core::AdminFuture;

impl BrokerAdmin for AdminSession {
    fn list_brokers<'a>(&'a mut self, request: &'a ListBrokersRequest) -> AdminFuture<'a, ListBrokersResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| AdminError::backend("examine_broker_cluster_info", error.to_string()))?;
            let broker_names = cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(request.cluster.as_str()))
                .cloned()
                .unwrap_or_default();
            let broker_table = cluster_info.broker_addr_table.unwrap_or_default();
            let now_millis = self.clock.now_millis();
            let mut brokers = Vec::new();
            for broker_name in broker_names {
                let Some(broker_data) = broker_table.get(&broker_name) else {
                    continue;
                };
                for (broker_id, broker_addr) in broker_data.broker_addrs() {
                    let runtime = self.inner.fetch_broker_runtime_stats(broker_addr.clone()).await.ok();
                    brokers.push(build_broker_summary(
                        request.cluster.clone(),
                        broker_name.to_string(),
                        *broker_id,
                        broker_addr.to_string(),
                        runtime.as_ref(),
                        now_millis,
                    ));
                }
            }
            brokers.sort_by(|left, right| {
                left.broker_name
                    .cmp(&right.broker_name)
                    .then(left.broker_id.cmp(&right.broker_id))
            });
            Ok(ListBrokersResult { brokers })
        })
    }

    fn probe_broker_runtime<'a>(
        &'a mut self,
        request: &'a ProbeBrokerRuntimeRequest,
    ) -> AdminFuture<'a, ProbeBrokerRuntimeResult> {
        Box::pin(async move {
            self.ensure_open()?;
            let cluster_info = self
                .inner
                .examine_broker_cluster_info()
                .await
                .map_err(|error| AdminError::backend("examine_broker_cluster_info", error.to_string()))?;
            let broker_names = cluster_info
                .cluster_addr_table
                .as_ref()
                .and_then(|table| table.get(request.cluster.as_str()))
                .cloned()
                .unwrap_or_default();
            let broker_table = cluster_info.broker_addr_table.unwrap_or_default();
            let mut result = ProbeBrokerRuntimeResult::default();
            for broker_name in broker_names {
                let Some(broker_data) = broker_table.get(&broker_name) else {
                    continue;
                };
                for broker_addr in broker_data.broker_addrs().values() {
                    result.attempted += 1;
                    if let Err(error) = self.inner.fetch_broker_runtime_stats(broker_addr.clone()).await {
                        result.failures.push(format!("{broker_addr}: {error}"));
                    }
                }
            }
            Ok(result)
        })
    }
}

fn build_broker_summary(
    cluster: String,
    broker_name: String,
    broker_id: u64,
    broker_addr: String,
    runtime: Option<&KVTable>,
    now_millis: u64,
) -> BrokerSummary {
    let in_tps = runtime_value(runtime, "putTps")
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);
    let out_tps = runtime_value(runtime, "getTransferredTps")
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);
    let send_queue = runtime_value(runtime, "sendThreadPoolQueueSize").unwrap_or_default();
    let pull_queue = runtime_value(runtime, "pullThreadPoolQueueSize").unwrap_or_default();
    let ack_queue = runtime_value(runtime, "ackThreadPoolQueueSize").unwrap_or("N");
    let send_wait = runtime_value(runtime, "sendThreadPoolQueueHeadWaitTimeMills").unwrap_or_default();
    let pull_wait = runtime_value(runtime, "pullThreadPoolQueueHeadWaitTimeMills").unwrap_or_default();
    let ack_wait = runtime_value(runtime, "ackThreadPoolQueueHeadWaitTimeMills").unwrap_or("N");
    let timer_read_behind = runtime_i64(runtime, "timerReadBehind");
    let timer_offset_behind = runtime_i64(runtime, "timerOffsetBehind");
    let timer_congest_num = runtime_i64(runtime, "timerCongestNum");
    let timer_enqueue_tps = runtime_f32(runtime, "timerEnqueueTps");
    let timer_dequeue_tps = runtime_f32(runtime, "timerDequeueTps");
    let hour = runtime_value(runtime, "earliestMessageTimeStamp")
        .and_then(|value| value.parse::<u64>().ok())
        .map(|timestamp| now_millis.saturating_sub(timestamp) as f64 / 3_600_000.0)
        .unwrap_or(0.0);
    let space = runtime_value(runtime, "commitLogDiskRatio")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0);

    BrokerSummary {
        cluster,
        broker_name,
        broker_id,
        broker_addr,
        version: runtime_value(runtime, "brokerVersionDesc")
            .unwrap_or_default()
            .to_string(),
        in_tps: format!("{:>9.2}({send_queue},{send_wait}ms)", in_tps),
        out_tps: format!("{:>9.2}({pull_queue},{pull_wait}ms|{ack_queue},{ack_wait}ms)", out_tps),
        timer_progress: format!(
            "{timer_read_behind}-{timer_offset_behind}({:.1}w, {timer_enqueue_tps:.1}, {timer_dequeue_tps:.1})",
            timer_congest_num as f32 / 10_000.0
        ),
        page_cache_lock_time_millis: runtime_value(runtime, "pageCacheLockTimeMills")
            .unwrap_or_default()
            .to_string(),
        hour: format!("{hour:.2}"),
        space: format!("{space:.4}"),
        broker_active: runtime_value(runtime, "brokerActive").is_some_and(|value| value == "true"),
    }
}

fn runtime_value<'a>(runtime: Option<&'a KVTable>, key: &str) -> Option<&'a str> {
    runtime
        .and_then(|runtime| runtime.table.get(&CheetahString::from(key)))
        .map(CheetahString::as_str)
}

fn runtime_i64(runtime: Option<&KVTable>, key: &str) -> i64 {
    runtime_value(runtime, key)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0)
}

fn runtime_f32(runtime: Option<&KVTable>, key: &str) -> f32 {
    runtime_value(runtime, key)
        .and_then(|value| value.parse::<f32>().ok())
        .unwrap_or(0.0)
}
