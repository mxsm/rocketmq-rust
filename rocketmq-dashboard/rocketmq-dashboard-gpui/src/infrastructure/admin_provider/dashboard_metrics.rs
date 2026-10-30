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

use std::sync::Arc;

use super::{GpuiAdminProvider, ProviderFailure, query_for_revision, select_admin};

#[derive(Clone, PartialEq)]
pub(crate) struct SafeTopicRate {
    pub topic: String,
    pub produced_messages_24h: Option<u64>,
    pub produce_tps: Option<f64>,
    pub consume_tps: Option<f64>,
    pub failed_targets: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SafeConsumerSummary {
    pub group_count: Option<usize>,
    pub complete_backlog: Option<i64>,
    pub failed_targets: usize,
}

impl GpuiAdminProvider {
    pub async fn topic_rates(
        self: &Arc<Self>,
        revision: u64,
        topics: Vec<String>,
    ) -> Result<Vec<SafeTopicRate>, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dashboard-topic-rates", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.topic_rates(&topics))
                .await
                .map(|rates| {
                    rates
                        .into_iter()
                        .map(|rate| SafeTopicRate {
                            topic: rate.topic,
                            produced_messages_24h: rate.produced_messages_24h,
                            produce_tps: rate.produce_tps,
                            consume_tps: rate.consume_tps,
                            failed_targets: rate.failed_targets,
                        })
                        .collect()
                })
        })
        .await
    }

    pub async fn consumer_summary(self: &Arc<Self>, revision: u64) -> Result<SafeConsumerSummary, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dashboard-consumer-summary", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.consumer_summary())
                .await
                .map(|summary| SafeConsumerSummary {
                    group_count: summary.group_count,
                    complete_backlog: summary.complete_backlog,
                    failed_targets: summary.failed_targets,
                })
        })
        .await
    }
}
