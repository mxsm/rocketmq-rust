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

use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyResult;
use rocketmq_proxy_core::ResourceIdentity;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::cluster_admission::ClusterExecutionLanes;
use super::cluster_admission::ClusterExecutionPolicy;
use super::ClusterCommand;
use crate::config::ClusterConfig;
use crate::config::ClusterExecutionDiagnostics;

/// Key distribution used by the admission and retirement microbenchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterAdmissionPattern {
    SameKey,
    DistinctKeys,
}

/// Result of one synchronous keyed-admission probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterAdmissionProbe {
    pub command_count: usize,
    pub lane_count: usize,
    pub drained_count: usize,
    pub diagnostics: ClusterExecutionDiagnostics,
}

/// Exercises the real count/byte admission, exact-key registry, and lane retirement
/// implementation without network I/O.
pub fn run_cluster_admission_probe(
    command_count: usize,
    pattern: ClusterAdmissionPattern,
) -> ProxyResult<ClusterAdmissionProbe> {
    if command_count == 0 {
        return Err(ProxyError::Transport {
            message: "cluster admission benchmark requires at least one command".to_owned(),
        });
    }

    let control_reserve = 2;
    let policy = ClusterExecutionPolicy {
        capacity_count: command_count.saturating_add(control_reserve).saturating_add(1),
        capacity_bytes: command_count.saturating_mul(1_024).max(1 << 20),
        max_queue_age: Duration::from_secs(30),
        io_max_inflight: 16,
        control_reserve,
        lane_idle_timeout: Duration::from_secs(30),
    };
    let lanes = ClusterExecutionLanes::new(policy)?;
    let config = ClusterConfig::default();
    let mut registrations = Vec::with_capacity(match pattern {
        ClusterAdmissionPattern::SameKey => 1,
        ClusterAdmissionPattern::DistinctKeys => command_count,
    });

    for index in 0..command_count {
        let topic = match pattern {
            ClusterAdmissionPattern::SameKey => "TopicA".to_owned(),
            ClusterAdmissionPattern::DistinctKeys => format!("Topic{index}"),
        };
        let (reply, _receiver) = oneshot::channel();
        if let Some(registration) = lanes.enqueue(
            ClusterCommand::QueryRoute {
                topic: ResourceIdentity::new("", topic),
                reply,
            },
            CancellationToken::new(),
            &config,
        )? {
            registrations.push(registration);
        }
    }

    let lane_count = registrations.len();
    let mut drained_count = 0;
    for registration in &registrations {
        while registration.queue.try_pop().is_some() {
            drained_count += 1;
        }
        if !lanes.retire(registration) {
            return Err(ProxyError::Transport {
                message: "cluster admission benchmark failed to retire an empty lane".to_owned(),
            });
        }
    }
    let diagnostics = lanes.snapshot();
    Ok(ClusterAdmissionProbe {
        command_count,
        lane_count,
        drained_count,
        diagnostics,
    })
}
