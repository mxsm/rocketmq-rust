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

use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use rocketmq_proxy_core::ProxyError;
use rocketmq_proxy_core::ProxyMessage;
use rocketmq_proxy_core::ProxyResult;
use rocketmq_proxy_core::ResourceIdentity;
use rocketmq_proxy_core::SendMessageEntry;
use rocketmq_proxy_core::SendMessageRequest;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::cluster_admission::ClusterExecutionLanes;
use super::cluster_admission::ClusterExecutionPolicy;
use super::ClusterCommand;
use crate::config::ClusterConfig;
use crate::config::ClusterExecutionDiagnostics;

/// Key distribution used only by the admission/retirement algorithm microbenchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterAdmissionMicroPattern {
    SameKey,
    DistinctKeys,
}

/// Result of one synchronous admission/retirement algorithm microprobe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterAdmissionMicroProbe {
    pub command_count: usize,
    pub lane_count: usize,
    pub drained_count: usize,
    pub diagnostics: ClusterExecutionDiagnostics,
}

/// Exercises count/byte admission, the exact-key registry, and retirement
/// without modelling blocked I/O or claiming production throughput.
pub fn run_cluster_admission_microprobe(
    command_count: usize,
    pattern: ClusterAdmissionMicroPattern,
) -> ProxyResult<ClusterAdmissionMicroProbe> {
    if command_count == 0 {
        return Err(ProxyError::Transport {
            message: "cluster admission microbenchmark requires at least one command".to_owned(),
        });
    }

    let control_reserve = 2;
    let policy = ClusterExecutionPolicy {
        capacity_count: command_count.saturating_add(control_reserve).saturating_add(1),
        capacity_bytes: command_count.saturating_mul(1_024).max(1 << 20),
        max_queue_age: Duration::from_secs(30),
        io_max_inflight: 16,
        control_reserve,
        long_poll_max_inflight: 256,
        lane_idle_timeout: Duration::from_secs(30),
    };
    let lanes = ClusterExecutionLanes::new(policy)?;
    let config = ClusterConfig::default();
    let mut registrations = Vec::with_capacity(match pattern {
        ClusterAdmissionMicroPattern::SameKey => 1,
        ClusterAdmissionMicroPattern::DistinctKeys => command_count,
    });

    for index in 0..command_count {
        let topic = match pattern {
            ClusterAdmissionMicroPattern::SameKey => "TopicA".to_owned(),
            ClusterAdmissionMicroPattern::DistinctKeys => format!("Topic{index}"),
        };
        if let Some(registration) = lanes.enqueue(query_route_command(&topic), CancellationToken::new(), &config)? {
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
                message: "cluster admission microbenchmark failed to retire an empty lane".to_owned(),
            });
        }
    }
    Ok(ClusterAdmissionMicroProbe {
        command_count,
        lane_count,
        drained_count,
        diagnostics: lanes.snapshot(),
    })
}

/// Result of the blocked-key mixed-execution probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMixedExecutionProbe {
    pub unrelated_command_count: usize,
    pub unrelated_operation_count: usize,
    pub lane_count: usize,
    pub drained_count: usize,
    pub unrelated_completion_latencies: Vec<Duration>,
    pub diagnostics: ClusterExecutionDiagnostics,
}

/// Exercises the production admission budget, exact-key registry, and queue
/// retirement while one key simulates blocked remote I/O.
///
/// The slow lane is processed by a dedicated worker. Unrelated lanes are
/// drained only after the worker confirms that the slow command entered its
/// blocking section, so their recorded completion latency measures head-of-line
/// isolation rather than raw lane construction cost.
pub fn run_cluster_mixed_execution_probe(
    rounds: usize,
    unrelated_commands_per_round: usize,
    unrelated_key_count: usize,
    messages_per_command: usize,
    message_size_bytes: usize,
    blocked_io_duration: Duration,
) -> ProxyResult<ClusterMixedExecutionProbe> {
    if rounds == 0
        || unrelated_commands_per_round == 0
        || unrelated_key_count == 0
        || messages_per_command == 0
        || message_size_bytes == 0
    {
        return Err(ProxyError::Transport {
            message: "cluster mixed-execution benchmark dimensions must be positive".to_owned(),
        });
    }
    if !unrelated_commands_per_round.is_multiple_of(unrelated_key_count) {
        return Err(ProxyError::Transport {
            message: "cluster mixed-execution commands must divide evenly across keys".to_owned(),
        });
    }
    if blocked_io_duration.is_zero() {
        return Err(ProxyError::Transport {
            message: "cluster mixed-execution blocked I/O duration must be positive".to_owned(),
        });
    }

    let control_reserve = 2;
    let measured_per_round = unrelated_commands_per_round.saturating_add(1);
    let retained_payload_per_round = unrelated_commands_per_round
        .saturating_mul(messages_per_command)
        .saturating_mul(message_size_bytes);
    let policy = ClusterExecutionPolicy {
        capacity_count: measured_per_round.saturating_add(control_reserve).saturating_add(1),
        capacity_bytes: retained_payload_per_round
            .saturating_add(measured_per_round.saturating_mul(4_096))
            .max(1 << 20),
        max_queue_age: Duration::from_secs(30),
        io_max_inflight: unrelated_key_count.saturating_add(control_reserve).max(3),
        control_reserve,
        long_poll_max_inflight: 256,
        lane_idle_timeout: Duration::from_secs(30),
    };
    let lanes = ClusterExecutionLanes::new(policy)?;
    let config = ClusterConfig::default();
    let slow_topic = "BlockedTopic";
    let slow_registration = register_lane(&lanes, &config, slow_topic)?;
    let unrelated_topics = (0..unrelated_key_count)
        .map(|index| format!("IndependentTopic{index}"))
        .collect::<Vec<_>>();
    let unrelated_registrations = unrelated_topics
        .iter()
        .map(|topic| register_send_lane(&lanes, &config, topic, messages_per_command, message_size_bytes))
        .collect::<ProxyResult<Vec<_>>>()?;
    let commands_per_key = unrelated_commands_per_round / unrelated_key_count;
    let unrelated_command_count =
        rounds
            .checked_mul(unrelated_commands_per_round)
            .ok_or_else(|| ProxyError::Transport {
                message: "cluster mixed-execution command count overflowed".to_owned(),
            })?;
    let unrelated_operation_count = unrelated_command_count
        .checked_mul(messages_per_command)
        .ok_or_else(|| ProxyError::Transport {
            message: "cluster mixed-execution operation count overflowed".to_owned(),
        })?;
    let (start_sender, start_receiver) = mpsc::sync_channel::<()>(0);
    let (entered_sender, entered_receiver) = mpsc::channel::<()>();
    let (finished_sender, finished_receiver) = mpsc::channel::<()>();
    let worker_registration = slow_registration.clone();

    let (latencies, drained_count) = thread::scope(|scope| {
        let worker = scope.spawn(move || -> Result<usize, String> {
            let mut drained = 0usize;
            while start_receiver.recv().is_ok() {
                let queued = worker_registration
                    .queue
                    .try_pop()
                    .ok_or_else(|| "slow Proxy lane did not contain its admitted command".to_owned())?;
                entered_sender
                    .send(())
                    .map_err(|_| "slow Proxy lane could not publish its entered signal".to_owned())?;
                thread::sleep(blocked_io_duration);
                drop(queued);
                drained = drained
                    .checked_add(1)
                    .ok_or_else(|| "slow Proxy lane drain count overflowed".to_owned())?;
                finished_sender
                    .send(())
                    .map_err(|_| "slow Proxy lane could not publish its completion signal".to_owned())?;
            }
            Ok(drained)
        });

        let measured = (|| -> ProxyResult<(Vec<Duration>, usize)> {
            let mut latencies = Vec::with_capacity(rounds);
            let mut drained = 0usize;
            for _ in 0..rounds {
                enqueue_existing_query(&lanes, &config, slow_topic)?;
                start_sender.send(()).map_err(|_| ProxyError::Transport {
                    message: "slow Proxy lane worker stopped before the round began".to_owned(),
                })?;
                entered_receiver.recv().map_err(|_| ProxyError::Transport {
                    message: "slow Proxy lane worker did not enter simulated remote I/O".to_owned(),
                })?;

                let started = Instant::now();
                for topic in &unrelated_topics {
                    for _ in 0..commands_per_key {
                        enqueue_existing_send(&lanes, &config, topic, messages_per_command, message_size_bytes)?;
                    }
                }
                for registration in &unrelated_registrations {
                    for _ in 0..commands_per_key {
                        let queued = registration.queue.try_pop().ok_or_else(|| ProxyError::Transport {
                            message: "unrelated Proxy lane omitted an admitted command".to_owned(),
                        })?;
                        drop(queued);
                        drained = drained.checked_add(1).ok_or_else(|| ProxyError::Transport {
                            message: "unrelated Proxy lane drain count overflowed".to_owned(),
                        })?;
                    }
                    if registration.queue.try_pop().is_some() {
                        return Err(ProxyError::Transport {
                            message: "unrelated Proxy lane retained extra commands".to_owned(),
                        });
                    }
                }
                latencies.push(started.elapsed());
                finished_receiver.recv().map_err(|_| ProxyError::Transport {
                    message: "slow Proxy lane worker did not complete its round".to_owned(),
                })?;
            }
            Ok((latencies, drained))
        })();

        drop(start_sender);
        let worker_result = worker.join().map_err(|_| ProxyError::Transport {
            message: "slow Proxy lane worker panicked".to_owned(),
        });
        let (latencies, unrelated_drained) = measured?;
        let slow_drained = worker_result?.map_err(|message| ProxyError::Transport { message })?;
        Ok::<_, ProxyError>((latencies, unrelated_drained.saturating_add(slow_drained)))
    })?;

    if !lanes.retire(&slow_registration) {
        return Err(ProxyError::Transport {
            message: "cluster mixed-execution benchmark failed to retire the slow lane".to_owned(),
        });
    }
    for registration in &unrelated_registrations {
        if !lanes.retire(registration) {
            return Err(ProxyError::Transport {
                message: "cluster mixed-execution benchmark failed to retire an unrelated lane".to_owned(),
            });
        }
    }
    let diagnostics = lanes.snapshot();
    Ok(ClusterMixedExecutionProbe {
        unrelated_command_count,
        unrelated_operation_count,
        lane_count: unrelated_key_count.saturating_add(1),
        drained_count,
        unrelated_completion_latencies: latencies,
        diagnostics,
    })
}

fn register_lane(
    lanes: &ClusterExecutionLanes,
    config: &ClusterConfig,
    topic: &str,
) -> ProxyResult<super::cluster_admission::ClusterLaneRegistration> {
    let registration = lanes
        .enqueue(query_route_command(topic), CancellationToken::new(), config)?
        .ok_or_else(|| ProxyError::Transport {
            message: format!("cluster mixed-execution benchmark did not create lane for {topic}"),
        })?;
    let queued = registration.queue.try_pop().ok_or_else(|| ProxyError::Transport {
        message: format!("cluster mixed-execution benchmark omitted setup command for {topic}"),
    })?;
    drop(queued);
    Ok(registration)
}

fn register_send_lane(
    lanes: &ClusterExecutionLanes,
    config: &ClusterConfig,
    producer_key: &str,
    messages_per_command: usize,
    message_size_bytes: usize,
) -> ProxyResult<super::cluster_admission::ClusterLaneRegistration> {
    let registration = lanes
        .enqueue(
            send_message_command(producer_key, messages_per_command, message_size_bytes),
            CancellationToken::new(),
            config,
        )?
        .ok_or_else(|| ProxyError::Transport {
            message: format!("cluster mixed-execution benchmark did not create send lane for {producer_key}"),
        })?;
    let queued = registration.queue.try_pop().ok_or_else(|| ProxyError::Transport {
        message: format!("cluster mixed-execution benchmark omitted setup send for {producer_key}"),
    })?;
    drop(queued);
    Ok(registration)
}

fn enqueue_existing_query(lanes: &ClusterExecutionLanes, config: &ClusterConfig, topic: &str) -> ProxyResult<()> {
    if lanes
        .enqueue(query_route_command(topic), CancellationToken::new(), config)?
        .is_some()
    {
        return Err(ProxyError::Transport {
            message: format!("cluster mixed-execution benchmark unexpectedly recreated query lane for {topic}"),
        });
    }
    Ok(())
}

fn enqueue_existing_send(
    lanes: &ClusterExecutionLanes,
    config: &ClusterConfig,
    producer_key: &str,
    messages_per_command: usize,
    message_size_bytes: usize,
) -> ProxyResult<()> {
    if lanes
        .enqueue(
            send_message_command(producer_key, messages_per_command, message_size_bytes),
            CancellationToken::new(),
            config,
        )?
        .is_some()
    {
        return Err(ProxyError::Transport {
            message: format!("cluster mixed-execution benchmark unexpectedly recreated send lane for {producer_key}"),
        });
    }
    Ok(())
}

fn query_route_command(topic: &str) -> ClusterCommand {
    let (reply, _receiver) = oneshot::channel();
    ClusterCommand::QueryRoute {
        topic: ResourceIdentity::new("", topic),
        reply,
    }
}

fn send_message_command(producer_key: &str, messages_per_command: usize, message_size_bytes: usize) -> ClusterCommand {
    let messages = (0..messages_per_command)
        .map(|index| {
            let topic = ResourceIdentity::new("", "ArchitectureProxyPerformance");
            SendMessageEntry {
                topic: topic.clone(),
                client_message_id: format!("{producer_key}-{index}"),
                message: ProxyMessage::new(topic.name(), vec![0x5a; message_size_bytes]),
                queue_id: None,
            }
        })
        .collect();
    let (reply, _receiver) = oneshot::channel();
    ClusterCommand::SendMessage {
        request: SendMessageRequest {
            messages,
            timeout: Some(Duration::from_secs(30)),
        },
        client_id: Some(producer_key.to_owned()),
        request_id: format!("benchmark-{producer_key}"),
        reply,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrelated_keys_complete_before_the_blocked_lane_releases() {
        let blocked = Duration::from_millis(50);
        let probe = run_cluster_mixed_execution_probe(2, 8, 2, 4, 16, blocked).expect("mixed execution probe");

        assert_eq!(probe.unrelated_command_count, 16);
        assert_eq!(probe.unrelated_operation_count, 64);
        assert_eq!(probe.lane_count, 3);
        assert_eq!(probe.drained_count, 18);
        assert!(
            probe
                .unrelated_completion_latencies
                .iter()
                .all(|latency| *latency < blocked),
            "unrelated lanes must finish while the slow lane is blocked"
        );
        assert_eq!(probe.diagnostics.active_keys, 0);
        assert_eq!(probe.diagnostics.queued_and_active, 0);
        assert_eq!(probe.diagnostics.retained_bytes, 0);
        assert_eq!(probe.diagnostics.rejected, 0);
    }
}
