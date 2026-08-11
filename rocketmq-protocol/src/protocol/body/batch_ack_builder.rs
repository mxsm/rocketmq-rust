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

use std::collections::BTreeMap;

use bitvec::prelude::BitVec;
use bitvec::prelude::Lsb0;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;

use crate::protocol::body::batch_ack::BatchAck;
use crate::protocol::body::batch_ack::SerializableBitVec;
use crate::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
use crate::protocol::header::extra_info_util::AckExtraInfo;
use crate::protocol::header::extra_info_util::ExtraInfoUtil;

pub const DEFAULT_MAX_BATCH_ACK_ENTRIES: usize = 4096;
pub const DEFAULT_MAX_BATCH_ACK_OFFSET_SPAN: usize = 1_048_576;

#[derive(Debug, Clone, Copy)]
pub struct BatchAckBuildLimits {
    pub max_entries: usize,
    pub max_offset_span: usize,
}

impl Default for BatchAckBuildLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_MAX_BATCH_ACK_ENTRIES,
            max_offset_span: DEFAULT_MAX_BATCH_ACK_OFFSET_SPAN,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BatchAckInput<'a> {
    pub entry_index: usize,
    pub consumer_group: &'a str,
    pub topic: &'a str,
    pub receipt_handle: &'a str,
}

#[derive(Debug)]
pub struct BatchAckBuildFailure {
    pub entry_index: usize,
    pub error: RocketMQError,
}

#[derive(Debug)]
pub struct BatchAckBrokerRequest {
    pub broker_name: CheetahString,
    pub body: BatchAckMessageRequestBody,
    pub entry_indexes: Vec<usize>,
}

#[derive(Debug, Default)]
pub struct BatchAckBuildResult {
    pub requests: Vec<BatchAckBrokerRequest>,
    pub failures: Vec<BatchAckBuildFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CheckpointKey {
    consumer_group: String,
    topic: String,
    retry: String,
    start_offset: i64,
    queue_id: i32,
    revive_queue_id: i32,
    pop_time: i64,
    invisible_time: i64,
}

struct PendingAck {
    ack: BatchAck,
    entry_indexes: Vec<usize>,
}

pub fn build_batch_ack_requests(inputs: &[BatchAckInput<'_>]) -> BatchAckBuildResult {
    build_batch_ack_requests_with_limits(inputs, BatchAckBuildLimits::default())
}

pub fn build_batch_ack_requests_with_limits(
    inputs: &[BatchAckInput<'_>],
    limits: BatchAckBuildLimits,
) -> BatchAckBuildResult {
    let mut result = BatchAckBuildResult::default();
    let mut brokers: BTreeMap<String, BTreeMap<CheckpointKey, PendingAck>> = BTreeMap::new();

    for (position, input) in inputs.iter().enumerate() {
        if position >= limits.max_entries {
            result.failures.push(BatchAckBuildFailure {
                entry_index: input.entry_index,
                error: RocketMQError::illegal_argument(format!(
                    "batch ACK input exceeds the configured {} entry limit",
                    limits.max_entries
                )),
            });
            continue;
        }

        match parse_input(*input, limits.max_offset_span) {
            Ok((extra, bit_index)) => {
                let key = CheckpointKey {
                    consumer_group: input.consumer_group.to_owned(),
                    topic: input.topic.to_owned(),
                    retry: extra.retry.to_owned(),
                    start_offset: extra.ck_queue_offset,
                    queue_id: extra.queue_id,
                    revive_queue_id: extra.revive_queue_id,
                    pop_time: extra.pop_time,
                    invisible_time: extra.invisible_time,
                };
                let pending = brokers
                    .entry(extra.broker_name.to_owned())
                    .or_default()
                    .entry(key)
                    .or_insert_with(|| PendingAck {
                        ack: BatchAck {
                            consumer_group: CheetahString::from(input.consumer_group),
                            topic: CheetahString::from(input.topic),
                            retry: CheetahString::from(extra.retry),
                            start_offset: extra.ck_queue_offset,
                            queue_id: extra.queue_id,
                            revive_queue_id: extra.revive_queue_id,
                            pop_time: extra.pop_time,
                            invisible_time: extra.invisible_time,
                            bit_set: SerializableBitVec(BitVec::<u64, Lsb0>::new()),
                        },
                        entry_indexes: Vec::new(),
                    });
                if pending.ack.bit_set.0.len() <= bit_index {
                    pending.ack.bit_set.0.resize(bit_index + 1, false);
                }
                pending.ack.bit_set.0.set(bit_index, true);
                pending.entry_indexes.push(input.entry_index);
            }
            Err(error) => result.failures.push(BatchAckBuildFailure {
                entry_index: input.entry_index,
                error,
            }),
        }
    }

    result.requests = brokers
        .into_iter()
        .map(|(broker_name, checkpoints)| {
            let mut acks = Vec::with_capacity(checkpoints.len());
            let mut entry_indexes = Vec::new();
            for pending in checkpoints.into_values() {
                acks.push(pending.ack);
                entry_indexes.extend(pending.entry_indexes);
            }
            let broker_name = CheetahString::from_string(broker_name);
            BatchAckBrokerRequest {
                body: BatchAckMessageRequestBody {
                    broker_name: broker_name.clone(),
                    acks,
                },
                broker_name,
                entry_indexes,
            }
        })
        .collect();
    result
}

fn parse_input<'a>(
    input: BatchAckInput<'a>,
    max_offset_span: usize,
) -> Result<(AckExtraInfo<'a>, usize), RocketMQError> {
    if input.consumer_group.trim().is_empty() {
        return Err(RocketMQError::illegal_argument("batch ACK consumer group is blank"));
    }
    if input.topic.trim().is_empty() {
        return Err(RocketMQError::illegal_argument("batch ACK topic is blank"));
    }

    let extra = ExtraInfoUtil::parse_ack_extra_info(input.receipt_handle)?;
    let delta = extra
        .queue_offset
        .checked_sub(extra.ck_queue_offset)
        .ok_or_else(|| RocketMQError::illegal_argument("batch ACK queue offset overflow"))?;
    let bit_index = usize::try_from(delta)
        .map_err(|_| RocketMQError::illegal_argument("batch ACK queue offset precedes checkpoint offset"))?;
    if bit_index >= max_offset_span {
        return Err(RocketMQError::illegal_argument(format!(
            "batch ACK offset span {bit_index} exceeds the configured {max_offset_span} bit limit"
        )));
    }
    Ok((extra, bit_index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::header::extra_info_util::ExtraInfoUtil;

    fn receipt(start: i64, broker: &str, queue_id: i32, offset: i64) -> String {
        ExtraInfoUtil::build_extra_info_with_offset(start, 100, 15_000, 0, "topic", broker, queue_id, offset)
    }

    #[test]
    fn thirty_two_offsets_share_one_broker_request_and_checkpoint_bitmap() {
        let receipts = (0..32)
            .map(|offset| receipt(10, "broker-a", 1, 10 + offset))
            .collect::<Vec<_>>();
        let inputs = receipts
            .iter()
            .enumerate()
            .map(|(entry_index, receipt_handle)| BatchAckInput {
                entry_index,
                consumer_group: "group",
                topic: "topic",
                receipt_handle,
            })
            .collect::<Vec<_>>();

        let built = build_batch_ack_requests(&inputs);
        assert!(built.failures.is_empty());
        assert_eq!(built.requests.len(), 1);
        assert_eq!(built.requests[0].body.acks.len(), 1);
        assert_eq!(built.requests[0].entry_indexes.len(), 32);
        assert!(built.requests[0].body.acks[0].bit_set.0.iter().by_vals().all(|bit| bit));
    }

    #[test]
    fn broker_and_checkpoint_identity_are_grouped_without_mixing() {
        let receipts = [
            receipt(10, "broker-a", 1, 10),
            receipt(20, "broker-a", 1, 20),
            receipt(10, "broker-b", 1, 11),
        ];
        let inputs = receipts
            .iter()
            .enumerate()
            .map(|(entry_index, receipt_handle)| BatchAckInput {
                entry_index,
                consumer_group: "group",
                topic: "topic",
                receipt_handle,
            })
            .collect::<Vec<_>>();

        let built = build_batch_ack_requests(&inputs);
        assert!(built.failures.is_empty());
        assert_eq!(built.requests.len(), 2);
        assert_eq!(built.requests[0].body.acks.len(), 2);
        assert_eq!(built.requests[1].body.acks.len(), 1);
    }

    #[test]
    fn malformed_and_huge_spans_are_failures_without_large_bitmaps() {
        let huge = receipt(0, "broker-a", 1, i64::MAX);
        let inputs = [
            BatchAckInput {
                entry_index: 3,
                consumer_group: "group",
                topic: "topic",
                receipt_handle: "bad",
            },
            BatchAckInput {
                entry_index: 7,
                consumer_group: "group",
                topic: "topic",
                receipt_handle: &huge,
            },
        ];

        let built = build_batch_ack_requests_with_limits(
            &inputs,
            BatchAckBuildLimits {
                max_entries: 32,
                max_offset_span: 64,
            },
        );
        assert!(built.requests.is_empty());
        assert_eq!(
            built
                .failures
                .iter()
                .map(|failure| failure.entry_index)
                .collect::<Vec<_>>(),
            vec![3, 7]
        );
    }

    #[test]
    fn duplicate_offsets_preserve_all_entry_indexes() {
        let receipt = receipt(10, "broker-a", 1, 10);
        let inputs = [
            BatchAckInput {
                entry_index: 1,
                consumer_group: "group",
                topic: "topic",
                receipt_handle: &receipt,
            },
            BatchAckInput {
                entry_index: 2,
                consumer_group: "group",
                topic: "topic",
                receipt_handle: &receipt,
            },
        ];
        let built = build_batch_ack_requests(&inputs);
        assert_eq!(built.requests[0].entry_indexes, vec![1, 2]);
        assert_eq!(built.requests[0].body.acks[0].bit_set.0.count_ones(), 1);
    }
}
