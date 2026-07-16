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

use crate::transfer::batch::TransferBatch;
use crate::transfer::batch::TransferPlan;
use crate::transfer::error::TransferError;
use crate::transfer::error::TransferResult;
use crate::transfer::segment::SegmentLease;

pub const DEFAULT_TRANSFER_BATCH_SIZE: usize = 256 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct TransferPlanInput {
    pub requested_offset: i64,
    pub next_transfer_offset: i64,
    pub max_commit_log_offset: i64,
    pub configured_max_batch_bytes: usize,
    pub flow_control_available_bytes: usize,
    pub mapped_file_size: usize,
    pub allow_cross_file_batch: bool,
    pub heartbeat_due: bool,
}

pub struct TransferPlanner;

impl TransferPlanner {
    pub fn plan<F>(input: TransferPlanInput, select_segments: F) -> TransferResult<TransferPlan>
    where
        F: FnOnce(i64, usize, bool) -> TransferResult<Vec<SegmentLease>>,
    {
        if input.mapped_file_size == 0 {
            return Err(TransferError::InvalidInput(
                "mapped_file_size must be greater than zero".to_string(),
            ));
        }

        let next_offset = Self::resolve_next_offset(input);
        if next_offset < 0 {
            return Ok(TransferPlan::NoData);
        }
        if next_offset >= input.max_commit_log_offset {
            return Ok(Self::no_data_or_heartbeat(input.heartbeat_due, next_offset));
        }

        let max_body_bytes = Self::max_body_bytes(input, next_offset);
        if max_body_bytes == 0 {
            return Ok(TransferPlan::NoData);
        }

        let segments = select_segments(next_offset, max_body_bytes, input.allow_cross_file_batch)?;
        if segments.is_empty() {
            return Ok(Self::no_data_or_heartbeat(input.heartbeat_due, next_offset));
        }

        let total_body_len = segments.iter().map(SegmentLease::len).sum::<usize>();
        if total_body_len == 0 {
            return Ok(TransferPlan::NoData);
        }
        if total_body_len > max_body_bytes {
            return Err(TransferError::SegmentSelection(format!(
                "selected {} bytes exceeds planned max {} bytes",
                total_body_len, max_body_bytes
            )));
        }

        Ok(TransferPlan::Data(TransferBatch::data(next_offset, segments)))
    }

    fn resolve_next_offset(input: TransferPlanInput) -> i64 {
        if input.next_transfer_offset != -1 {
            return input.next_transfer_offset;
        }
        if input.requested_offset == 0 {
            input.max_commit_log_offset - (input.max_commit_log_offset % input.mapped_file_size as i64)
        } else {
            input.requested_offset
        }
    }

    fn max_body_bytes(input: TransferPlanInput, next_offset: i64) -> usize {
        let configured = if input.configured_max_batch_bytes == 0 {
            DEFAULT_TRANSFER_BATCH_SIZE
        } else {
            input.configured_max_batch_bytes
        };
        let available = (input.max_commit_log_offset - next_offset).max(0) as usize;
        let mut max_body_bytes = configured.min(input.flow_control_available_bytes).min(available);
        if !input.allow_cross_file_batch {
            let position_in_file = next_offset.rem_euclid(input.mapped_file_size as i64) as usize;
            let remaining_in_file = input.mapped_file_size.saturating_sub(position_in_file);
            max_body_bytes = max_body_bytes.min(remaining_in_file);
        }
        max_body_bytes
    }

    fn no_data_or_heartbeat(heartbeat_due: bool, next_offset: i64) -> TransferPlan {
        if heartbeat_due {
            TransferPlan::Heartbeat { next_offset }
        } else {
            TransferPlan::NoData
        }
    }
}
