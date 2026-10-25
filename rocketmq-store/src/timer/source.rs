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

use bytes::Bytes;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;

use crate::log_file::commit_log::CommitLogReadHandle;
use crate::timer::payload_cursor::TimerPayloadCursor;
use crate::timer::payload_cursor::TimerPayloadCursorViolation;

/// One Timer source frame as read from the CommitLog.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "a source batch holds each decoded message once; boxing would add an allocation per record"
)]
pub(crate) enum TimerSourceRead {
    /// The frame and its message, decoded without the body.
    Frame { bytes: Bytes, message: MessageExt },
    /// The locator is empty or negative, or the frame does not fit the byte budget.
    InvalidLocator,
    /// The CommitLog no longer holds the frame.
    Missing,
    /// The CommitLog returned fewer bytes than the locator declares.
    ShortRead,
    /// The bytes are not a supported message record.
    Undecodable,
}

/// Reads Timer source frames by `(physical offset, size)` locator, one result per locator.
///
/// Frames that follow each other in the CommitLog, as Timer consume queue entries usually do,
/// are read with one bulk read, and a frame inside one mapped file is a zero-copy slice. Messages
/// are decoded without their body: source stages index by properties and timestamps, and a stage
/// that copies the payload uses the raw frame. Frames past `max_bytes` in total are
/// `InvalidLocator`.
pub(crate) fn read_timer_source_frames(
    commit_log: &CommitLogReadHandle,
    locators: &[(i64, i32)],
    max_bytes: usize,
) -> Vec<TimerSourceRead> {
    let mut output = Vec::with_capacity(locators.len());
    let mut cursor = 0usize;
    let mut retained_bytes = 0usize;
    while cursor < locators.len() {
        let (start, first_size) = locators[cursor];
        let Ok(first_size) = usize::try_from(first_size) else {
            output.push(TimerSourceRead::InvalidLocator);
            cursor += 1;
            continue;
        };
        if start < 0 || first_size == 0 || retained_bytes.saturating_add(first_size) > max_bytes {
            output.push(TimerSourceRead::InvalidLocator);
            cursor += 1;
            continue;
        }

        let run_start = cursor;
        let mut run_bytes = first_size;
        let mut expected_next = start.saturating_add(first_size as i64);
        cursor += 1;
        while cursor < locators.len() {
            let (next_offset, next_size) = locators[cursor];
            let Ok(next_size) = usize::try_from(next_size) else {
                break;
            };
            if next_offset != expected_next
                || next_size == 0
                || retained_bytes.saturating_add(run_bytes).saturating_add(next_size) > max_bytes
                || run_bytes.saturating_add(next_size) > i32::MAX as usize
            {
                break;
            }
            run_bytes += next_size;
            expected_next = expected_next.saturating_add(next_size as i64);
            cursor += 1;
        }
        retained_bytes = retained_bytes.saturating_add(run_bytes);

        let Some(segments) = commit_log.get_bulk_data(start, run_bytes as i32) else {
            output.extend((run_start..cursor).map(|_| TimerSourceRead::Missing));
            continue;
        };
        let mut payload_cursor = TimerPayloadCursor::new(segments);
        if payload_cursor.remaining() != run_bytes {
            output.extend((run_start..cursor).map(|_| TimerSourceRead::ShortRead));
            continue;
        }
        for (_, size) in &locators[run_start..cursor] {
            output.push(match payload_cursor.take_frame(*size as usize) {
                Ok(bytes) => decode_frame(bytes),
                Err(TimerPayloadCursorViolation::InvalidFrameSize { .. }) => TimerSourceRead::InvalidLocator,
                Err(TimerPayloadCursorViolation::ShortRead { .. }) => TimerSourceRead::ShortRead,
            });
        }
    }
    output
}

fn decode_frame(bytes: Bytes) -> TimerSourceRead {
    let mut unread = bytes.clone();
    match MessageDecoder::decode(&mut unread, false, false, false, false, false) {
        Some(message) => TimerSourceRead::Frame { bytes, message },
        None => TimerSourceRead::Undecodable,
    }
}
