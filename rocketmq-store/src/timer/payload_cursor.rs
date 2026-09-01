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

use std::collections::VecDeque;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

use crate::base::select_result::SelectMappedBufferResult;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimerPayloadCursorViolation {
    InvalidFrameSize {
        cursor: usize,
        requested: usize,
        remaining: usize,
    },
    ShortRead {
        cursor: usize,
        requested: usize,
        remaining: usize,
    },
}

/// Sequential frame reader over owner-backed CommitLog selections.
///
/// A frame wholly contained in one mapped range becomes a zero-copy `Bytes`
/// slice. Only a frame that crosses a mapped-file boundary receives an exact,
/// message-sized scratch allocation.
pub(crate) struct TimerPayloadCursor {
    segments: VecDeque<Bytes>,
    segment_position: usize,
    remaining: usize,
    cursor: usize,
    copied_bytes: usize,
}

impl TimerPayloadCursor {
    pub(crate) fn new(segments: Vec<SelectMappedBufferResult>) -> Self {
        let segments = segments
            .into_iter()
            .map(SelectMappedBufferResult::into_owner_bytes)
            .collect::<VecDeque<_>>();
        let remaining = segments.iter().map(Bytes::len).sum();
        Self {
            segments,
            segment_position: 0,
            remaining,
            cursor: 0,
            copied_bytes: 0,
        }
    }

    pub(crate) const fn remaining(&self) -> usize {
        self.remaining
    }

    pub(crate) fn take_frame(&mut self, size: usize) -> Result<Bytes, TimerPayloadCursorViolation> {
        if size == 0 {
            return Err(self.violation(true, size));
        }
        if size > self.remaining {
            return Err(self.violation(false, size));
        }
        self.discard_empty_segments();

        let available = self
            .segments
            .front()
            .map(|segment| segment.len().saturating_sub(self.segment_position))
            .unwrap_or_default();
        if available >= size {
            let start = self.segment_position;
            let end = start + size;
            let Some(segment) = self.segments.front() else {
                return Err(self.violation(false, size));
            };
            let frame = segment.slice(start..end);
            self.segment_position = end;
            self.remaining -= size;
            self.cursor += size;
            self.discard_empty_segments();
            return Ok(frame);
        }

        let mut frame = BytesMut::with_capacity(size);
        while frame.len() < size {
            self.discard_empty_segments();
            let Some(segment) = self.segments.front() else {
                return Err(self.violation(false, size));
            };
            let needed = size - frame.len();
            let available = segment.len().saturating_sub(self.segment_position);
            let take = available.min(needed);
            frame.put_slice(&segment[self.segment_position..self.segment_position + take]);
            self.segment_position += take;
        }
        self.remaining -= size;
        self.cursor += size;
        self.copied_bytes += size;
        self.discard_empty_segments();
        Ok(frame.freeze())
    }

    fn violation(&self, invalid_size: bool, requested: usize) -> TimerPayloadCursorViolation {
        if invalid_size {
            TimerPayloadCursorViolation::InvalidFrameSize {
                cursor: self.cursor,
                requested,
                remaining: self.remaining,
            }
        } else {
            TimerPayloadCursorViolation::ShortRead {
                cursor: self.cursor,
                requested,
                remaining: self.remaining,
            }
        }
    }

    fn discard_empty_segments(&mut self) {
        while self
            .segments
            .front()
            .is_some_and(|segment| self.segment_position == segment.len())
        {
            self.segments.pop_front();
            self.segment_position = 0;
        }
    }

    #[cfg(test)]
    pub(crate) const fn copied_bytes(&self) -> usize {
        self.copied_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection(start_offset: u64, bytes: Bytes) -> SelectMappedBufferResult {
        SelectMappedBufferResult::from_bytes(start_offset, bytes).expect("test selection must fit")
    }

    #[test]
    fn frame_inside_one_segment_keeps_owner_without_copy() {
        let source = Bytes::from_static(b"abcdefgh");
        let source_ptr = source.as_ptr();
        let mut cursor = TimerPayloadCursor::new(vec![selection(0, source)]);

        let frame = cursor.take_frame(4).expect("read first frame");

        assert_eq!(frame.as_ref(), b"abcd");
        assert_eq!(frame.as_ptr(), source_ptr);
        assert_eq!(cursor.copied_bytes(), 0);
        assert_eq!(cursor.remaining(), 4);
    }

    #[test]
    fn frame_crossing_segments_copies_exactly_one_frame() {
        let mut cursor = TimerPayloadCursor::new(vec![
            selection(10, Bytes::from_static(b"abc")),
            selection(13, Bytes::from_static(b"defghi")),
        ]);

        let frame = cursor.take_frame(6).expect("read cross-segment frame");

        assert_eq!(frame.as_ref(), b"abcdef");
        assert_eq!(cursor.copied_bytes(), 6);
        assert_eq!(cursor.remaining(), 3);
        assert_eq!(cursor.take_frame(3).expect("read tail").as_ref(), b"ghi");
        assert_eq!(cursor.copied_bytes(), 6);
    }

    #[test]
    fn short_read_fails_without_partial_consumption() {
        let mut cursor = TimerPayloadCursor::new(vec![selection(0, Bytes::from_static(b"abc"))]);

        assert_eq!(
            cursor.take_frame(4),
            Err(TimerPayloadCursorViolation::ShortRead {
                cursor: 0,
                requested: 4,
                remaining: 3,
            })
        );
        assert_eq!(cursor.remaining(), 3);
        assert_eq!(cursor.copied_bytes(), 0);
    }
}
