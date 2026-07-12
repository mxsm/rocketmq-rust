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

//! Runtime-neutral CommitLog V1 framing and bounded frame iteration.

use bytes::Buf;
use bytes::Bytes;

/// CommitLog V1 message magic code (`daa320a7`).
pub const MESSAGE_MAGIC_CODE: i32 = -626843481;

/// CommitLog V2 message magic code (`daa320ab`).
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626843477;

/// CommitLog end-of-segment blank magic code (`cbd43194`).
pub const BLANK_MAGIC_CODE: i32 = -875286124;

const PARSE_BATCH_SIZE: usize = 64 * 1024;
const MIN_MESSAGE_SIZE: usize = 4 + 4;

fn frame_fits(absolute_offset: usize, frame_size: usize, source_len: usize) -> bool {
    absolute_offset
        .checked_add(frame_size)
        .is_some_and(|frame_end| frame_end <= source_len)
}

/// Returns whether `frame` starts with a complete CommitLog blank marker.
#[inline]
pub fn is_blank_message(frame: &Bytes) -> bool {
    if frame.len() < MIN_MESSAGE_SIZE {
        return false;
    }
    i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) == BLANK_MAGIC_CODE
}

/// Static source of bounded, copied CommitLog frame bytes.
///
/// Implementations expose a fixed length for the lifetime of a cursor. A successful [`Self::read`]
/// must return exactly `len` copied bytes for a range wholly within that fixed length; otherwise it
/// returns `None`.
pub trait CommitLogFrameSource {
    /// Returns the immutable byte length visible to a frame cursor.
    fn source_len(&self) -> usize;

    /// Returns exactly `len` copied bytes beginning at `offset` when the full range is available.
    fn read(&self, offset: usize, len: usize) -> Option<Bytes>;
}

/// Batched cursor over length-prefixed CommitLog frames.
pub struct CommitLogFrameCursor<S> {
    source: S,
    current_offset: usize,
    source_len: usize,
    buffer: Bytes,
}

impl<S: CommitLogFrameSource> CommitLogFrameCursor<S> {
    /// Creates a cursor at offset zero using the source's fixed length snapshot.
    pub fn new(source: S) -> Self {
        let source_len = source.source_len();
        Self {
            source,
            current_offset: 0,
            source_len,
            buffer: Bytes::new(),
        }
    }

    fn refill_buffer(&mut self) -> bool {
        if self.current_offset >= self.source_len {
            return false;
        }

        let remaining = self.source_len - self.current_offset;
        let fetch_size = remaining.min(PARSE_BATCH_SIZE);
        let Some(bytes) = self.source.read(self.current_offset, fetch_size) else {
            return false;
        };
        if bytes.len() != fetch_size {
            return false;
        }
        self.buffer = bytes;
        true
    }

    /// Returns the next complete frame, its absolute offset, and its declared size.
    ///
    /// A non-positive size, incomplete header, unavailable read, or frame extending beyond the
    /// fixed source length ends iteration without advancing the current offset.
    pub fn next_message(&mut self) -> Option<(Bytes, usize, usize)> {
        loop {
            if self.buffer.remaining() < MIN_MESSAGE_SIZE
                && (!self.refill_buffer() || self.buffer.remaining() < MIN_MESSAGE_SIZE)
            {
                return None;
            }

            let total_size = {
                let mut peek = self.buffer.clone();
                peek.get_i32()
            };
            if total_size <= 0 {
                return None;
            }

            let frame_size = total_size as usize;
            let absolute_offset = self.current_offset;
            if !frame_fits(absolute_offset, frame_size, self.source_len) {
                return None;
            }

            if self.buffer.remaining() < frame_size {
                if frame_size > PARSE_BATCH_SIZE {
                    let frame = self.source.read(self.current_offset, frame_size)?;
                    if frame.len() != frame_size {
                        return None;
                    }
                    self.current_offset += frame_size;
                    self.buffer = Bytes::new();
                    return Some((frame, absolute_offset, frame_size));
                }

                if !self.refill_buffer() {
                    return None;
                }
                continue;
            }

            let frame = self.buffer.copy_to_bytes(frame_size);
            self.current_offset += frame_size;
            return Some((frame, absolute_offset, frame_size));
        }
    }

    /// Returns the absolute offset immediately after the last complete returned frame.
    pub fn current_offset(&self) -> usize {
        self.current_offset
    }
}

#[cfg(test)]
mod tests {
    use super::frame_fits;

    #[test]
    fn frame_fit_accepts_equal_boundary_and_rejects_overflow() {
        assert!(frame_fits(8, 8, 16));
        assert!(!frame_fits(usize::MAX, 1, usize::MAX));
    }
}
