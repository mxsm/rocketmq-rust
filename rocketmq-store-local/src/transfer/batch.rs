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

use std::io;

use bytes::Bytes;

use crate::transfer::segment::SegmentLease;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferKind {
    Heartbeat,
    Data,
}

pub struct TransferBatch {
    pub frame_header: Bytes,
    pub segments: Vec<SegmentLease>,
    pub total_body_len: usize,
    pub start_offset: i64,
    pub next_offset: i64,
    pub kind: TransferKind,
}

impl TransferBatch {
    pub fn data(start_offset: i64, segments: Vec<SegmentLease>) -> Self {
        let total_body_len = segments.iter().map(SegmentLease::len).sum::<usize>();
        Self {
            frame_header: Bytes::new(),
            segments,
            total_body_len,
            start_offset,
            next_offset: start_offset + total_body_len as i64,
            kind: TransferKind::Data,
        }
    }

    pub fn body_bytes(&self) -> Option<Bytes> {
        match self.segments.as_slice() {
            [segment] => segment.as_bytes(),
            [] => Some(Bytes::new()),
            segments => {
                let mut bytes = Vec::with_capacity(self.total_body_len);
                for segment in segments {
                    bytes.extend_from_slice(segment.as_bytes()?.as_ref());
                }
                Some(Bytes::from(bytes))
            }
        }
    }

    /// Materializes only file-backed segments into exact byte ranges.
    ///
    /// Callers must run this operation on the storage I/O lane because positional file reads may
    /// block. Existing byte-backed segments retain their shared buffers.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when an exact file range cannot be read or a segment has no readable
    /// backing source.
    pub fn into_bytes_backed(self) -> io::Result<Self> {
        let mut materialized = Vec::with_capacity(self.segments.len());
        for segment in self.segments {
            let bytes = match segment.as_bytes() {
                Some(bytes) => bytes,
                None => segment
                    .as_file_range()
                    .ok_or_else(|| io::Error::other("HA segment has neither bytes nor a file range"))?
                    .to_bytes()?,
            };
            let bytes = bytes.slice(..segment.len().min(bytes.len()));
            materialized.push(SegmentLease::from_bytes(
                segment.segment().global_offset(),
                segment.segment().position_in_file(),
                bytes,
                segment.segment().cache_state(),
            ));
        }
        Ok(Self {
            frame_header: self.frame_header,
            segments: materialized,
            total_body_len: self.total_body_len,
            start_offset: self.start_offset,
            next_offset: self.next_offset,
            kind: self.kind,
        })
    }
}

pub enum TransferPlan {
    NoData,
    Heartbeat { next_offset: i64 },
    Data(TransferBatch),
}
