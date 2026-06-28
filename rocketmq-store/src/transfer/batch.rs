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
}

pub enum TransferPlan {
    NoData,
    Heartbeat { next_offset: i64 },
    Data(TransferBatch),
}
