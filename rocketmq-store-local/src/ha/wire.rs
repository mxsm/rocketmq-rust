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

use std::ops::Range;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use tokio_util::codec::Decoder;

/// Default HA transfer header: physical offset followed by body size.
pub const TRANSFER_HEADER_SIZE: usize = 8 + 4;
/// Controller-mode transfer header adds the master's confirmed offset.
pub const CONTROLLER_TRANSFER_HEADER_SIZE: usize = TRANSFER_HEADER_SIZE + 8;
/// Slave offset report without a controller broker id.
pub const REPORT_HEADER_SIZE: usize = 8;
/// Controller-mode slave report includes the broker id.
pub const CONTROLLER_REPORT_HEADER_SIZE: usize = 16;
/// Compatibility default when `ha_transfer_batch_size` is zero.
pub const DEFAULT_HA_TRANSFER_BATCH_SIZE: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferHeader {
    pub master_phy_offset: i64,
    pub body_size: usize,
    pub confirm_offset: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HaWireError {
    #[error("transfer header underflow: expected at least {expected} bytes, got {actual}")]
    HeaderUnderflow { expected: usize, actual: usize },
    #[error("transfer header contains negative body size: {0}")]
    NegativeBodySize(i32),
    #[error("transfer body size exceeds i32: {0}")]
    BodySizeOverflow(usize),
    #[error("master pushed offset != slave max, slave: {slave_offset}, master: {master_offset}")]
    OffsetMismatch { slave_offset: i64, master_offset: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaFramePlan {
    pub master_phy_offset: i64,
    pub body_range: Range<usize>,
    pub confirm_offset: Option<i64>,
    pub next_dispatch_position: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetFrame {
    pub offset: i64,
    pub broker_id: Option<i64>,
}

pub struct OffsetDecoder {
    frame_size: usize,
}

impl OffsetDecoder {
    pub const fn new(frame_size: usize) -> Self {
        Self { frame_size }
    }
}

impl Decoder for OffsetDecoder {
    type Item = OffsetFrame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.frame_size < REPORT_HEADER_SIZE || src.len() < self.frame_size {
            return Ok(None);
        }

        let offset = i64::from_be_bytes(
            src[..REPORT_HEADER_SIZE]
                .try_into()
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HA offset report"))?,
        );
        let broker_id = if self.frame_size >= CONTROLLER_REPORT_HEADER_SIZE {
            Some(i64::from_be_bytes(
                src[REPORT_HEADER_SIZE..CONTROLLER_REPORT_HEADER_SIZE]
                    .try_into()
                    .map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid HA controller report")
                    })?,
            ))
        } else {
            None
        };

        src.advance(self.frame_size);
        Ok(Some(OffsetFrame { offset, broker_id }))
    }
}

pub const fn transfer_header_size(enable_controller_mode: bool) -> usize {
    if enable_controller_mode {
        CONTROLLER_TRANSFER_HEADER_SIZE
    } else {
        TRANSFER_HEADER_SIZE
    }
}

pub const fn effective_ha_transfer_batch_size(configured_batch_size: usize) -> usize {
    if configured_batch_size == 0 {
        DEFAULT_HA_TRANSFER_BATCH_SIZE
    } else {
        configured_batch_size
    }
}

pub fn encode_transfer_header(
    buffer: &mut BytesMut,
    master_phy_offset: i64,
    body_size: usize,
    enable_controller_mode: bool,
    confirm_offset: i64,
) -> Result<Bytes, HaWireError> {
    let body_size = i32::try_from(body_size).map_err(|_| HaWireError::BodySizeOverflow(body_size))?;
    buffer.clear();
    buffer.put_i64(master_phy_offset);
    buffer.put_i32(body_size);
    if enable_controller_mode {
        buffer.put_i64(confirm_offset);
    }
    Ok(buffer.split().freeze())
}

pub fn encode_offset_report(
    buffer: &mut BytesMut,
    max_offset: i64,
    enable_controller_mode: bool,
    reported_broker_id: i64,
) -> Bytes {
    buffer.clear();
    buffer.put_i64(max_offset);
    if enable_controller_mode {
        buffer.put_i64(reported_broker_id);
    }
    buffer.split().freeze()
}

pub fn decode_transfer_header(src: &[u8], enable_controller_mode: bool) -> Result<TransferHeader, HaWireError> {
    let header_size = transfer_header_size(enable_controller_mode);
    if src.len() < header_size {
        return Err(HaWireError::HeaderUnderflow {
            expected: header_size,
            actual: src.len(),
        });
    }

    let master_phy_offset = i64::from_be_bytes(src[0..8].try_into().expect("fixed transfer offset width"));
    let body_size = i32::from_be_bytes(src[8..12].try_into().expect("fixed transfer body-size width"));
    if body_size < 0 {
        return Err(HaWireError::NegativeBodySize(body_size));
    }
    let confirm_offset = enable_controller_mode.then(|| {
        i64::from_be_bytes(
            src[TRANSFER_HEADER_SIZE..CONTROLLER_TRANSFER_HEADER_SIZE]
                .try_into()
                .expect("fixed controller confirm-offset width"),
        )
    });

    Ok(TransferHeader {
        master_phy_offset,
        body_size: body_size as usize,
        confirm_offset,
    })
}

/// Plans one complete follower frame without applying CommitLog side effects.
pub fn plan_replica_frame(
    buffer: &[u8],
    dispatch_position: usize,
    enable_controller_mode: bool,
    slave_phy_offset: i64,
) -> Result<Option<ReplicaFramePlan>, HaWireError> {
    let header_size = transfer_header_size(enable_controller_mode);
    let available = buffer.len().saturating_sub(dispatch_position);
    if available < header_size {
        return Ok(None);
    }

    let header_end = dispatch_position + header_size;
    let header = decode_transfer_header(&buffer[dispatch_position..header_end], enable_controller_mode)?;
    if slave_phy_offset != 0 && slave_phy_offset != header.master_phy_offset {
        return Err(HaWireError::OffsetMismatch {
            slave_offset: slave_phy_offset,
            master_offset: header.master_phy_offset,
        });
    }

    let frame_size = header_size.saturating_add(header.body_size);
    if available < frame_size {
        return Ok(None);
    }
    let body_end = header_end + header.body_size;
    Ok(Some(ReplicaFramePlan {
        master_phy_offset: header.master_phy_offset,
        body_range: header_end..body_end,
        confirm_offset: header.confirm_offset,
        next_dispatch_position: body_end,
    }))
}

pub const fn clamp_confirm_offset(confirm_offset: i64, min_phy_offset: i64, max_phy_offset: i64) -> i64 {
    if confirm_offset < min_phy_offset {
        min_phy_offset
    } else if confirm_offset > max_phy_offset {
        max_phy_offset
    } else {
        confirm_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_header_and_replica_plan_preserve_wire_offsets() {
        let mut buffer = BytesMut::new();
        let header = encode_transfer_header(&mut buffer, 128, 4, true, 96).expect("encode header");
        let mut frame = header.to_vec();
        frame.extend_from_slice(b"data");

        let plan = plan_replica_frame(&frame, 0, true, 128)
            .expect("plan frame")
            .expect("complete frame");
        assert_eq!(plan.master_phy_offset, 128);
        assert_eq!(&frame[plan.body_range], b"data");
        assert_eq!(plan.confirm_offset, Some(96));
        assert_eq!(plan.next_dispatch_position, CONTROLLER_TRANSFER_HEADER_SIZE + 4);
    }

    #[test]
    fn partial_frame_waits_without_advancing() {
        let mut buffer = BytesMut::new();
        let header = encode_transfer_header(&mut buffer, 0, 4, false, 0).expect("encode header");
        assert_eq!(plan_replica_frame(&header, 0, false, 0).expect("plan"), None);
    }

    #[test]
    fn offset_report_decoder_preserves_controller_broker_id() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&encode_offset_report(&mut BytesMut::new(), 42, true, 7));
        let frame = OffsetDecoder::new(CONTROLLER_REPORT_HEADER_SIZE)
            .decode(&mut buffer)
            .expect("decode")
            .expect("frame");
        assert_eq!(frame.offset, 42);
        assert_eq!(frame.broker_id, Some(7));
    }
}
