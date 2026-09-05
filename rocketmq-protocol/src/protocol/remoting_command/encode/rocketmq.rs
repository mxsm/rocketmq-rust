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

use bytes::BufMut;
use bytes::BytesMut;

use super::super::RemotingCommand;
use super::super::SerializeType;
use crate::protocol::command_custom_header::HeaderEncodeCapability;
use crate::rocketmq_serializable::RocketMQSerializable;

impl RemotingCommand {
    /// Optimized ROCKETMQ binary encoding with minimal allocations
    #[inline]
    pub(super) fn fast_encode_rocketmq(
        &mut self,
        dst: &mut BytesMut,
        body_length: usize,
    ) -> rocketmq_error::RocketMQResult<()> {
        let begin_index = dst.len();
        dst.reserve(8 + RocketMQSerializable::INITIAL_ENCODE_CAPACITY);
        dst.put_i64(0); // Placeholder for total_length + serialize_type

        let capability = self.custom_header_encode_capability();

        let direct_header = (capability == HeaderEncodeCapability::DirectBinary
            && self.remark.is_none()
            && self.ext_fields.is_absent())
        .then(|| self.command_custom_header_ref())
        .flatten();
        let header_size = match direct_header {
            Some(header) => RocketMQSerializable::try_rocketmq_protocol_encode_direct(self, header, dst),
            None => RocketMQSerializable::try_rocketmq_protocol_encode_with_capability(self, dst, capability),
        }
        .map_err(crate::protocol::header_codec::into_rocketmq_error)?;
        let (total_length, serialize_type) =
            Self::checked_frame_lengths(header_size, body_length, SerializeType::ROCKETMQ)?;

        // Write total_length and serialize_type at the beginning (in-place update)
        let total_length = total_length.to_be_bytes();
        let serialize_type_bytes = serialize_type.to_be_bytes();

        dst[begin_index..begin_index + 4].copy_from_slice(&total_length);
        dst[begin_index + 4..begin_index + 8].copy_from_slice(&serialize_type_bytes);
        Ok(())
    }
}
