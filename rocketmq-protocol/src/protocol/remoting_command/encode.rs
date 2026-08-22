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
use bytes::BytesMut;

use super::RemotingCommand;
use super::SerializeType;
use crate::rocketmq_serializable::RocketMQSerializable;

mod json;
mod rocketmq;

impl RemotingCommand {
    /// Encode header with optimized path selection
    #[inline]
    pub fn header_encode(&mut self) -> Option<Bytes> {
        match self.serialize_type {
            SerializeType::ROCKETMQ => {
                let mut encoded = BytesMut::new();
                RocketMQSerializable::try_rocketmq_protocol_encode(self, &mut encoded).ok()?;
                Some(encoded.freeze())
            }
            SerializeType::JSON => {
                self.try_make_custom_header_to_net().ok()?;
                #[cfg(feature = "simd")]
                {
                    match simd_json::to_vec(self) {
                        Ok(value) => Some(Bytes::from(value)),
                        Err(_) => None,
                    }
                }
                #[cfg(not(feature = "simd"))]
                {
                    match serde_json::to_vec(self) {
                        Ok(value) => Some(Bytes::from(value)),
                        Err(_) => None,
                    }
                }
            }
        }
    }

    #[inline]
    pub fn fast_header_encode(&mut self, dst: &mut BytesMut) {
        let _ = self.try_fast_header_encode(dst);
    }

    /// Encodes the frame header and rolls the destination back on failure.
    ///
    /// # Errors
    ///
    /// Returns the custom-header validation or direct-binary encoding failure.
    #[inline]
    pub fn try_fast_header_encode(&mut self, dst: &mut BytesMut) -> rocketmq_error::RocketMQResult<()> {
        let body_length = self.body.as_ref().map_or(0, Bytes::len);
        self.try_fast_header_encode_with_body_length(dst, body_length)
    }

    #[inline]
    pub(crate) fn try_fast_header_encode_with_body_length(
        &mut self,
        dst: &mut BytesMut,
        body_length: usize,
    ) -> rocketmq_error::RocketMQResult<()> {
        let checkpoint = dst.len();
        let result = match self.body.as_ref() {
            Some(body) if body.len() != body_length => Err(rocketmq_error::SerializationError::encode_failed(
                "remoting-command",
                "explicit body length does not match the in-memory body",
            )
            .into()),
            _ => self.try_fast_header_encode_inner(dst, body_length),
        };
        if result.is_err() {
            dst.truncate(checkpoint);
        }
        result
    }

    #[inline]
    fn try_fast_header_encode_inner(
        &mut self,
        dst: &mut BytesMut,
        body_length: usize,
    ) -> rocketmq_error::RocketMQResult<()> {
        match self.serialize_type {
            SerializeType::JSON => self.fast_encode_json(dst, body_length),
            SerializeType::ROCKETMQ => self.fast_encode_rocketmq(dst, body_length),
        }
    }
}
