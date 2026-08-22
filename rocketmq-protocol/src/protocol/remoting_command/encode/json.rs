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
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::header_codec::write_json_string;
use crate::protocol::LanguageCode;

#[inline]
fn write_json_i32(out: &mut BytesMut, value: i32) {
    let mut buffer = itoa::Buffer::new();
    out.extend_from_slice(buffer.format(value).as_bytes());
}

#[inline]
const fn language_name(language: LanguageCode) -> &'static str {
    match language {
        LanguageCode::JAVA => "JAVA",
        LanguageCode::CPP => "CPP",
        LanguageCode::DOTNET => "DOTNET",
        LanguageCode::PYTHON => "PYTHON",
        LanguageCode::DELPHI => "DELPHI",
        LanguageCode::ERLANG => "ERLANG",
        LanguageCode::RUBY => "RUBY",
        LanguageCode::OTHER => "OTHER",
        LanguageCode::HTTP => "HTTP",
        LanguageCode::GO => "GO",
        LanguageCode::PHP => "PHP",
        LanguageCode::OMS => "OMS",
        LanguageCode::RUST => "RUST",
        LanguageCode::NODE_JS => "NODE_JS",
    }
}

impl RemotingCommand {
    /// Optimized JSON encoding with pre-calculated capacity and zero-copy optimizations
    #[inline]
    pub(super) fn fast_encode_json(
        &mut self,
        dst: &mut BytesMut,
        body_length: usize,
    ) -> rocketmq_error::RocketMQResult<()> {
        let direct_fields = !self.custom_header_to_net
            && self.ext_fields.is_absent()
            && self
                .command_custom_header_ref()
                .is_some_and(CommandCustomHeader::supports_direct_json_fields);
        if direct_fields {
            return self.fast_encode_json_direct(dst, body_length);
        }

        self.try_make_custom_header_to_net()
            .map_err(|error| rocketmq_error::RocketMQError::request_header_error(error.to_string()))?;

        let estimated_header_size = self.estimate_json_header_size();
        let begin_index = dst.len();

        dst.reserve(8 + estimated_header_size);
        dst.put_i64(0);
        let header_index = dst.len();

        #[cfg(feature = "simd")]
        let encode_result = simd_json::to_writer((&mut *dst).writer(), self);

        #[cfg(not(feature = "simd"))]
        let encode_result = serde_json::to_writer((&mut *dst).writer(), self);

        encode_result.map_err(|error| {
            rocketmq_error::SerializationError::encode_failed("remoting-command", error.to_string())
        })?;
        let header_length = dst.len() - header_index;
        let (total_length, marked_header_length) =
            Self::checked_frame_lengths(header_length, body_length, SerializeType::JSON)?;

        dst[begin_index..begin_index + 4].copy_from_slice(&total_length.to_be_bytes());
        dst[begin_index + 4..begin_index + 8].copy_from_slice(&marked_header_length.to_be_bytes());
        Ok(())
    }

    #[inline]
    fn fast_encode_json_direct(&self, dst: &mut BytesMut, body_length: usize) -> rocketmq_error::RocketMQResult<()> {
        let header = self.command_custom_header_ref().ok_or_else(|| {
            rocketmq_error::SerializationError::encode_failed(
                "remoting-command",
                "direct JSON header capability was selected without a custom header",
            )
        })?;
        let begin_index = dst.len();

        // Avoid a full preflight walk over every typed field. A single 1 KiB
        // allocation covers normal request headers and lets unusually large
        // values fall back to BytesMut's regular growth policy.
        dst.reserve(1024);
        dst.put_i64(0);
        let header_index = dst.len();

        dst.extend_from_slice(b"{\"code\":");
        write_json_i32(dst, self.code);
        dst.extend_from_slice(b",\"language\":");
        write_json_string(dst, language_name(self.language));
        dst.extend_from_slice(b",\"version\":");
        write_json_i32(dst, self.version);
        dst.extend_from_slice(b",\"opaque\":");
        write_json_i32(dst, self.opaque);
        dst.extend_from_slice(b",\"flag\":");
        write_json_i32(dst, self.flag);
        dst.extend_from_slice(b",\"remark\":");
        if let Some(remark) = &self.remark {
            write_json_string(dst, remark.as_str());
        } else {
            dst.extend_from_slice(b"null");
        }
        dst.extend_from_slice(b",\"extFields\":");
        header
            .encode_direct_json_fields(dst)
            .map_err(|error| rocketmq_error::RocketMQError::request_header_error(error.to_string()))?;
        dst.extend_from_slice(b",\"serializeTypeCurrentRPC\":\"JSON\"}");

        let header_length = dst.len() - header_index;
        let (total_length, marked_header_length) =
            Self::checked_frame_lengths(header_length, body_length, SerializeType::JSON)?;
        dst[begin_index..begin_index + 4].copy_from_slice(&total_length.to_be_bytes());
        dst[begin_index + 4..begin_index + 8].copy_from_slice(&marked_header_length.to_be_bytes());
        Ok(())
    }

    /// Estimate JSON header size to reduce buffer reallocations
    /// This is an approximation based on typical field sizes
    #[inline]
    fn estimate_json_header_size(&self) -> usize {
        let mut size = 100; // Base JSON overhead

        if let Some(ref remark) = self.remark {
            size += remark.len() + 20; // "remark":"..." + quotes
        }

        if let Some(ext) = self.ext_fields.as_map() {
            // Approximate: each entry adds ~30 bytes overhead + key/value lengths
            size += ext.iter().map(|(k, v)| k.len() + v.len() + 30).sum::<usize>();
        }

        size
    }
}
