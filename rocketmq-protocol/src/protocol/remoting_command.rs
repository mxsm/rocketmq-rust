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

use std::fmt;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(test)]
use std::collections::HashMap;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use serde::ser::SerializeMap;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::RemotingCommandType;
use super::SerializeType;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::HeaderEncodeCapability;
use crate::protocol::header_codec::write_json_string;
use crate::protocol::header_codec::BinaryHeaderFields;
use crate::protocol::header_codec::JsonHeaderFields;
use crate::protocol::LanguageCode;
use crate::rocketmq_serializable::RocketMQSerializable;

#[cfg(test)]
use crate::protocol::command_custom_header::FromMap;

pub const SERIALIZE_TYPE_PROPERTY: &str = "rocketmq.serialize.type";
pub const SERIALIZE_TYPE_ENV: &str = "ROCKETMQ_SERIALIZE_TYPE";
pub const REMOTING_VERSION_KEY: &str = "rocketmq.remoting.version";

static REQUEST_ID: AtomicI32 = AtomicI32::new(0);

#[cfg(test)]
std::thread_local! {
    static REQUEST_ID_GENERATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn request_id_generation_count() -> usize {
    REQUEST_ID_GENERATIONS.get()
}

#[inline]
fn next_request_id_from(counter: &AtomicI32) -> i32 {
    counter.fetch_add(1, Ordering::AcqRel)
}

#[inline]
fn next_request_id() -> i32 {
    #[cfg(test)]
    REQUEST_ID_GENERATIONS.set(REQUEST_ID_GENERATIONS.get() + 1);
    next_request_id_from(&REQUEST_ID)
}

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

mod accessors;
mod constructors;
mod custom_header;
mod extension_fields;
mod flags;
mod json_decode;

use extension_fields::ExtensionFields;
use json_decode::try_decode_json_header;
use json_decode::try_decode_json_header_bytes;

fn serialize_ext_fields<S>(ext_fields: &ExtensionFields, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(ext_fields) = ext_fields.as_map() else {
        return serializer.serialize_none();
    };
    let mut entries = ext_fields.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
    let mut map = serializer.serialize_map(Some(entries.len()))?;
    for (key, value) in entries {
        map.serialize_entry(key, value)?;
    }
    map.end()
}

fn deserialize_ext_fields<'de, D>(deserializer: D) -> Result<ExtensionFields, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<JsonHeaderFields>::deserialize(deserializer)
        .map(|fields| fields.map_or_else(ExtensionFields::default, ExtensionFields::from_json_raw))
}

#[derive(Serialize, Deserialize)]
pub struct RemotingCommand {
    code: i32,
    language: LanguageCode,
    version: i32,
    opaque: i32,

    ///flag -> bit: 00
    /// The lowest bit of the flag indicates whether it is a response command.
    /// Non-zero indicates a response command, while 0 indicates a request command.
    /// The second bit indicates whether it is a one-way request.
    /// Non-zero indicates a one-way request.
    flag: i32,
    remark: Option<CheetahString>,

    #[serde(
        rename = "extFields",
        default,
        serialize_with = "serialize_ext_fields",
        deserialize_with = "deserialize_ext_fields"
    )]
    ext_fields: ExtensionFields,

    #[serde(skip)]
    body: Option<Bytes>,
    #[serde(skip)]
    suspended: bool,
    #[serde(skip)]
    command_custom_header: Option<Arc<Box<dyn CommandCustomHeader + Send + Sync + 'static>>>,
    #[serde(skip)]
    custom_header_to_net: bool,
    #[serde(rename = "serializeTypeCurrentRPC")]
    serialize_type: SerializeType,
}

impl Clone for RemotingCommand {
    fn clone(&self) -> Self {
        Self {
            code: self.code,
            language: self.language,
            version: self.version,
            opaque: self.opaque,
            flag: self.flag,
            remark: self.remark.clone(),
            ext_fields: self.ext_fields.clone(),
            body: self.body.clone(),
            suspended: self.suspended,
            command_custom_header: self.command_custom_header.clone(),
            custom_header_to_net: self.custom_header_to_net,
            serialize_type: self.serialize_type,
        }
    }
}

impl fmt::Display for RemotingCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RemotingCommand [code={}, language={}, version={}, opaque={}, flag(B)={:b}, remark={}, extFields={:?}, \
             serializeTypeCurrentRPC={}]",
            self.code,
            self.language,
            self.version,
            self.opaque,
            self.flag,
            self.remark.as_ref().unwrap_or(&CheetahString::default()),
            self.ext_fields,
            self.serialize_type
        )
    }
}

impl Default for RemotingCommand {
    fn default() -> Self {
        Self::with_resolved_defaults(0, SerializeType::JSON)
    }
}

impl RemotingCommand {
    /// Legacy ambiguous-success response factory.
    ///
    /// New code should call [`Self::create_success_response_command`] so the
    /// response intent is visible during review. Call
    /// [`Self::create_java_default_error_response_command`] when matching
    /// Java's unset-response behavior instead.
    #[deprecated(
        note = "use create_success_response_command for SUCCESS or create_java_default_error_response_command for Java-compatible unset errors"
    )]
    pub fn create_response_command() -> Self {
        Self::create_success_response_command()
    }

    /// Legacy ambiguous-success typed-header response factory.
    ///
    /// New code should call [`Self::create_success_response_command_with_header`].
    /// Call [`Self::create_java_default_error_response_command_with_header`]
    /// when matching Java's unset-response behavior instead.
    #[deprecated(
        note = "use create_success_response_command_with_header for SUCCESS or create_java_default_error_response_command_with_header for Java-compatible unset errors"
    )]
    pub fn create_response_command_with_header(header: impl CommandCustomHeader + Sync + Send + 'static) -> Self {
        Self::create_success_response_command_with_header(header)
    }

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

    /// Encode header with body length information
    #[inline]
    pub fn encode_header(&mut self) -> Option<Bytes> {
        let body_length = self.body.as_ref().map_or(0, |b| b.len());
        self.encode_header_with_body_length(body_length)
    }

    /// Optimized header encoding with pre-calculated capacity
    #[inline]
    pub fn encode_header_with_body_length(&mut self, body_length: usize) -> Option<Bytes> {
        // Encode header data
        let header_data = self.header_encode()?;
        let header_len = header_data.len();
        let (total_length, marked_header_length) =
            Self::checked_frame_lengths(header_len, body_length, self.serialize_type).ok()?;

        // Allocate exact capacity
        let mut result = BytesMut::with_capacity(8 + header_len);

        // Write total length
        result.put_i32(total_length);

        // Write serialize type with embedded header length
        result.put_i32(marked_header_length);

        // Write header data
        result.put(header_data);

        Some(result.freeze())
    }

    /// Convert custom header to network format (merge into ext_fields)
    #[inline]
    pub fn make_custom_header_to_net(&mut self) {
        let _ = self.try_make_custom_header_to_net();
    }

    #[inline]
    pub fn materialize_custom_header_to_ext_fields(&mut self) {
        self.make_custom_header_to_net();
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

    /// Optimized JSON encoding with pre-calculated capacity and zero-copy optimizations
    #[inline]
    fn fast_encode_json(&mut self, dst: &mut BytesMut, body_length: usize) -> rocketmq_error::RocketMQResult<()> {
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

    /// Optimized ROCKETMQ binary encoding with minimal allocations
    #[inline]
    fn fast_encode_rocketmq(&mut self, dst: &mut BytesMut, body_length: usize) -> rocketmq_error::RocketMQResult<()> {
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
        .map_err(|error| rocketmq_error::RocketMQError::request_header_error(error.to_string()))?;
        let (total_length, serialize_type) =
            Self::checked_frame_lengths(header_size, body_length, SerializeType::ROCKETMQ)?;

        // Write total_length and serialize_type at the beginning (in-place update)
        let total_length = total_length.to_be_bytes();
        let serialize_type_bytes = serialize_type.to_be_bytes();

        dst[begin_index..begin_index + 4].copy_from_slice(&total_length);
        dst[begin_index + 4..begin_index + 8].copy_from_slice(&serialize_type_bytes);
        Ok(())
    }

    fn checked_frame_lengths(
        header_length: usize,
        body_length: usize,
        serialize_type: SerializeType,
    ) -> rocketmq_error::RocketMQResult<(i32, i32)> {
        const MAX_HEADER_LENGTH: usize = 0x00ff_ffff;
        if header_length > MAX_HEADER_LENGTH {
            return Err(rocketmq_error::SerializationError::encode_failed(
                "remoting-command",
                format!("encoded header is {header_length} bytes, exceeding the 24-bit wire limit"),
            )
            .into());
        }
        let payload_length = 4usize
            .checked_add(header_length)
            .and_then(|length| length.checked_add(body_length))
            .ok_or_else(|| {
                rocketmq_error::SerializationError::encode_failed("remoting-command", "encoded frame length overflow")
            })?;
        let total_length = i32::try_from(payload_length).map_err(|_| {
            rocketmq_error::SerializationError::encode_failed(
                "remoting-command",
                format!("encoded payload is {payload_length} bytes, exceeding the signed 32-bit wire limit"),
            )
        })?;
        let header_length = i32::try_from(header_length).map_err(|_| {
            rocketmq_error::SerializationError::encode_failed("remoting-command", "encoded header length overflow")
        })?;
        Ok((
            total_length,
            RemotingCommand::mark_serialize_type(header_length, serialize_type),
        ))
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

    /// Decodes one command with the historical 16 MiB announced-payload ceiling.
    ///
    /// Transport owners with an explicit total-wire limit should use
    /// [`Self::decode_with_max_frame_bytes`] so inbound and outbound policy stays symmetric.
    #[inline]
    pub fn decode(src: &mut BytesMut) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        Self::decode_with_max_frame_bytes(src, 16 * 1024 * 1024 + 4)
    }

    /// Decodes one command bounded by a caller-owned complete wire-frame limit.
    ///
    /// `max_frame_bytes` includes the four-byte announced-length prefix.
    ///
    /// # Errors
    ///
    /// Returns a serialization error for a negative, overflowing, oversized, or malformed frame.
    #[inline]
    pub fn decode_with_max_frame_bytes(
        src: &mut BytesMut,
        max_frame_bytes: usize,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        const FRAME_HEADER_SIZE: usize = 4;
        const SERIALIZE_TYPE_SIZE: usize = 4;
        const MIN_PAYLOAD_SIZE: usize = SERIALIZE_TYPE_SIZE; // Minimum: just serialize_type field

        let available = src.len();

        // Early return if not enough data for frame header
        if available < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Read total size without advancing the buffer (peek)
        let announced_size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]);
        let total_size = usize::try_from(announced_size).map_err(|_| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "remoting_command",
                message: format!("Invalid negative frame size {announced_size}"),
            })
        })?;
        let full_frame_size = total_size.checked_add(FRAME_HEADER_SIZE).ok_or_else(|| {
            rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                format: "remoting_command",
                message: format!("Frame size {total_size} overflows the wire envelope"),
            })
        })?;

        if full_frame_size > max_frame_bytes {
            return Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "remoting_command",
                    message: format!("Wire frame size {full_frame_size} exceeds configured limit {max_frame_bytes}"),
                },
            ));
        }

        // Wait for complete frame
        if available < full_frame_size {
            return Ok(None);
        }

        // Now validate minimum total_size (we have the complete frame)
        if total_size < MIN_PAYLOAD_SIZE {
            return Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "remoting_command",
                    message: format!("Invalid total_size {total_size}, minimum required is {MIN_PAYLOAD_SIZE}"),
                },
            ));
        }

        // Extract the complete frame before validating the marked header so
        // malformed complete frames preserve the established consume-on-error
        // behavior.
        let frame_data = src.split_to(full_frame_size);
        let ori_header_length = i32::from_be_bytes([frame_data[4], frame_data[5], frame_data[6], frame_data[7]]);
        let header_length = parse_header_length(ori_header_length);

        // Validate header length
        if header_length > total_size - SERIALIZE_TYPE_SIZE {
            return Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "remoting_command",
                    message: format!("Invalid header length {header_length}, total size {total_size}"),
                },
            ));
        }

        let protocol_type = parse_serialize_type(ori_header_length)?;
        let frame = frame_data.freeze();
        let header_start = FRAME_HEADER_SIZE + SERIALIZE_TYPE_SIZE;
        let header_end = header_start + header_length;
        let mut cmd = match protocol_type {
            SerializeType::ROCKETMQ => {
                RocketMQSerializable::rocket_mq_protocol_decode_bytes(frame.slice(header_start..header_end))?
            }
            SerializeType::JSON => {
                if let Some(cmd) = try_decode_json_header_bytes(frame.slice(header_start..header_end)) {
                    cmd
                } else {
                    // Unsupported JSON shapes retain the Serde/SIMD
                    // compatibility path. Copying is confined to this cold
                    // fallback after consuming the complete frame.
                    let mut header_data = BytesMut::from(&frame[header_start..header_end]);
                    Self::decode_json_header_fallback(&mut header_data, header_length)?
                }
            }
        };
        cmd.set_serialize_type_ref(protocol_type);
        if header_end < frame.len() {
            cmd.set_body_mut_ref(frame.slice(header_end..));
        }
        Ok(Some(cmd))
    }

    #[cold]
    #[inline(never)]
    fn decode_json_header_fallback(
        src: &mut BytesMut,
        _header_length: usize,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        #[cfg(feature = "simd")]
        {
            let mut slice = src.split_to(_header_length).to_vec();
            simd_json::from_slice::<RemotingCommand>(&mut slice).map_err(|error| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "json",
                    message: format!("SIMD JSON deserialization error: {error}"),
                })
            })
        }

        #[cfg(not(feature = "simd"))]
        {
            serde_json::from_slice::<RemotingCommand>(src).map_err(|error| {
                rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                    format: "json",
                    message: format!("JSON deserialization error: {error}"),
                })
            })
        }
    }

    /// Optimized header decoding with type-based dispatch
    #[inline]
    pub fn header_decode(
        src: &mut BytesMut,
        header_length: usize,
        type_: SerializeType,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match type_ {
            SerializeType::JSON => {
                if let Some(cmd) = try_decode_json_header(src, header_length) {
                    return Ok(Some(cmd.set_serialize_type(SerializeType::JSON)));
                }
                let cmd = Self::decode_json_header_fallback(src, header_length)?;
                Ok(Some(cmd.set_serialize_type(SerializeType::JSON)))
            }
            SerializeType::ROCKETMQ => {
                // Deserialize binary header
                let cmd = RocketMQSerializable::rocket_mq_protocol_decode(src, header_length)?;
                Ok(Some(cmd.set_serialize_type(SerializeType::ROCKETMQ)))
            }
        }
    }

    #[inline]
    pub fn mark_serialize_type(header_length: i32, protocol_type: SerializeType) -> i32 {
        ((protocol_type.get_code() as i32) << 24) | (header_length & 0x00FFFFFF)
    }

    pub fn read_custom_header_ref_unchecked<T>(&self) -> rocketmq_error::RocketMQResult<&T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_ref::<T>()
    }

    /// Compatibility name for the former shared-reference mutation escape.
    ///
    /// Mutation now requires exclusive access to this command and succeeds only
    /// when the safely shared header is uniquely owned.
    #[deprecated(note = "use read_custom_header_mut; shared-reference mutation is no longer supported")]
    pub fn read_custom_header_mut_from_ref<T>(&mut self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.read_custom_header_mut::<T>()
    }

    pub fn read_custom_header_mut_unchecked<T>(&mut self) -> rocketmq_error::RocketMQResult<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_mut::<T>()
    }
}

/// Extract header length from the combined serialize_type field
#[inline]
pub fn parse_header_length(size: i32) -> usize {
    (size & 0x00FFFFFF) as usize
}

/// Combine serialize type code with header length
#[inline]
pub fn mark_protocol_type(source: i32, serialize_type: SerializeType) -> i32 {
    ((serialize_type.get_code() as i32) << 24) | (source & 0x00FFFFFF)
}

/// Extract serialize type from the combined field
#[inline]
pub fn parse_serialize_type(size: i32) -> rocketmq_error::RocketMQResult<SerializeType> {
    let code = (size >> 24) as u8;
    SerializeType::value_of(code).ok_or({
        rocketmq_error::RocketMQError::Protocol(rocketmq_error::ProtocolError::UnsupportedSerializationType {
            serialize_type: code,
        })
    })
}

#[cfg(test)]
mod tests;
