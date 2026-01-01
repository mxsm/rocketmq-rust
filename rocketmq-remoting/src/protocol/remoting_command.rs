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

use std::collections::HashMap;
use std::fmt;
use std::hint;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_common::common::mq_version::RocketMqVersion;
#[cfg(not(feature = "simd"))]
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;
use tracing::error;

use super::RemotingCommandType;
use super::SerializeType;
use crate::code::request_code::RequestCode;
use crate::code::response_code::RemotingSysResponseCode;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::LanguageCode;
use crate::rocketmq_serializable::RocketMQSerializable;

pub const SERIALIZE_TYPE_PROPERTY: &str = "rocketmq.serialize.type";
pub const SERIALIZE_TYPE_ENV: &str = "ROCKETMQ_SERIALIZE_TYPE";
pub const REMOTING_VERSION_KEY: &str = "rocketmq.remoting.version";

static REQUEST_ID: std::sync::LazyLock<Arc<AtomicI32>> = std::sync::LazyLock::new(|| Arc::new(AtomicI32::new(0)));

static CONFIG_VERSION: std::sync::LazyLock<i32> = std::sync::LazyLock::new(|| {
    EnvUtils::get_property(REMOTING_VERSION_KEY)
        .unwrap_or(String::from("0"))
        .parse::<i32>()
        .unwrap_or(0)
});

pub static SERIALIZE_TYPE_CONFIG_IN_THIS_SERVER: std::sync::LazyLock<SerializeType> = std::sync::LazyLock::new(|| {
    let protocol = std::env::var(SERIALIZE_TYPE_PROPERTY)
        .unwrap_or_else(|_| std::env::var(SERIALIZE_TYPE_ENV).unwrap_or_else(|_| "".to_string()));
    match protocol.as_str() {
        "JSON" => SerializeType::JSON,
        "ROCKETMQ" => SerializeType::ROCKETMQ,
        _ => SerializeType::JSON,
    }
});

fn set_cmd_version(cmd: &mut RemotingCommand) {
    cmd.set_version_ref(*CONFIG_VERSION);
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

    #[serde(rename = "extFields")]
    ext_fields: Option<HashMap<CheetahString, CheetahString>>,

    #[serde(skip)]
    body: Option<Bytes>,
    #[serde(skip)]
    suspended: bool,
    #[serde(skip)]
    command_custom_header: Option<ArcMut<Box<dyn CommandCustomHeader + Send + Sync + 'static>>>,
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
        let opaque = REQUEST_ID.fetch_add(1, Ordering::AcqRel);
        RemotingCommand {
            code: 0,
            language: LanguageCode::RUST, // Replace with your actual enum variant
            version: 0,
            opaque,
            flag: 0,
            remark: None,
            ext_fields: None,
            body: None,
            suspended: false,
            command_custom_header: None,
            serialize_type: *SERIALIZE_TYPE_CONFIG_IN_THIS_SERVER,
        }
    }
}

impl RemotingCommand {
    pub(crate) const RPC_ONEWAY: i32 = 1;
    pub(crate) const RPC_TYPE: i32 = 0;
}

impl RemotingCommand {
    pub fn new_request(code: impl Into<i32>, body: impl Into<Bytes>) -> Self {
        Self::default().set_code(code).set_body(body)
    }

    pub fn create_request_command<T>(code: impl Into<i32>, header: T) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        let mut command = Self::default().set_code(code.into()).set_command_custom_header(header);
        set_cmd_version(&mut command);
        command
    }

    pub fn create_remoting_command(code: impl Into<i32>) -> Self {
        let command = Self::default();
        command.set_code(code.into())
    }

    pub fn get_and_add() -> i32 {
        REQUEST_ID.fetch_add(1, Ordering::AcqRel)
    }

    pub fn create_response_command_with_code(code: impl Into<i32>) -> Self {
        Self::default().set_code(code).mark_response_type()
    }

    pub fn create_response_command_with_code_remark(code: impl Into<i32>, remark: impl Into<CheetahString>) -> Self {
        Self::default()
            .set_code(code)
            .set_remark_option(Some(remark.into()))
            .mark_response_type()
    }

    pub fn create_response_command() -> Self {
        Self::default()
            .set_code(RemotingSysResponseCode::Success)
            .mark_response_type()
    }

    pub fn create_response_command_with_header(header: impl CommandCustomHeader + Sync + Send + 'static) -> Self {
        Self::default()
            .set_code(RemotingSysResponseCode::Success)
            .set_command_custom_header(header)
            .mark_response_type()
    }

    pub fn set_command_custom_header<T>(mut self, command_custom_header: T) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.command_custom_header = Some(ArcMut::new(Box::new(command_custom_header)));
        self
    }

    pub fn set_command_custom_header_origin(
        mut self,
        command_custom_header: Option<ArcMut<Box<dyn CommandCustomHeader + Send + Sync + 'static>>>,
    ) -> Self {
        self.command_custom_header = command_custom_header;
        self
    }

    pub fn set_command_custom_header_ref<T>(&mut self, command_custom_header: T)
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.command_custom_header = Some(ArcMut::new(Box::new(command_custom_header)));
    }

    pub fn set_code(mut self, code: impl Into<i32>) -> Self {
        self.code = code.into();
        self
    }

    pub fn set_code_ref(&mut self, code: impl Into<i32>) {
        self.code = code.into();
    }

    pub fn set_code_mut(&mut self, code: impl Into<i32>) -> &mut Self {
        self.code = code.into();
        self
    }

    pub fn set_language(mut self, language: LanguageCode) -> Self {
        self.language = language;
        self
    }

    pub fn set_version_ref(&mut self, version: i32) {
        self.version = version;
    }

    pub fn set_version(mut self, version: i32) -> Self {
        self.version = version;
        self
    }

    #[inline]
    pub fn set_opaque(mut self, opaque: i32) -> Self {
        self.opaque = opaque;
        self
    }

    #[inline]
    pub fn set_opaque_mut(&mut self, opaque: i32) {
        self.opaque = opaque;
    }

    #[inline]
    pub fn set_flag(mut self, flag: i32) -> Self {
        self.flag = flag;
        self
    }

    #[inline]
    pub fn set_remark_option(mut self, remark: Option<impl Into<CheetahString>>) -> Self {
        self.remark = remark.map(|item| item.into());
        self
    }

    #[inline]
    pub fn set_remark(mut self, remark: impl Into<CheetahString>) -> Self {
        self.remark = Some(remark.into());
        self
    }

    #[inline]
    pub fn set_remark_option_mut(&mut self, remark: Option<impl Into<CheetahString>>) {
        self.remark = remark.map(|item| item.into());
    }

    #[inline]
    pub fn set_remark_mut(&mut self, remark: impl Into<CheetahString>) {
        self.remark = Some(remark.into());
    }

    #[inline]
    pub fn set_ext_fields(mut self, ext_fields: HashMap<CheetahString, CheetahString>) -> Self {
        self.ext_fields = Some(ext_fields);
        self
    }

    #[inline]
    pub fn set_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    #[inline]
    pub fn set_body_mut_ref(&mut self, body: impl Into<Bytes>) {
        self.body = Some(body.into());
    }

    #[inline]
    pub fn set_suspended(mut self, suspended: bool) -> Self {
        self.suspended = suspended;
        self
    }

    #[inline]
    pub fn set_suspended_ref(&mut self, suspended: bool) {
        self.suspended = suspended;
    }

    #[inline]
    pub fn set_serialize_type(mut self, serialize_type: SerializeType) -> Self {
        self.serialize_type = serialize_type;
        self
    }

    #[inline]
    pub fn mark_response_type(mut self) -> Self {
        let mark = 1 << Self::RPC_TYPE;
        self.flag |= mark;
        self
    }

    #[inline]
    pub fn mark_response_type_ref(&mut self) {
        let mark = 1 << Self::RPC_TYPE;
        self.flag |= mark;
    }

    #[inline]
    pub fn mark_oneway_rpc(mut self) -> Self {
        let mark = 1 << Self::RPC_ONEWAY;
        self.flag |= mark;
        self
    }

    #[inline]
    pub fn mark_oneway_rpc_ref(&mut self) {
        let mark = 1 << Self::RPC_ONEWAY;
        self.flag |= mark;
    }

    #[inline]
    pub fn get_serialize_type(&self) -> SerializeType {
        self.serialize_type
    }

    /// Encode header with optimized path selection
    #[inline]
    pub fn header_encode(&mut self) -> Option<Bytes> {
        self.make_custom_header_to_net();
        match self.serialize_type {
            SerializeType::ROCKETMQ => Some(RocketMQSerializable::rocket_mq_protocol_encode_bytes(self)),
            SerializeType::JSON => {
                #[cfg(feature = "simd")]
                {
                    match simd_json::to_vec(self) {
                        Ok(value) => Some(Bytes::from(value)),
                        Err(e) => {
                            error!("Failed to encode JSON header with simd-json: {}", e);
                            None
                        }
                    }
                }
                #[cfg(not(feature = "simd"))]
                {
                    match serde_json::to_vec(self) {
                        Ok(value) => Some(Bytes::from(value)),
                        Err(e) => {
                            error!("Failed to encode JSON header: {}", e);
                            None
                        }
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

        // Calculate frame size: 4 (total_length) + 4 (serialize_type) + header_len
        let frame_header_size = 8;
        let total_length = 4 + header_len + body_length; // 4 is for serialize_type field

        // Allocate exact capacity
        let mut result = BytesMut::with_capacity(frame_header_size + header_len);

        // Write total length
        result.put_i32(total_length as i32);

        // Write serialize type with embedded header length
        result.put_i32(mark_protocol_type(header_len as i32, self.serialize_type));

        // Write header data
        result.put(header_data);

        Some(result.freeze())
    }

    /// Convert custom header to network format (merge into ext_fields)
    #[inline]
    pub fn make_custom_header_to_net(&mut self) {
        if let Some(header) = &self.command_custom_header {
            if let Some(header_map) = header.to_map() {
                match &mut self.ext_fields {
                    None => {
                        self.ext_fields = Some(header_map);
                    }
                    Some(ext) => {
                        // Merge header map into existing ext_fields
                        for (key, value) in header_map {
                            ext.insert(key, value);
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn fast_header_encode(&mut self, dst: &mut BytesMut) {
        match self.serialize_type {
            SerializeType::JSON => {
                self.fast_encode_json(dst);
            }
            SerializeType::ROCKETMQ => {
                self.fast_encode_rocketmq(dst);
            }
        }
    }

    /// Optimized JSON encoding with pre-calculated capacity and zero-copy optimizations
    #[inline]
    fn fast_encode_json(&mut self, dst: &mut BytesMut) {
        self.make_custom_header_to_net();

        // Pre-calculate approximate header size to reduce reallocations
        let estimated_header_size = self.estimate_json_header_size();
        let body_length = self.body.as_ref().map_or(0, |b| b.len());

        // Reserve space upfront: 4 (total_length) + 4 (serialize_type) + estimated_header + body
        dst.reserve(8 + estimated_header_size + body_length);

        // Encode header using simd-json for better performance when available
        #[cfg(feature = "simd")]
        let encode_result = simd_json::to_vec(self);

        #[cfg(not(feature = "simd"))]
        let encode_result = serde_json::to_vec(self);

        match encode_result {
            Ok(header_bytes) => {
                let header_length = header_bytes.len() as i32;
                let body_length = body_length as i32;
                let total_length = 4 + header_length + body_length;

                // Write frame header
                dst.put_i32(total_length);
                dst.put_i32(RemotingCommand::mark_serialize_type(header_length, SerializeType::JSON));

                // Write header bytes (zero-copy from Vec)
                dst.put_slice(&header_bytes);
            }
            Err(e) => {
                error!("Failed to encode JSON header: {}", e);
                // Write minimal error frame
                dst.put_i32(4); // total_length: just the serialize_type field
                dst.put_i32(RemotingCommand::mark_serialize_type(0, SerializeType::JSON));
            }
        }
    }

    /// Optimized ROCKETMQ binary encoding with minimal allocations
    #[inline]
    fn fast_encode_rocketmq(&mut self, dst: &mut BytesMut) {
        let begin_index = dst.len();

        // Reserve space for total_length (4 bytes) and serialize_type (4 bytes)
        dst.reserve(8);
        dst.put_i64(0); // Placeholder for total_length + serialize_type

        // Check if custom header supports fast codec
        if let Some(header) = self.command_custom_header_ref() {
            if !header.support_fast_codec() {
                self.make_custom_header_to_net();
            }
        }

        // Encode header directly to buffer
        let header_size = RocketMQSerializable::rocketmq_protocol_encode(self, dst);
        let body_length = self.body.as_ref().map_or(0, |b| b.len()) as i32;

        // Calculate serialize type with header length embedded
        let serialize_type = RemotingCommand::mark_serialize_type(header_size as i32, SerializeType::ROCKETMQ);

        // Write total_length and serialize_type at the beginning (in-place update)
        let total_length = (header_size as i32 + body_length).to_be_bytes();
        let serialize_type_bytes = serialize_type.to_be_bytes();

        dst[begin_index..begin_index + 4].copy_from_slice(&total_length);
        dst[begin_index + 4..begin_index + 8].copy_from_slice(&serialize_type_bytes);
    }

    /// Estimate JSON header size to reduce buffer reallocations
    /// This is an approximation based on typical field sizes
    #[inline]
    fn estimate_json_header_size(&self) -> usize {
        let mut size = 100; // Base JSON overhead

        if let Some(ref remark) = self.remark {
            size += remark.len() + 20; // "remark":"..." + quotes
        }

        if let Some(ref ext) = self.ext_fields {
            // Approximate: each entry adds ~30 bytes overhead + key/value lengths
            size += ext.iter().map(|(k, v)| k.len() + v.len() + 30).sum::<usize>();
        }

        size
    }

    /// Optimized decode with enhanced boundary checks and reduced allocations
    #[inline]
    pub fn decode(src: &mut BytesMut) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        const FRAME_HEADER_SIZE: usize = 4;
        const SERIALIZE_TYPE_SIZE: usize = 4;
        const MIN_PAYLOAD_SIZE: usize = SERIALIZE_TYPE_SIZE; // Minimum: just serialize_type field

        let available = src.len();

        // Early return if not enough data for frame header
        if available < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Read total size without advancing the buffer (peek)
        let total_size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Validate total_size to prevent overflow attacks (check max first)
        if total_size > 16 * 1024 * 1024 {
            return Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "remoting_command",
                    message: format!("Frame size {total_size} exceeds maximum allowed (16MB)"),
                },
            ));
        }

        // Wait for complete frame
        let full_frame_size = total_size + FRAME_HEADER_SIZE;
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

        // Extract complete frame (zero-copy split)
        let mut cmd_data = src.split_to(full_frame_size);
        cmd_data.advance(FRAME_HEADER_SIZE); // Skip total_size field

        // Ensure we have serialize_type field (should always pass after above checks)
        if cmd_data.remaining() < SERIALIZE_TYPE_SIZE {
            return Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "remoting_command",
                    message: "Incomplete serialize_type field".to_string(),
                },
            ));
        }

        // Parse header length and protocol type
        let ori_header_length = cmd_data.get_i32();
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

        // Split header and body (zero-copy)
        let mut header_data = cmd_data.split_to(header_length);

        // Decode header
        let mut cmd = RemotingCommand::header_decode(&mut header_data, header_length, protocol_type)?;

        // Attach body if present (zero-copy freeze)
        if let Some(ref mut cmd) = cmd {
            let body_length = total_size - SERIALIZE_TYPE_SIZE - header_length;
            if body_length > 0 {
                if cmd_data.remaining() >= body_length {
                    cmd.set_body_mut_ref(cmd_data.split_to(body_length).freeze());
                } else {
                    return Err(rocketmq_error::RocketMQError::Serialization(
                        rocketmq_error::SerializationError::DecodeFailed {
                            format: "remoting_command",
                            message: format!(
                                "Insufficient body data: expected {body_length}, available {}",
                                cmd_data.remaining()
                            ),
                        },
                    ));
                }
            }
        }

        Ok(cmd)
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
                // Deserialize JSON header using simd-json when available
                #[cfg(feature = "simd")]
                let cmd = {
                    let mut slice = src.split_to(header_length).to_vec();
                    simd_json::from_slice::<RemotingCommand>(&mut slice).map_err(|error| {
                        rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                            format: "json",
                            message: format!("SIMD JSON deserialization error: {error}"),
                        })
                    })?
                };

                #[cfg(not(feature = "simd"))]
                let cmd = SerdeJsonUtils::from_json_slice::<RemotingCommand>(src).map_err(|error| {
                    rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
                        format: "json",
                        message: format!("JSON deserialization error: {error}"),
                    })
                })?;

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
    pub fn get_body(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    #[inline]
    pub fn get_body_mut(&mut self) -> Option<&mut Bytes> {
        self.body.as_mut()
    }

    #[inline]
    pub fn mark_serialize_type(header_length: i32, protocol_type: SerializeType) -> i32 {
        ((protocol_type.get_code() as i32) << 24) | (header_length & 0x00FFFFFF)
    }

    #[inline]
    pub fn code(&self) -> i32 {
        self.code
    }

    #[inline]
    pub fn request_code(&self) -> RequestCode {
        RequestCode::from(self.code)
    }

    #[inline]
    pub fn code_ref(&self) -> &i32 {
        &self.code
    }

    #[inline]
    pub fn language(&self) -> LanguageCode {
        self.language
    }

    #[inline]
    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn rocketmq_version(&self) -> RocketMqVersion {
        RocketMqVersion::from_ordinal(self.version as u32)
    }

    #[inline]
    pub fn opaque(&self) -> i32 {
        self.opaque
    }

    #[inline]
    pub fn flag(&self) -> i32 {
        self.flag
    }

    #[inline]
    pub fn remark(&self) -> Option<&CheetahString> {
        self.remark.as_ref()
    }

    #[inline]
    pub fn ext_fields(&self) -> Option<&HashMap<CheetahString, CheetahString>> {
        self.ext_fields.as_ref()
    }

    #[inline]
    pub fn body(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    #[inline]
    pub fn take_body(&mut self) -> Option<Bytes> {
        self.body.take()
    }

    #[inline]
    pub fn suspended(&self) -> bool {
        self.suspended
    }

    #[inline]
    pub fn serialize_type(&self) -> SerializeType {
        self.serialize_type
    }

    pub fn decode_command_custom_header<T>(&self) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
    {
        match self.ext_fields {
            None => Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "ExtFields is None".to_string(),
                },
            )),
            Some(ref header) => T::from(header),
        }
    }

    pub fn decode_command_custom_header_fast<T>(&self) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
        T: Default + CommandCustomHeader,
    {
        match self.ext_fields {
            None => Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "ExtFields is None".to_string(),
                },
            )),
            Some(ref header) => {
                let mut target = T::default();
                if target.support_fast_codec() {
                    target.decode_fast(header)?;
                    Ok(target)
                } else {
                    T::from(header)
                }
            }
        }
    }

    #[inline]
    pub fn is_response_type(&self) -> bool {
        let bits = 1 << Self::RPC_TYPE;
        (self.flag & bits) == bits
    }

    #[inline]
    pub fn is_oneway_rpc(&self) -> bool {
        let bits = 1 << Self::RPC_ONEWAY;
        (self.flag & bits) == bits
    }

    pub fn get_type(&self) -> RemotingCommandType {
        if self.is_response_type() {
            RemotingCommandType::RESPONSE
        } else {
            RemotingCommandType::REQUEST
        }
    }

    #[inline]
    pub fn with_opaque(&mut self, opaque: i32) -> &mut Self {
        self.opaque = opaque;
        self
    }

    pub fn add_ext_field(&mut self, key: impl Into<CheetahString>, value: impl Into<CheetahString>) -> &mut Self {
        if let Some(ref mut ext) = self.ext_fields {
            ext.insert(key.into(), value.into());
        }
        self
    }

    #[inline]
    pub fn with_code(&mut self, code: impl Into<i32>) -> &mut Self {
        self.code = code.into();
        self
    }

    #[inline]
    pub fn with_remark(&mut self, remark: impl Into<CheetahString>) -> &mut Self {
        self.remark = Some(remark.into());
        self
    }

    #[inline]
    pub fn get_ext_fields(&self) -> Option<&HashMap<CheetahString, CheetahString>> {
        self.ext_fields.as_ref()
    }

    pub fn read_custom_header_ref<T>(&self) -> Option<&T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_ref() {
            None => None,
            Some(value) => value.as_ref().as_any().downcast_ref::<T>(),
        }
    }

    pub fn read_custom_header_ref_unchecked<T>(&self) -> &T
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_ref() {
            None => unsafe { hint::unreachable_unchecked() },
            Some(value) => value.as_ref().as_any().downcast_ref::<T>().unwrap(),
        }
    }

    pub fn read_custom_header_mut<T>(&mut self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_mut() {
            None => None,
            Some(value) => value.as_mut().as_any_mut().downcast_mut::<T>(),
        }
    }

    pub fn read_custom_header_mut_from_ref<T>(&self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_ref() {
            None => None,
            Some(value) => value.mut_from_ref().as_any_mut().downcast_mut::<T>(),
        }
    }

    pub fn read_custom_header_mut_unchecked<T>(&mut self) -> &mut T
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_mut() {
            None => unsafe { hint::unreachable_unchecked() },
            Some(value) => value.as_mut().as_any_mut().downcast_mut::<T>().unwrap(),
        }
    }

    pub fn command_custom_header_ref(&self) -> Option<&dyn CommandCustomHeader> {
        match self.command_custom_header.as_ref() {
            None => None,
            Some(value) => Some(value.as_ref().as_ref()),
        }
    }

    pub fn command_custom_header_mut(&mut self) -> Option<&mut dyn CommandCustomHeader> {
        match self.command_custom_header.as_mut() {
            None => None,
            Some(value) => Some(value.as_mut().as_mut()),
        }
    }

    pub fn create_new_request_id() -> i32 {
        REQUEST_ID.fetch_add(1, Ordering::AcqRel)
    }

    #[inline]
    pub fn add_ext_field_if_not_exist(&mut self, key: impl Into<CheetahString>, value: impl Into<CheetahString>) {
        if let Some(ref mut ext) = self.ext_fields {
            ext.entry(key.into()).or_insert(value.into());
        }
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

impl AsRef<RemotingCommand> for RemotingCommand {
    #[inline]
    fn as_ref(&self) -> &RemotingCommand {
        self
    }
}

impl AsMut<RemotingCommand> for RemotingCommand {
    #[inline]
    fn as_mut(&mut self) -> &mut RemotingCommand {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remoting_command() {
        let command = RemotingCommand::create_remoting_command(1)
            .set_code(1)
            .set_language(LanguageCode::JAVA)
            .set_opaque(1)
            .set_flag(1)
            .set_ext_fields(HashMap::new())
            .set_remark_option(Some("remark".to_string()));

        assert_eq!(
            "{\"code\":1,\"language\":\"JAVA\",\"version\":0,\"opaque\":1,\"flag\":1,\"remark\":\"remark\",\"\
             extFields\":{},\"serializeTypeCurrentRPC\":\"JSON\"}",
            serde_json::to_string(&command).unwrap()
        );
    }

    #[test]
    fn test_mark_serialize_type() {
        let i = RemotingCommand::mark_serialize_type(261, SerializeType::JSON);
        assert_eq!(i, 261);

        let i = RemotingCommand::mark_serialize_type(16777215, SerializeType::JSON);
        assert_eq!(i, 16777215);

        println!("i={}", RemotingCommand::default().opaque);
        println!("i={}", RemotingCommand::default().opaque);
        println!("i={}", RemotingCommand::default().opaque);
        println!("i={}", RemotingCommand::default().opaque);
    }
}
