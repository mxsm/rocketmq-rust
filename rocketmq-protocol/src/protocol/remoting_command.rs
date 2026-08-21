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
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_model::version::RocketMqVersion;
use serde::ser::SerializeMap;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::RemotingCommandType;
use super::SerializeType;
use crate::code::request_code::RequestCode;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::command_custom_header::HeaderEncodeCapability;
use crate::protocol::header_codec::write_json_string;
use crate::protocol::header_codec::BinaryHeaderFields;
use crate::protocol::header_codec::HeaderCodecError;
use crate::protocol::header_codec::JsonHeaderFields;
use crate::protocol::header_field_merge::merge_header_and_dynamic;
use crate::protocol::remoting_command_defaults::application_remoting_command_factory;
use crate::protocol::remoting_command_defaults::RemotingCommandDefaults;
use crate::protocol::LanguageCode;
use crate::rocketmq_serializable::RocketMQSerializable;

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

mod extension_fields;
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
    /// Constructs a command from defaults resolved by the owning facade.
    ///
    /// The protocol crate deliberately does not read process environment or configuration files.
    /// Legacy facades resolve those sources and pass the resulting wire values here.
    pub fn with_resolved_defaults(version: i32, serialize_type: SerializeType) -> Self {
        let opaque = next_request_id();
        RemotingCommand {
            code: 0,
            language: LanguageCode::RUST, // Replace with your actual enum variant
            version,
            opaque,
            flag: 0,
            remark: None,
            ext_fields: ExtensionFields::default(),
            body: None,
            suspended: false,
            command_custom_header: None,
            custom_header_to_net: false,
            serialize_type,
        }
    }

    pub(crate) fn from_binary_wire_parts(
        code: i32,
        language: LanguageCode,
        version: i32,
        opaque: i32,
        flag: i32,
        remark: Option<CheetahString>,
        ext_fields: BinaryHeaderFields,
    ) -> Self {
        Self {
            code,
            language,
            version,
            opaque,
            flag,
            remark,
            ext_fields: ExtensionFields::from_rocketmq_raw(ext_fields),
            body: None,
            suspended: false,
            command_custom_header: None,
            custom_header_to_net: false,
            serialize_type: SerializeType::ROCKETMQ,
        }
    }
}

impl RemotingCommand {
    pub(crate) const RPC_ONEWAY: i32 = 1;
    pub(crate) const RPC_TYPE: i32 = 0;
}

impl RemotingCommand {
    pub fn new_request(code: impl Into<i32>, body: impl Into<Bytes>) -> Self {
        application_remoting_command_factory().create_request(code, body)
    }

    pub fn create_request_command<T>(code: impl Into<i32>, header: T) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        application_remoting_command_factory().create_request_command(code, header)
    }

    /// Creates a request using defaults resolved by the transport/facade owner.
    pub fn create_request_command_with_defaults<T>(
        code: impl Into<i32>,
        header: T,
        version: i32,
        serialize_type: SerializeType,
    ) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        crate::protocol::remoting_command_defaults::RemotingCommandFactory::new(RemotingCommandDefaults::new(
            version,
            serialize_type,
        ))
        .create_request_command(code, header)
    }

    pub fn create_remoting_command(code: impl Into<i32>) -> Self {
        application_remoting_command_factory().create_remoting_command(code)
    }

    pub fn get_and_add() -> i32 {
        next_request_id()
    }

    pub fn create_response_command_with_code(code: impl Into<i32>) -> Self {
        application_remoting_command_factory().create_response_command_with_code(code)
    }

    /// Creates a response with an explicit code and typed custom header.
    pub fn create_response_command_with_code_and_header(
        code: impl Into<i32>,
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> Self {
        application_remoting_command_factory().create_response_command_with_code_and_header(code, header)
    }

    pub fn create_response_command_with_code_remark(code: impl Into<i32>, remark: impl Into<CheetahString>) -> Self {
        application_remoting_command_factory().create_response_command_with_code_remark(code, remark)
    }

    /// Creates an explicitly successful response.
    pub fn create_success_response_command() -> Self {
        application_remoting_command_factory().create_success_response_command()
    }

    /// Creates an explicitly successful response with a typed custom header.
    pub fn create_success_response_command_with_header(
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> Self {
        application_remoting_command_factory().create_success_response_command_with_header(header)
    }

    /// Creates the unset error response used by Java's typed-header factory.
    pub fn create_java_default_error_response_command() -> Self {
        application_remoting_command_factory().create_java_default_error_response_command()
    }

    /// Creates the unset Java-compatible error response with a typed header.
    pub fn create_java_default_error_response_command_with_header(
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> Self {
        application_remoting_command_factory().create_java_default_error_response_command_with_header(header)
    }

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

    pub fn set_command_custom_header<T>(mut self, command_custom_header: T) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.invalidate_materialized_custom_header();
        self.command_custom_header = Some(Arc::new(Box::new(command_custom_header)));
        self.custom_header_to_net = false;
        self
    }

    pub fn set_command_custom_header_boxed(
        mut self,
        command_custom_header: Box<dyn CommandCustomHeader + Send + Sync + 'static>,
    ) -> Self {
        self.invalidate_materialized_custom_header();
        self.command_custom_header = Some(Arc::new(command_custom_header));
        self.custom_header_to_net = false;
        self
    }

    pub fn set_command_custom_header_origin<T>(mut self, command_custom_header: Option<T>) -> Self
    where
        T: std::ops::Deref<Target = Box<dyn CommandCustomHeader + Send + Sync + 'static>>,
    {
        self.invalidate_materialized_custom_header();
        if let Some(header_fields) = command_custom_header.as_ref().and_then(|header| header.to_map()) {
            self.ext_fields.get_or_insert_map().extend(header_fields);
        }
        self.command_custom_header = None;
        self.custom_header_to_net = true;
        self
    }

    pub fn set_command_custom_header_ref<T>(&mut self, command_custom_header: T)
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.invalidate_materialized_custom_header();
        self.command_custom_header = Some(Arc::new(Box::new(command_custom_header)));
        self.custom_header_to_net = false;
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
        self.ext_fields.replace_map(ext_fields);
        self.custom_header_to_net = false;
        self
    }

    #[cfg(test)]
    fn set_binary_ext_fields(mut self, ext_fields: BinaryHeaderFields) -> Self {
        self.ext_fields = ExtensionFields::from_rocketmq_raw(ext_fields);
        self.custom_header_to_net = false;
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
    pub(crate) fn set_serialize_type_ref(&mut self, serialize_type: SerializeType) {
        self.serialize_type = serialize_type;
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

    /// Fallibly merges the custom header into dynamic extension fields.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, conversion, alias, or dynamic-field
    /// collision error without mutating this command.
    pub fn try_make_custom_header_to_net(&mut self) -> Result<(), HeaderCodecError> {
        if self.custom_header_to_net {
            return Ok(());
        }

        if let Some(header) = self.command_custom_header_ref() {
            let merged = merge_header_and_dynamic(header, self.ext_fields.as_map())?;
            self.ext_fields.replace_map(merged);
        }
        self.custom_header_to_net = true;
        Ok(())
    }

    #[inline]
    pub fn materialize_custom_header_to_ext_fields(&mut self) {
        self.make_custom_header_to_net();
    }

    fn invalidate_materialized_custom_header(&mut self) {
        if !self.custom_header_to_net {
            return;
        }

        let owned_keys = match (self.command_custom_header_ref(), self.ext_fields.as_map()) {
            (Some(header), Some(fields)) => {
                let mut keys = fields
                    .keys()
                    .filter(|key| header.contains_wire_key(key.as_str()))
                    .cloned()
                    .collect::<Vec<_>>();
                if let Some(legacy_fields) = header.to_map() {
                    keys.extend(legacy_fields.into_keys());
                }
                keys
            }
            _ => Vec::new(),
        };
        if let Some(fields) = self.ext_fields.as_map_mut() {
            for key in owned_keys {
                fields.remove(&key);
            }
        }
        self.custom_header_to_net = false;
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
        self.ext_fields.as_map()
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
        if T::SUPPORTS_HEADER_FIELD_SOURCE {
            if let Some(source) = self.ext_fields.as_field_source() {
                return T::from_field_source(source);
            }
        }
        match self.ext_fields.as_map() {
            None => Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "ExtFields is None".to_string(),
                },
            )),
            Some(header) => T::from(header),
        }
    }

    pub fn decode_command_custom_header_fast<T>(&self) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
        T: Default + CommandCustomHeader,
    {
        if T::SUPPORTS_HEADER_FIELD_SOURCE {
            if let Some(source) = self.ext_fields.as_field_source() {
                return T::from_field_source(source);
            }
        }
        match self.ext_fields.as_map() {
            None => Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "ExtFields is None".to_string(),
                },
            )),
            Some(header) => {
                let mut target = T::default();
                if target.support_fast_codec() {
                    target.decode_fast(header)?;
                    target.check_fields()?;
                    Ok(target)
                } else {
                    T::from(header)
                }
            }
        }
    }

    /// Decodes a required custom request header and classifies any failure at
    /// the request-header boundary.
    ///
    /// `operation` must be a static, low-cardinality description. It is exposed
    /// as structured error context, while the source retains the decoder cause.
    ///
    /// # Errors
    ///
    /// Returns [`rocketmq_error::RocketMQError::RequestHeaderSource`] when the
    /// extension fields are absent or the header cannot be decoded.
    pub fn decode_required_header<T>(&self, operation: &'static str) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
    {
        self.decode_command_custom_header::<T>()
            .map_err(|source| required_header_decode_error(operation, source))
    }

    /// Decodes a required custom request header through the fast codec when the
    /// header supports it and classifies any failure at the request-header
    /// boundary.
    ///
    /// `operation` must be a static, low-cardinality description. It is exposed
    /// as structured error context, while the source retains the decoder cause.
    ///
    /// # Errors
    ///
    /// Returns [`rocketmq_error::RocketMQError::RequestHeaderSource`] when the
    /// extension fields are absent or the header cannot be decoded.
    pub fn decode_required_header_fast<T>(&self, operation: &'static str) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
        T: Default + CommandCustomHeader,
    {
        self.decode_command_custom_header_fast::<T>()
            .map_err(|source| required_header_decode_error(operation, source))
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
        self.ext_fields.get_or_insert_map().insert(key.into(), value.into());
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
        self.ext_fields.as_map()
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

    pub fn try_read_custom_header_ref<T>(&self) -> rocketmq_error::RocketMQResult<&T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_ref() {
            None => Err(Self::custom_header_missing_error::<T>()),
            Some(value) => value
                .as_ref()
                .as_any()
                .downcast_ref::<T>()
                .ok_or_else(Self::custom_header_type_mismatch_error::<T>),
        }
    }

    pub fn read_custom_header_ref_unchecked<T>(&self) -> rocketmq_error::RocketMQResult<&T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_ref::<T>()
    }

    pub fn read_custom_header_mut<T>(&mut self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        let header = self.command_custom_header.as_ref()?;
        if Arc::strong_count(header) != 1 || !header.as_ref().as_any().is::<T>() {
            return None;
        }
        self.invalidate_materialized_custom_header();
        Arc::get_mut(self.command_custom_header.as_mut()?)?
            .as_mut()
            .as_any_mut()
            .downcast_mut::<T>()
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

    pub fn try_read_custom_header_mut<T>(&mut self) -> rocketmq_error::RocketMQResult<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_ref() {
            None => return Err(Self::custom_header_missing_error::<T>()),
            Some(value) if Arc::strong_count(value) != 1 => return Err(Self::custom_header_shared_error()),
            Some(value) if !value.as_ref().as_any().is::<T>() => {
                return Err(Self::custom_header_type_mismatch_error::<T>());
            }
            Some(_) => {}
        }
        self.invalidate_materialized_custom_header();
        Arc::get_mut(
            self.command_custom_header
                .as_mut()
                .ok_or_else(Self::custom_header_missing_error::<T>)?,
        )
        .ok_or_else(Self::custom_header_shared_error)?
        .as_mut()
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(Self::custom_header_type_mismatch_error::<T>)
    }

    pub fn read_custom_header_mut_unchecked<T>(&mut self) -> rocketmq_error::RocketMQResult<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_mut::<T>()
    }

    pub fn command_custom_header_ref(&self) -> Option<&dyn CommandCustomHeader> {
        match self.command_custom_header.as_ref() {
            None => None,
            Some(value) => Some(value.as_ref().as_ref()),
        }
    }

    pub(crate) fn custom_header_encode_capability(&self) -> HeaderEncodeCapability {
        if self.custom_header_to_net {
            HeaderEncodeCapability::MapOnly
        } else {
            self.command_custom_header_ref()
                .map_or(HeaderEncodeCapability::MapOnly, CommandCustomHeader::encode_capability)
        }
    }

    pub fn command_custom_header_mut(&mut self) -> Option<&mut dyn CommandCustomHeader> {
        if self
            .command_custom_header
            .as_ref()
            .is_none_or(|header| Arc::strong_count(header) != 1)
        {
            return None;
        }
        self.invalidate_materialized_custom_header();
        match self.command_custom_header.as_mut() {
            None => None,
            Some(value) => Arc::get_mut(value).map(|header| header.as_mut() as &mut dyn CommandCustomHeader),
        }
    }

    pub fn create_new_request_id() -> i32 {
        next_request_id()
    }

    #[inline]
    pub fn add_ext_field_if_not_exist(&mut self, key: impl Into<CheetahString>, value: impl Into<CheetahString>) {
        self.ext_fields
            .get_or_insert_map()
            .entry(key.into())
            .or_insert(value.into());
    }

    /// Ensures the extension fields map is initialized.
    ///
    /// If `ext_fields` is `None`, initializes it with an empty `HashMap`.
    /// This method is idempotent and safe to call multiple times.
    #[inline]
    pub fn ensure_ext_fields_initialized(&mut self) {
        if self.ext_fields.is_absent() {
            let _ = self.ext_fields.get_or_insert_map();
        }
    }

    fn custom_header_missing_error<T>() -> rocketmq_error::RocketMQError
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
            format: "header",
            message: format!(
                "Command custom header is missing; expected {}.",
                std::any::type_name::<T>()
            ),
        })
    }

    fn custom_header_type_mismatch_error<T>() -> rocketmq_error::RocketMQError
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
            format: "header",
            message: format!(
                "Command custom header type mismatch; expected {}.",
                std::any::type_name::<T>()
            ),
        })
    }

    fn custom_header_shared_error() -> rocketmq_error::RocketMQError {
        rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
            format: "header",
            message: "Command custom header is shared by a cloned command and cannot be mutated safely.".to_string(),
        })
    }
}

#[inline]
fn required_header_decode_error(
    operation: &'static str,
    source: rocketmq_error::RocketMQError,
) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::request_header_source(operation, source)
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
    use std::collections::HashSet;

    use super::*;

    fn json_header_with_ext_fields(ext_fields: &str) -> BytesMut {
        BytesMut::from(
            format!(
                r#"{{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{ext_fields},"serializeTypeCurrentRPC":"JSON"}}"#
            )
            .as_bytes(),
        )
    }

    fn complete_frame(header: &[u8], serialize_type: SerializeType) -> BytesMut {
        let header_length = i32::try_from(header.len()).expect("test header length must fit the wire format");
        let total_length = header_length
            .checked_add(4)
            .expect("test payload length must fit the wire format");
        let mut frame = BytesMut::with_capacity(8 + header.len());
        frame.put_i32(total_length);
        frame.put_i32(RemotingCommand::mark_serialize_type(header_length, serialize_type));
        frame.extend_from_slice(header);
        frame
    }

    fn binary_header(extension_fields: &[u8]) -> Vec<u8> {
        let extension_fields_length =
            i32::try_from(extension_fields.len()).expect("test extension fields must fit the wire format");
        let mut header = BytesMut::with_capacity(21 + extension_fields.len());
        header.put_i16(10);
        header.put_u8(LanguageCode::RUST.get_code());
        header.put_i16(501);
        header.put_i32(7);
        header.put_i32(1);
        header.put_i32(0);
        header.put_i32(extension_fields_length);
        header.extend_from_slice(extension_fields);
        header.to_vec()
    }

    #[test]
    fn request_id_sequence_is_unique_under_contention() {
        const THREADS: usize = 8;
        const IDS_PER_THREAD: usize = 1_024;
        let counter = Arc::new(AtomicI32::new(0));
        let handles = (0..THREADS)
            .map(|_| {
                let counter = Arc::clone(&counter);
                std::thread::spawn(move || {
                    (0..IDS_PER_THREAD)
                        .map(|_| next_request_id_from(&counter))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        let ids = handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("request ID worker must finish"))
            .collect::<HashSet<_>>();

        assert_eq!(ids.len(), THREADS * IDS_PER_THREAD);
        assert_eq!(ids.iter().copied().min(), Some(0));
        assert_eq!(ids.iter().copied().max(), Some((THREADS * IDS_PER_THREAD - 1) as i32));
    }

    #[test]
    fn request_id_sequence_preserves_signed_wrap_behavior() {
        let counter = AtomicI32::new(i32::MAX);

        assert_eq!(next_request_id_from(&counter), i32::MAX);
        assert_eq!(next_request_id_from(&counter), i32::MIN);
    }

    #[test]
    fn request_id_storage_is_a_direct_static_atomic() {
        let source = include_str!("remoting_command.rs");
        let declaration = source
            .lines()
            .find(|line| line.starts_with("static REQUEST_ID:"))
            .expect("request ID declaration");

        assert_eq!(declaration, "static REQUEST_ID: AtomicI32 = AtomicI32::new(0);");
    }

    #[derive(Debug, Default, PartialEq, Eq)]
    struct TestCustomHeader {
        value: i32,
    }

    #[derive(Debug, Default, rocketmq_macros::RequestHeaderCodecV3)]
    #[header(type_id = "rocketmq_protocol::tests::RawStrictAliasHeader")]
    struct RawStrictAliasHeader {
        #[header(key = "canonical", alias = "legacy")]
        value: Option<CheetahString>,
    }

    impl CommandCustomHeader for TestCustomHeader {
        fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
            Some(HashMap::from([(
                CheetahString::from_static_str("value"),
                CheetahString::from_string(self.value.to_string()),
            )]))
        }
    }

    impl FromMap for TestCustomHeader {
        type Error = rocketmq_error::RocketMQError;
        type Target = Self;

        fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self, Self::Error> {
            let value = map
                .get(&CheetahString::from_static_str("value"))
                .ok_or_else(|| {
                    rocketmq_error::RocketMQError::illegal_argument("missing value test custom header field")
                })?
                .parse::<i32>()
                .map_err(|error| {
                    rocketmq_error::RocketMQError::illegal_argument(format!(
                        "invalid value test custom header field: {error}"
                    ))
                })?;
            Ok(Self { value })
        }
    }

    #[derive(Debug)]
    struct OtherCustomHeader;

    impl CommandCustomHeader for OtherCustomHeader {
        fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
            Some(HashMap::new())
        }
    }

    #[derive(Debug)]
    struct NoPreflightLengthHeader;

    impl CommandCustomHeader for NoPreflightLengthHeader {
        fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
            Some(HashMap::from([("field".into(), "value".into())]))
        }

        fn encoded_len_hint(&self) -> usize {
            panic!("ROCKETMQ frame encoding must not preflight custom-header lengths")
        }
    }

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
            format!(
                "{{\"code\":1,\"language\":\"JAVA\",\"version\":{},\"opaque\":1,\"flag\":1,\"remark\":\"remark\",\
                 \"extFields\":{{}},\"serializeTypeCurrentRPC\":\"JSON\"}}",
                crate::version::CURRENT_VERSION as i32
            ),
            serde_json::to_string(&command).unwrap()
        );
    }

    #[test]
    fn add_ext_field_initializes_absent() {
        let mut command = RemotingCommand::create_success_response_command();
        assert!(command.ext_fields.is_absent());

        command.add_ext_field("key", "value");

        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get("key"))
                .map(CheetahString::as_str),
            Some("value")
        );
    }

    #[test]
    fn add_ext_field_if_not_exist_initializes_absent() {
        let mut command = RemotingCommand::create_success_response_command();
        assert!(command.ext_fields.is_absent());

        command.add_ext_field_if_not_exist("key", "first");
        command.add_ext_field_if_not_exist("key", "second");

        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get("key"))
                .map(CheetahString::as_str),
            Some("first")
        );
    }

    #[test]
    fn add_ext_field_preserves_materialized_fields() {
        let mut command = RemotingCommand::create_success_response_command().set_ext_fields(HashMap::from([(
            CheetahString::from_static_str("existing"),
            CheetahString::from_static_str("preserved"),
        )]));

        command.add_ext_field("added", "value");

        let fields = command.ext_fields().expect("extension fields should exist");
        assert_eq!(fields.get("existing").map(CheetahString::as_str), Some("preserved"));
        assert_eq!(fields.get("added").map(CheetahString::as_str), Some("value"));
    }

    #[test]
    fn add_ext_field_preserves_json_raw_fields() {
        let mut header = json_header_with_ext_fields(r#"{"existing":"preserved"}"#);
        let header_length = header.len();
        let mut command = RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON)
            .unwrap()
            .unwrap();
        assert!(command.ext_fields.is_json_raw());

        command.add_ext_field("added", "value");

        let fields = command.ext_fields().expect("extension fields should exist");
        assert_eq!(fields.get("existing").map(CheetahString::as_str), Some("preserved"));
        assert_eq!(fields.get("added").map(CheetahString::as_str), Some("value"));
    }

    #[test]
    fn add_ext_field_preserves_rocketmq_raw_fields() {
        let mut source = RemotingCommand::create_success_response_command()
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_ext_fields(HashMap::from([(
                CheetahString::from_static_str("existing"),
                CheetahString::from_static_str("preserved"),
            )]));
        let mut encoded = BytesMut::new();
        source.try_fast_header_encode(&mut encoded).unwrap();
        let mut command = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
        assert!(command.ext_fields.is_rocketmq_raw());

        command.add_ext_field("added", "value");

        let fields = command.ext_fields().expect("extension fields should exist");
        assert_eq!(fields.get("existing").map(CheetahString::as_str), Some("preserved"));
        assert_eq!(fields.get("added").map(CheetahString::as_str), Some("value"));
    }

    #[test]
    fn add_ext_field_coexists_with_typed_header() {
        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let mut command =
                RemotingCommand::create_success_response_command_with_header(TestCustomHeader { value: 7 })
                    .set_serialize_type(serialize_type);
            command.add_ext_field("dynamic", "preserved");
            let mut encoded = BytesMut::new();

            command.try_fast_header_encode(&mut encoded).unwrap();
            let decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

            assert_eq!(
                decoded
                    .ext_fields()
                    .and_then(|fields| fields.get("value"))
                    .map(CheetahString::as_str),
                Some("7")
            );
            assert_eq!(
                decoded
                    .ext_fields()
                    .and_then(|fields| fields.get("dynamic"))
                    .map(CheetahString::as_str),
                Some("preserved")
            );
        }
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

    #[test]
    fn frame_length_checks_each_wire_boundary_and_limit_plus_one() {
        const MAX_HEADER_LENGTH: usize = 0x00ff_ffff;
        let (total, marked) =
            RemotingCommand::checked_frame_lengths(MAX_HEADER_LENGTH, 0, SerializeType::ROCKETMQ).unwrap();
        assert_eq!(total, 4 + MAX_HEADER_LENGTH as i32);
        assert_eq!(parse_header_length(marked), MAX_HEADER_LENGTH);
        assert!(RemotingCommand::checked_frame_lengths(MAX_HEADER_LENGTH + 1, 0, SerializeType::ROCKETMQ).is_err());

        let max_body = i32::MAX as usize - 4;
        let (total, _) = RemotingCommand::checked_frame_lengths(0, max_body, SerializeType::JSON).unwrap();
        assert_eq!(total, i32::MAX);
        assert!(RemotingCommand::checked_frame_lengths(0, max_body + 1, SerializeType::JSON).is_err());
    }

    #[test]
    fn structured_frame_fixture_decodes_canonical_json_extension_fields() {
        let header = br#"{"code":10,"language":"RUST","version":501,"opaque":7,"flag":0,"remark":null,"extFields":{"queueId":"1"},"serializeTypeCurrentRPC":"JSON"}"#;
        let mut frame = complete_frame(header, SerializeType::JSON);
        frame.extend_from_slice(b"next-frame");

        let command = RemotingCommand::decode(&mut frame)
            .expect("canonical JSON frame must decode")
            .expect("canonical JSON frame is complete");

        assert_eq!(frame.as_ref(), b"next-frame");
        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get("queueId"))
                .map(CheetahString::as_str),
            Some("1")
        );
    }

    #[test]
    fn structured_frame_fixture_decodes_flexible_json_extension_fields() {
        let header = r#" { "version" : 501, "code" : -7, "language" : "JAVA", "opaque" : 9, "flag" : 1, "remark" : "火箭", "extFields" : { "escaped\u004bey" : "quote\"slash\\solidus\/", "unicode" : "中🚀" }, "serializeTypeCurrentRPC" : "JSON" } "#;
        let mut frame = complete_frame(header.as_bytes(), SerializeType::JSON);

        let command = RemotingCommand::decode(&mut frame)
            .expect("flexible JSON frame must decode")
            .expect("flexible JSON frame is complete");

        assert!(frame.is_empty());
        assert_eq!(command.remark().map(CheetahString::as_str), Some("火箭"));
        let fields = command.ext_fields().expect("flexible JSON extension fields");
        assert_eq!(
            fields.get("escapedKey").map(CheetahString::as_str),
            Some("quote\"slash\\solidus/")
        );
        assert_eq!(fields.get("unicode").map(CheetahString::as_str), Some("中🚀"));
    }

    #[test]
    fn structured_frame_fixture_decodes_two_rocketmq_extension_fields() {
        let extension_fields = [
            0, 5, b'a', b'l', b'p', b'h', b'a', 0, 0, 0, 5, b'f', b'i', b'r', b's', b't', 0, 4, b'z', b'e', b't', b'a',
            0, 0, 0, 4, b'l', b'a', b's', b't',
        ];
        let mut frame = complete_frame(&binary_header(&extension_fields), SerializeType::ROCKETMQ);

        let command = RemotingCommand::decode(&mut frame)
            .expect("ROCKETMQ frame must decode")
            .expect("ROCKETMQ frame is complete");

        assert!(frame.is_empty());
        let fields = command.ext_fields().expect("ROCKETMQ extension fields");
        assert_eq!(fields.get("alpha").map(CheetahString::as_str), Some("first"));
        assert_eq!(fields.get("zeta").map(CheetahString::as_str), Some("last"));
    }

    #[test]
    fn incomplete_outer_frame_is_left_untouched() {
        let mut frame = BytesMut::from(&[0, 0, 0, 8, 0, 0, 0, 0][..]);
        let before = frame.clone();

        assert!(RemotingCommand::decode(&mut frame).unwrap().is_none());
        assert_eq!(frame, before);
    }

    #[test]
    fn malformed_input_regressions_reject_complete_extension_field_frames() {
        let invalid_json_header = [
            br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{"key":""#
                .as_slice(),
            &[0xff],
            br#""},"serializeTypeCurrentRPC":"JSON"}"#.as_slice(),
        ]
        .concat();
        let unterminated_json_header =
            br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{"key":"value"#;
        assert_eq!(unterminated_json_header.len(), 99);
        let invalid_binary_fields = [0, 1, 0xff, 0, 0, 0, 1, b'v'];
        let overlong_binary_fields = [0, 1, b'k', 0, 0, 4, 0];
        let mut cases = [
            (
                "invalid JSON UTF-8",
                complete_frame(&invalid_json_header, SerializeType::JSON),
            ),
            (
                "unterminated JSON extension field",
                complete_frame(unterminated_json_header, SerializeType::JSON),
            ),
            (
                "invalid ROCKETMQ UTF-8",
                complete_frame(&binary_header(&invalid_binary_fields), SerializeType::ROCKETMQ),
            ),
            (
                "overlong ROCKETMQ extension-field value",
                complete_frame(&binary_header(&overlong_binary_fields), SerializeType::ROCKETMQ),
            ),
        ];

        for (name, frame) in &mut cases {
            assert!(RemotingCommand::decode(frame).is_err(), "{name}");
            assert!(frame.is_empty(), "complete malformed frame must be consumed: {name}");
        }
    }

    #[test]
    fn rocketmq_frame_encoding_does_not_preflight_custom_header_lengths() {
        let mut command = RemotingCommand::create_request_command(1, NoPreflightLengthHeader)
            .set_serialize_type(SerializeType::ROCKETMQ);
        let mut encoded = BytesMut::new();

        command.try_fast_header_encode(&mut encoded).unwrap();

        let decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("field"))
                .map(CheetahString::as_str),
            Some("value")
        );
    }

    #[test]
    fn try_read_custom_header_ref_reports_missing_header() {
        let command = RemotingCommand::create_remoting_command(1);

        let error = command.try_read_custom_header_ref::<TestCustomHeader>().unwrap_err();

        assert!(error.to_string().contains("missing"));
        assert!(command.read_custom_header_ref_unchecked::<TestCustomHeader>().is_err());
    }

    #[test]
    fn try_read_custom_header_ref_reports_type_mismatch() {
        let command = RemotingCommand::create_request_command(1, TestCustomHeader::default());

        let error = command.try_read_custom_header_ref::<OtherCustomHeader>().unwrap_err();

        assert!(error.to_string().contains("type mismatch"));
        assert!(command.read_custom_header_ref_unchecked::<OtherCustomHeader>().is_err());
    }

    #[test]
    fn try_read_custom_header_mut_updates_expected_header() {
        let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });

        let header = command.try_read_custom_header_mut::<TestCustomHeader>().unwrap();
        header.value = 9;

        let header = command.try_read_custom_header_ref::<TestCustomHeader>().unwrap();
        assert_eq!(header.value, 9);
    }

    #[test]
    fn clone_preserves_concrete_header_without_eagerly_hiding_dynamic_collisions() {
        let original =
            RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 }).set_ext_fields(HashMap::from([
                (CheetahString::from("hook-field"), CheetahString::from("preserved")),
            ]));
        let mut cloned = original.clone();

        assert_eq!(
            cloned.try_read_custom_header_ref::<TestCustomHeader>().unwrap().value,
            7
        );
        assert_eq!(
            cloned
                .ext_fields()
                .and_then(|fields| fields.get("hook-field"))
                .map(CheetahString::as_str),
            Some("preserved")
        );
        assert!(cloned.ext_fields().is_none_or(|fields| !fields.contains_key("value")));
        cloned.try_make_custom_header_to_net().unwrap();
        assert_eq!(
            cloned
                .ext_fields()
                .and_then(|fields| fields.get("value"))
                .map(CheetahString::as_str),
            Some("7")
        );
    }

    #[test]
    fn typed_dynamic_conflict_fails_json_and_rocketmq_with_full_rollback() {
        use crate::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;

        for serialize_type in [SerializeType::JSON, SerializeType::ROCKETMQ] {
            let header = SendMessageResponseHeader::new("typed".into(), 1, 2, None, None, None);
            let mut command = RemotingCommand::create_success_response_command_with_header(header)
                .set_serialize_type(serialize_type)
                .set_ext_fields(HashMap::from([("msgId".into(), "dynamic".into())]));
            let mut destination = BytesMut::from(&b"prefix"[..]);

            let error = command.try_fast_header_encode(&mut destination).unwrap_err();

            assert_eq!(error.kind(), rocketmq_error::ErrorKind::RequestHeaderError);
            assert_eq!(destination.as_ref(), b"prefix");
            assert_eq!(
                command
                    .ext_fields()
                    .and_then(|fields| fields.get("msgId"))
                    .map(CheetahString::as_str),
                Some("dynamic")
            );
        }
    }

    #[test]
    fn generated_fast_header_streams_json_without_materializing_extension_fields() {
        use crate::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;

        let header = SendMessageResponseHeader::new(
            "msg-\"\\\n-主题".into(),
            -3,
            i64::MAX,
            Some(CheetahString::new()),
            Some("batch-a".into()),
            None,
        );
        let mut command = RemotingCommand::create_success_response_command_with_header(header)
            .set_language(LanguageCode::GO)
            .set_version(501)
            .set_opaque(7)
            .set_remark("remark-\"\\\n-主题")
            .set_serialize_type(SerializeType::JSON);
        let mut materialized = command.clone();
        materialized.try_make_custom_header_to_net().unwrap();
        let expected = serde_json::to_value(&materialized).unwrap();

        let mut encoded = BytesMut::new();
        command.try_fast_header_encode(&mut encoded).unwrap();

        assert!(!command.custom_header_to_net);
        assert!(command.ext_fields.is_absent());
        let marked_header_length = i32::from_be_bytes(encoded[4..8].try_into().unwrap());
        let header_length = (marked_header_length & 0x00ff_ffff) as usize;
        let actual: serde_json::Value = serde_json::from_slice(&encoded[8..8 + header_length]).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual["extFields"]["transactionId"], "");
        assert_eq!(actual["extFields"]["queueId"], "-3");
        assert_eq!(actual["extFields"]["queueOffset"], i64::MAX.to_string());
    }

    #[test]
    fn present_empty_is_preserved_in_map_and_normalized_after_rocketmq_decode() {
        use crate::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;

        let header = SendMessageResponseHeader::new("msg".into(), 1, 2, Some(CheetahString::new()), None, None);
        let mut materialized = RemotingCommand::create_success_response_command_with_header(header);
        materialized.try_make_custom_header_to_net().unwrap();
        assert_eq!(
            materialized
                .ext_fields()
                .and_then(|fields| fields.get("transactionId"))
                .map(CheetahString::as_str),
            Some("")
        );

        let header = SendMessageResponseHeader::new("msg".into(), 1, 2, Some(CheetahString::new()), None, None);
        let mut command = RemotingCommand::create_success_response_command_with_header(header)
            .set_serialize_type(SerializeType::ROCKETMQ);
        let mut encoded = BytesMut::new();
        command.try_fast_header_encode(&mut encoded).unwrap();
        let decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

        assert!(decoded
            .ext_fields()
            .is_some_and(|fields| !fields.contains_key("transactionId")));
    }

    #[test]
    fn rocketmq_decode_keeps_extension_fields_raw_until_compatibility_map_access() {
        let mut command = RemotingCommand::create_remoting_command(1)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_ext_fields(HashMap::from([
                (
                    CheetahString::from_static_str("alpha"),
                    CheetahString::from_static_str("first"),
                ),
                (
                    CheetahString::from_static_str("beta"),
                    CheetahString::from_static_str("second"),
                ),
            ]));
        let mut encoded = BytesMut::new();
        command.try_fast_header_encode(&mut encoded).unwrap();

        let decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

        assert!(decoded.ext_fields.is_rocketmq_raw());
        assert!(!decoded.ext_fields.has_materialized_map());
        let cloned = decoded.clone();
        assert!(cloned.ext_fields.is_rocketmq_raw());
        assert!(!cloned.ext_fields.has_materialized_map());

        let fields = decoded.ext_fields().unwrap();
        assert_eq!(fields.get("alpha").map(CheetahString::as_str), Some("first"));
        assert_eq!(fields.get("beta").map(CheetahString::as_str), Some("second"));
        assert!(decoded.ext_fields.is_rocketmq_raw());
        assert!(decoded.ext_fields.has_materialized_map());
        assert!(!cloned.ext_fields.has_materialized_map());

        let json = serde_json::to_value(&cloned).unwrap();
        assert_eq!(json["extFields"]["alpha"], "first");
        assert_eq!(json["extFields"]["beta"], "second");
        assert!(cloned.ext_fields.has_materialized_map());
    }

    #[test]
    fn json_decode_keeps_extension_fields_raw_until_compatibility_map_access() {
        use crate::rpc::rpc_request_header::RpcRequestHeader;

        let mut header = json_header_with_ext_fields(
            r#"{"ns":"first","namespace":"legacy","ns":"last","nsd":"false","empty":"","escaped":"line\n\"quoted\"\\tail","unicode":"火箭"}"#,
        );
        let header_length = header.len();
        let command = RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON)
            .unwrap()
            .unwrap();

        assert!(command.ext_fields.is_json_raw());
        assert!(!command.ext_fields.has_materialized_map());
        let cloned = command.clone();
        assert!(cloned.ext_fields.is_json_raw());
        assert!(!cloned.ext_fields.has_materialized_map());

        let decoded = command.decode_command_custom_header::<RpcRequestHeader>().unwrap();
        assert_eq!(decoded.namespace.as_deref(), Some("last"));
        assert_eq!(decoded.namespaced, Some(false));
        assert!(!command.ext_fields.has_materialized_map());

        let fields = command.ext_fields().unwrap();
        assert_eq!(fields.get("ns").map(CheetahString::as_str), Some("last"));
        assert_eq!(fields.get("empty").map(CheetahString::as_str), Some(""));
        assert_eq!(
            fields.get("escaped").map(CheetahString::as_str),
            Some("line\n\"quoted\"\\tail")
        );
        assert_eq!(fields.get("unicode").map(CheetahString::as_str), Some("火箭"));
        assert!(command.ext_fields.is_json_raw());
        assert!(command.ext_fields.has_materialized_map());
        assert!(!cloned.ext_fields.has_materialized_map());

        let json = serde_json::to_value(&cloned).unwrap();
        assert_eq!(json["extFields"]["ns"], "last");
        assert_eq!(json["extFields"]["empty"], "");
        assert_eq!(json["extFields"]["unicode"], "火箭");
        assert!(cloned.ext_fields.is_json_raw());
        assert!(cloned.ext_fields.has_materialized_map());
    }

    #[test]
    fn display_summarizes_raw_extension_fields_without_materializing_or_exposing_values() {
        let mut json_header = json_header_with_ext_fields(r#"{"token":"json-secret"}"#);
        let json_length = json_header.len();
        let json = RemotingCommand::header_decode(&mut json_header, json_length, SerializeType::JSON)
            .unwrap()
            .unwrap();

        let mut rocketmq_source = RemotingCommand::create_remoting_command(1)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_ext_fields(HashMap::from([(
                CheetahString::from_static_str("token"),
                CheetahString::from_static_str("rocketmq-secret"),
            )]));
        let mut encoded = BytesMut::new();
        rocketmq_source.try_fast_header_encode(&mut encoded).unwrap();
        let rocketmq = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

        let json_display = json.to_string();
        let rocketmq_display = rocketmq.to_string();

        assert!(json_display.contains("JsonRaw(count=1, materialized=false)"));
        assert!(rocketmq_display.contains("RocketMqRaw(count=1, materialized=false)"));
        assert!(!json_display.contains("json-secret"));
        assert!(!rocketmq_display.contains("rocketmq-secret"));
        assert!(!json.ext_fields.has_materialized_map());
        assert!(!rocketmq.ext_fields.has_materialized_map());
    }

    #[test]
    fn populated_raw_cache_clone_mutation_preserves_the_original_for_both_protocols() {
        let mut json_header = json_header_with_ext_fields(r#"{"original":"json"}"#);
        let json_length = json_header.len();
        let json = RemotingCommand::header_decode(&mut json_header, json_length, SerializeType::JSON)
            .unwrap()
            .unwrap();

        let mut rocketmq_source = RemotingCommand::create_remoting_command(1)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_ext_fields(HashMap::from([(
                CheetahString::from_static_str("original"),
                CheetahString::from_static_str("rocketmq"),
            )]));
        let mut encoded = BytesMut::new();
        rocketmq_source.try_fast_header_encode(&mut encoded).unwrap();
        let rocketmq = RemotingCommand::decode(&mut encoded).unwrap().unwrap();

        for original in [json, rocketmq] {
            assert!(original
                .ext_fields()
                .is_some_and(|fields| fields.contains_key("original")));
            assert!(original.ext_fields.has_materialized_map());

            let mut cloned = original.clone();
            cloned.add_ext_field("cloned", "only");

            assert!(original
                .ext_fields()
                .is_some_and(|fields| !fields.contains_key("cloned")));
            assert!(cloned
                .ext_fields()
                .is_some_and(|fields| fields.get("cloned").is_some_and(|value| value == "only")));
        }
    }

    #[test]
    fn json_decode_preserves_absent_null_and_empty_extension_fields() {
        let mut missing = BytesMut::from(
            r#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"serializeTypeCurrentRPC":"JSON"}"#
                .as_bytes(),
        );
        let missing_length = missing.len();
        let missing_command = RemotingCommand::header_decode(&mut missing, missing_length, SerializeType::JSON)
            .unwrap()
            .unwrap();
        assert!(missing_command.ext_fields().is_none());

        let mut null = json_header_with_ext_fields("null");
        let null_length = null.len();
        let null_command = RemotingCommand::header_decode(&mut null, null_length, SerializeType::JSON)
            .unwrap()
            .unwrap();
        assert!(null_command.ext_fields().is_none());

        let mut empty = json_header_with_ext_fields("{}");
        let empty_length = empty.len();
        let empty_command = RemotingCommand::header_decode(&mut empty, empty_length, SerializeType::JSON)
            .unwrap()
            .unwrap();
        assert!(empty_command.ext_fields.is_json_raw());
        assert!(!empty_command.ext_fields.has_materialized_map());
        assert!(empty_command.ext_fields().unwrap().is_empty());
        assert!(empty_command.ext_fields.has_materialized_map());
    }

    #[test]
    fn v3_production_json_decode_uses_raw_fields_without_materializing_the_map() {
        use crate::protocol::header::notification_request_header::NotificationRequestHeader;

        let header = NotificationRequestHeader {
            consumer_group: "group-a".into(),
            topic: "topic-a".into(),
            queue_id: 3,
            poll_time: 15_000,
            born_time: 1_720_000_000_000,
            ..Default::default()
        };
        let mut encoded = BytesMut::new();
        RemotingCommand::create_request_command(1, header)
            .set_serialize_type(SerializeType::JSON)
            .try_fast_header_encode(&mut encoded)
            .unwrap();
        let command = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
        assert!(command.ext_fields.is_json_raw());
        assert!(!command.ext_fields.has_materialized_map());

        let standard = command
            .decode_command_custom_header::<NotificationRequestHeader>()
            .unwrap();
        assert_eq!(standard.queue_id, 3);
        assert!(!command.ext_fields.has_materialized_map());

        let fast = command
            .decode_command_custom_header_fast::<NotificationRequestHeader>()
            .unwrap();
        assert_eq!(fast.queue_id, 3);
        assert!(!command.ext_fields.has_materialized_map());
    }

    #[test]
    fn json_raw_fallback_and_mutation_materialize_the_compatibility_map() {
        let mut header = json_header_with_ext_fields(r#"{"value":"7"}"#);
        let header_length = header.len();
        let mut command = RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON)
            .unwrap()
            .unwrap();
        assert!(command.ext_fields.is_json_raw());
        assert!(!command.ext_fields.has_materialized_map());

        let decoded = command.decode_command_custom_header::<TestCustomHeader>().unwrap();
        assert_eq!(decoded.value, 7);
        assert!(command.ext_fields.is_json_raw());
        assert!(command.ext_fields.has_materialized_map());

        command.add_ext_field("added", "field");
        assert!(!command.ext_fields.is_json_raw());
        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get("value"))
                .map(CheetahString::as_str),
            Some("7")
        );
        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get("added"))
                .map(CheetahString::as_str),
            Some("field")
        );
    }

    #[test]
    fn json_envelope_rejects_non_string_extension_field_values() {
        for value in ["7", "true", "null", "{}", "[]"] {
            let mut header = json_header_with_ext_fields(&format!(r#"{{"value":{value}}}"#));
            let header_length = header.len();

            assert!(RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON).is_err());
        }
    }

    #[test]
    fn v3_production_decode_uses_raw_fields_without_materializing_the_map() {
        use crate::protocol::header::notification_request_header::NotificationRequestHeader;

        let header = NotificationRequestHeader {
            consumer_group: "group-a".into(),
            topic: "topic-a".into(),
            queue_id: 3,
            poll_time: 15_000,
            born_time: 1_720_000_000_000,
            ..Default::default()
        };
        let mut encoded = BytesMut::new();
        RemotingCommand::create_request_command(1, header)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .try_fast_header_encode(&mut encoded)
            .unwrap();
        let command = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
        assert!(command.ext_fields.is_rocketmq_raw());
        assert!(!command.ext_fields.has_materialized_map());

        let standard = command
            .decode_command_custom_header::<NotificationRequestHeader>()
            .unwrap();
        assert_eq!(standard.queue_id, 3);
        assert!(!command.ext_fields.has_materialized_map());

        let fast = command
            .decode_command_custom_header_fast::<NotificationRequestHeader>()
            .unwrap();
        assert_eq!(fast.queue_id, 3);
        assert!(!command.ext_fields.has_materialized_map());
    }

    #[test]
    fn raw_direct_decode_preserves_duplicate_and_alias_precedence_without_a_map() {
        use crate::rpc::rpc_request_header::RpcRequestHeader;

        fn append_field(out: &mut BytesMut, key: &str, value: &str) {
            out.put_u16(key.len() as u16);
            out.extend_from_slice(key.as_bytes());
            out.put_i32(value.len() as i32);
            out.extend_from_slice(value.as_bytes());
        }

        let mut payload = BytesMut::new();
        append_field(&mut payload, "ns", "first-canonical");
        append_field(&mut payload, "namespace", "legacy");
        append_field(&mut payload, "ns", "last-canonical");
        append_field(&mut payload, "nsd", "true");
        append_field(&mut payload, "nsd", "false");
        let command = RemotingCommand::create_remoting_command(1)
            .set_binary_ext_fields(BinaryHeaderFields::new(payload.freeze()).unwrap());

        let decoded = command.decode_command_custom_header::<RpcRequestHeader>().unwrap();

        assert_eq!(decoded.namespace.as_deref(), Some("last-canonical"));
        assert_eq!(decoded.namespaced, Some(false));
        assert!(command.ext_fields.is_rocketmq_raw());
        assert!(!command.ext_fields.has_materialized_map());

        let mut converged = BytesMut::new();
        append_field(&mut converged, "canonical", "superseded");
        append_field(&mut converged, "legacy", "final");
        append_field(&mut converged, "canonical", "final");
        let command = RemotingCommand::create_remoting_command(1)
            .set_binary_ext_fields(BinaryHeaderFields::new(converged.freeze()).unwrap());
        let decoded = command.decode_command_custom_header::<RawStrictAliasHeader>().unwrap();
        assert_eq!(decoded.value.as_deref(), Some("final"));
        assert!(!command.ext_fields.has_materialized_map());

        let mut conflicting = BytesMut::new();
        append_field(&mut conflicting, "canonical", "canonical-value");
        append_field(&mut conflicting, "legacy", "legacy-value");
        let command = RemotingCommand::create_remoting_command(1)
            .set_binary_ext_fields(BinaryHeaderFields::new(conflicting.freeze()).unwrap());
        assert!(command.decode_command_custom_header::<RawStrictAliasHeader>().is_err());
        assert!(!command.ext_fields.has_materialized_map());
    }

    #[test]
    fn mutating_raw_extension_fields_materializes_and_invalidates_the_raw_view() {
        let mut command = RemotingCommand::create_remoting_command(1)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_ext_fields(HashMap::from([(
                CheetahString::from_static_str("alpha"),
                CheetahString::from_static_str("first"),
            )]));
        let mut encoded = BytesMut::new();
        command.try_fast_header_encode(&mut encoded).unwrap();
        let mut decoded = RemotingCommand::decode(&mut encoded).unwrap().unwrap();
        assert!(decoded.ext_fields.is_rocketmq_raw());

        decoded.add_ext_field("beta", "second");

        assert!(!decoded.ext_fields.is_rocketmq_raw());
        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("alpha"))
                .map(CheetahString::as_str),
            Some("first")
        );
        assert_eq!(
            decoded
                .ext_fields()
                .and_then(|fields| fields.get("beta"))
                .map(CheetahString::as_str),
            Some("second")
        );
    }

    #[test]
    fn rocketmq_envelope_rejects_invalid_utf8_before_returning_a_command() {
        let mut command = RemotingCommand::create_remoting_command(1)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_ext_fields(HashMap::from([(
                CheetahString::from_static_str("key"),
                CheetahString::from_static_str("v"),
            )]));
        let mut encoded = BytesMut::new();
        command.try_fast_header_encode(&mut encoded).unwrap();
        let last = encoded.len() - 1;
        encoded[last] = 0xff;

        assert!(RemotingCommand::decode(&mut encoded).is_err());
    }

    #[test]
    fn materialize_custom_header_to_ext_fields_keeps_header_decodable_and_header_object_visible() {
        let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });

        command.materialize_custom_header_to_ext_fields();

        assert!(command.command_custom_header_ref().is_some());
        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get(&CheetahString::from_static_str("value")))
                .map(CheetahString::as_str),
            Some("7")
        );
        let decoded = command.decode_command_custom_header::<TestCustomHeader>().unwrap();
        assert_eq!(decoded.value, 7);
    }

    #[test]
    fn required_header_decode_preserves_valid_standard_and_fast_results() {
        let command = RemotingCommand::create_remoting_command(1).set_ext_fields(HashMap::from([(
            CheetahString::from_static_str("value"),
            CheetahString::from_static_str("7"),
        )]));

        let standard = command
            .decode_required_header::<TestCustomHeader>("decode test header")
            .unwrap();
        let fast = command
            .decode_required_header_fast::<TestCustomHeader>("decode test header")
            .unwrap();

        assert_eq!(standard, TestCustomHeader { value: 7 });
        assert_eq!(fast, standard);
    }

    #[test]
    fn required_header_decode_maps_missing_malformed_and_overflow_to_typed_error() {
        let cases = [
            ("missing extension fields", RemotingCommand::create_remoting_command(1)),
            (
                "missing required field",
                RemotingCommand::create_remoting_command(1).set_ext_fields(HashMap::new()),
            ),
            (
                "malformed numeric field",
                RemotingCommand::create_remoting_command(1).set_ext_fields(HashMap::from([(
                    CheetahString::from_static_str("value"),
                    CheetahString::from_static_str("not-a-number"),
                )])),
            ),
            (
                "overflowing numeric field",
                RemotingCommand::create_remoting_command(1).set_ext_fields(HashMap::from([(
                    CheetahString::from_static_str("value"),
                    CheetahString::from_static_str("2147483648"),
                )])),
            ),
        ];

        for (case, command) in cases {
            let standard = command
                .decode_required_header::<TestCustomHeader>("decode test header")
                .expect_err(case);
            assert_eq!(standard.kind(), rocketmq_error::ErrorKind::RequestHeaderError, "{case}");
            assert!(std::error::Error::source(&standard).is_some(), "{case}");

            let fast = command
                .decode_required_header_fast::<TestCustomHeader>("decode test header")
                .expect_err(case);
            assert_eq!(fast.kind(), rocketmq_error::ErrorKind::RequestHeaderError, "{case}");
            assert!(std::error::Error::source(&fast).is_some(), "{case}");
        }
    }

    #[test]
    fn read_custom_header_mut_invalidates_materialized_ext_fields() {
        let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });
        command.materialize_custom_header_to_ext_fields();

        let header = command
            .read_custom_header_mut::<TestCustomHeader>()
            .expect("test header should be available");
        header.value = 9;
        command.make_custom_header_to_net();

        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get(&CheetahString::from_static_str("value")))
                .map(CheetahString::as_str),
            Some("9")
        );
        let decoded = command.decode_command_custom_header::<TestCustomHeader>().unwrap();
        assert_eq!(decoded.value, 9);
    }

    #[test]
    fn failed_shared_header_mutation_keeps_materialized_fields_intact() {
        let mut command = RemotingCommand::create_request_command(1, TestCustomHeader { value: 7 });
        command.try_make_custom_header_to_net().unwrap();
        let _clone = command.clone();

        assert!(command.try_read_custom_header_mut::<TestCustomHeader>().is_err());

        assert!(command.custom_header_to_net);
        assert_eq!(
            command
                .ext_fields()
                .and_then(|fields| fields.get("value"))
                .map(CheetahString::as_str),
            Some("7")
        );
    }

    #[test]
    fn fast_rocketmq_encode_frame_decodes_with_body() {
        let body = Bytes::from_static(b"rocketmq-body");
        let mut command = RemotingCommand::create_remoting_command(100)
            .set_language(LanguageCode::RUST)
            .set_opaque(7)
            .set_serialize_type(SerializeType::ROCKETMQ)
            .set_body(body.clone());

        let mut encoded = BytesMut::new();
        command.fast_header_encode(&mut encoded);
        encoded.extend_from_slice(&body);

        let decoded = RemotingCommand::decode(&mut encoded)
            .expect("fast rocketmq frame should decode")
            .expect("complete frame should produce command");

        assert_eq!(decoded.code(), 100);
        assert_eq!(decoded.opaque(), 7);
        assert_eq!(decoded.get_body(), Some(&body));
    }
}
