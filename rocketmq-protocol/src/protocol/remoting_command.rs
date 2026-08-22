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

use bytes::Bytes;
use cheetah_string::CheetahString;
use serde::ser::SerializeMap;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::RemotingCommandType;
use super::SerializeType;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::header_codec::BinaryHeaderFields;
use crate::protocol::header_codec::JsonHeaderFields;
use crate::protocol::LanguageCode;

#[cfg(test)]
use crate::protocol::command_custom_header::FromMap;
#[cfg(test)]
use bytes::BufMut;
#[cfg(test)]
use bytes::BytesMut;

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

mod accessors;
mod constructors;
mod custom_header;
mod decode;
mod encode;
mod extension_fields;
mod flags;
mod frame;

use extension_fields::ExtensionFields;

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

    /// Convert custom header to network format (merge into ext_fields)
    #[inline]
    pub fn make_custom_header_to_net(&mut self) {
        let _ = self.try_make_custom_header_to_net();
    }

    #[inline]
    pub fn materialize_custom_header_to_ext_fields(&mut self) {
        self.make_custom_header_to_net();
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
