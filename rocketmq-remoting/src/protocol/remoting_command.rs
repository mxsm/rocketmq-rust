/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::fmt;
use std::hint;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Once;

use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use lazy_static::lazy_static;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_error::RocketmqError;
use rocketmq_rust::ArcMut;
use serde::Deserialize;
use serde::Serialize;
use tracing::error;

use super::RemotingCommandType;
use super::RemotingSerializable;
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

lazy_static! {
    static ref requestId: Arc<AtomicI32> = Arc::new(AtomicI32::new(0));
    static ref CONFIG_VERSION: i32 = {
        EnvUtils::get_property(REMOTING_VERSION_KEY)
            .unwrap_or(String::from("0"))
            .parse::<i32>()
            .unwrap_or(0)
    };
    static ref INIT: Once = Once::new();
    pub static ref SERIALIZE_TYPE_CONFIG_IN_THIS_SERVER: SerializeType = {
        let protocol = std::env::var(SERIALIZE_TYPE_PROPERTY).unwrap_or_else(|_| {
            std::env::var(SERIALIZE_TYPE_ENV).unwrap_or_else(|_| "".to_string())
        });
        match protocol.as_str() {
            "JSON" => SerializeType::JSON,
            "ROCKETMQ" => SerializeType::ROCKETMQ,
            _ => SerializeType::JSON,
        }
    };
}

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
            "RemotingCommand [code={}, language={}, version={}, opaque={}, flag(B)={:b}, \
             remark={}, extFields={:?}, serializeTypeCurrentRPC={}]",
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
        let opaque = requestId.fetch_add(1, Ordering::AcqRel);
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
        let mut command = Self::default()
            .set_code(code.into())
            .set_command_custom_header(header);
        set_cmd_version(&mut command);
        command
    }

    pub fn create_remoting_command(code: impl Into<i32>) -> Self {
        let command = Self::default();
        command.set_code(code.into())
    }

    pub fn get_and_add() -> i32 {
        requestId.fetch_add(1, Ordering::AcqRel)
    }

    pub fn create_response_command_with_code(code: impl Into<i32>) -> Self {
        Self::default().set_code(code).mark_response_type()
    }

    pub fn create_response_command_with_code_remark(
        code: impl Into<i32>,
        remark: impl Into<CheetahString>,
    ) -> Self {
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

    pub fn create_response_command_with_header(
        header: impl CommandCustomHeader + Sync + Send + 'static,
    ) -> Self {
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

    pub fn header_encode(&mut self) -> Option<Bytes> {
        self.make_custom_header_to_net();
        if SerializeType::ROCKETMQ == self.serialize_type {
            Some(RocketMQSerializable::rocket_mq_protocol_encode_bytes(self))
        } else {
            match self.encode() {
                Ok(value) => Some(Bytes::from(value)),
                Err(e) => {
                    error!("Failed to encode generic: {}", e);
                    None
                }
            }
        }
    }

    pub fn encode_header(&mut self) -> Option<Bytes> {
        if let Some(body) = &self.body {
            let size = body.len();
            self.encode_header_with_body_length(size)
        } else {
            self.encode_header_with_body_length(0)
        }
    }
    pub fn encode_header_with_body_length(&mut self, body_length: usize) -> Option<Bytes> {
        //for zero copy
        // 1> header length size
        let mut length = 4;

        // 2> header data length
        let header_data = self.header_encode().unwrap();

        length += header_data.len();

        // 3> body data length
        length += body_length;

        let mut result = BytesMut::with_capacity(4 + length - body_length);

        // length
        result.put_i32(length as i32);

        // header length
        result.put_i32(mark_protocol_type(
            header_data.len() as i32,
            self.serialize_type,
        ));

        // header data
        result.put(header_data);

        Some(result.freeze())
    }

    pub fn make_custom_header_to_net(&mut self) {
        if let Some(header) = &self.command_custom_header {
            let option = header.to_map();

            match &mut self.ext_fields {
                None => {
                    self.ext_fields = option;
                }
                Some(ext) => {
                    if let Some(val) = option {
                        for (key, value) in &val {
                            ext.insert(key.clone(), value.clone());
                        }
                    }
                }
            }
        }
    }

    pub fn fast_header_encode(&mut self, dst: &mut BytesMut) {
        match self.serialize_type {
            SerializeType::JSON => {
                self.make_custom_header_to_net();
                let header = match serde_json::to_vec(self) {
                    Ok(value) => Some(value),
                    Err(e) => {
                        error!("Failed to encode generic: {}", e);
                        None
                    }
                };
                let header_length = header.as_ref().map_or(0, |h| h.len()) as i32;
                let body_length = self.body.as_ref().map_or(0, |b| b.len()) as i32;
                let total_length = 4 + header_length + body_length;

                dst.reserve((total_length + 4) as usize);
                dst.put_i32(total_length);
                let serialize_type =
                    RemotingCommand::mark_serialize_type(header_length, SerializeType::JSON);
                dst.put_i32(serialize_type);

                if let Some(header_inner) = header {
                    dst.put(header_inner.as_slice());
                }
            }
            SerializeType::ROCKETMQ => {
                let begin_index = dst.len();
                dst.put_i64(0);
                if let Some(header) = self.command_custom_header_ref() {
                    if !header.support_fast_codec() {
                        self.make_custom_header_to_net();
                    }
                }
                let header_size = RocketMQSerializable::rocketmq_protocol_encode(self, dst);
                let body_length = self.body.as_ref().map_or(0, |b| b.len()) as i32;
                let serialize_type = RemotingCommand::mark_serialize_type(
                    header_size as i32,
                    SerializeType::ROCKETMQ,
                );
                dst[begin_index..begin_index + 4]
                    .copy_from_slice(&(header_size as i32 + body_length).to_be_bytes());
                dst[begin_index + 4..begin_index + 8]
                    .copy_from_slice(&serialize_type.to_be_bytes());
            }
        }
    }

    pub fn decode(src: &mut BytesMut) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let read_to = src.len();
        if read_to < 4 {
            // Wait for more data when there are less than 4 bytes.
            return Ok(None);
        }
        //Read the total size as a big-endian i32 from the first 4 bytes.
        let total_size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if read_to < total_size + 4 {
            // Wait for more data when the available data is less than the total size.
            return Ok(None);
        }
        // Split the BytesMut to get the command data including the total size.
        let mut cmd_data = src.split_to(total_size + 4);
        // Discard the first i32 (total size).
        cmd_data.advance(4);
        if cmd_data.remaining() < 4 {
            return Ok(None);
        }
        // Read the header length as a big-endian i32.
        let ori_header_length = cmd_data.get_i32();
        let header_length = parse_header_length(ori_header_length);
        if header_length > total_size - 4 {
            return Err(RocketmqError::RemotingCommandDecoderError(format!(
                "Header length {header_length} is greater than total size {total_size}"
            )));
        }
        let protocol_type = parse_serialize_type(ori_header_length)?;
        // Assume the header is of i32 type and directly get it from the data.
        let mut header_data = cmd_data.split_to(header_length);

        let mut cmd =
            RemotingCommand::header_decode(&mut header_data, header_length, protocol_type)?;

        if let Some(cmd) = cmd.as_mut() {
            if total_size - 4 > header_length {
                cmd.set_body_mut_ref(cmd_data.split_to(total_size - 4 - header_length).freeze());
            }
        }
        Ok(cmd)
    }

    pub fn header_decode(
        src: &mut BytesMut,
        header_length: usize,
        type_: SerializeType,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        match type_ {
            SerializeType::JSON => {
                let cmd =
                    SerdeJsonUtils::from_json_slice::<RemotingCommand>(src).map_err(|error| {
                        // Handle deserialization error gracefully
                        RocketmqError::RemotingCommandDecoderError(format!(
                            "Deserialization error: {error}"
                        ))
                    })?;

                Ok(Some(cmd.set_serialize_type(SerializeType::JSON)))
            }
            SerializeType::ROCKETMQ => {
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
        T: FromMap<Target = T, Error = RocketmqError>,
    {
        match self.ext_fields {
            None => Err(RocketmqError::DeserializeHeaderError(
                "ExtFields is None".to_string(),
            )),
            Some(ref header) => T::from(header),
        }
    }

    pub fn decode_command_custom_header_fast<T>(&self) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = RocketmqError>,
        T: Default + CommandCustomHeader,
    {
        match self.ext_fields {
            None => Err(RocketmqError::DeserializeHeaderError(
                "ExtFields is None".to_string(),
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

    pub fn add_ext_field(
        &mut self,
        key: impl Into<CheetahString>,
        value: impl Into<CheetahString>,
    ) -> &mut Self {
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
        requestId.fetch_add(1, Ordering::AcqRel)
    }

    #[inline]
    pub fn add_ext_field_if_not_exist(
        &mut self,
        key: impl Into<CheetahString>,
        value: impl Into<CheetahString>,
    ) {
        if let Some(ref mut ext) = self.ext_fields {
            ext.entry(key.into()).or_insert(value.into());
        }
    }
}

pub fn parse_header_length(size: i32) -> usize {
    (size & 0xFFFFFF) as usize
}

pub fn mark_protocol_type(source: i32, serialize_type: SerializeType) -> i32 {
    ((serialize_type.get_code() as i32) << 24) | (source & 0x00FFFFFF)
}

pub fn parse_serialize_type(size: i32) -> rocketmq_error::RocketMQResult<SerializeType> {
    let code = (size >> 24) as u8;
    match SerializeType::value_of(code) {
        None => Err(RocketmqError::NotSupportSerializeType(code)),
        Some(value) => Ok(value),
    }
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
            "{\"code\":1,\"language\":\"JAVA\",\"version\":0,\"opaque\":1,\"flag\":1,\"remark\":\"\
             remark\",\"extFields\":{},\"serializeTypeCurrentRPC\":\"JSON\"}",
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
