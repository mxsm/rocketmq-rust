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
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, Once, RwLock,
    },
};

use bytes::Bytes;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use super::{RemotingCommandType, SerializeType};
use crate::{
    code::response_code::RemotingSysResponseCode,
    protocol::{
        command_custom_header::{CommandCustomHeader, FromMap},
        LanguageCode,
    },
};

lazy_static! {
    static ref OPAQUE_COUNTER: Arc<AtomicI32> = Arc::new(AtomicI32::new(0));
    static ref CONFIG_VERSION: RwLock<i32> = RwLock::new(-1);
    static ref INIT: Once = Once::new();
}

fn set_cmd_version(cmd: &mut RemotingCommand) {
    INIT.call_once(|| {
        let v = match std::env::var("REMOTING_VERSION_KEY") {
            Ok(value) => value.parse::<i32>().unwrap_or(-1),
            Err(_) => -1,
        };
        *CONFIG_VERSION.write().unwrap() = v;
    });

    let config_version = *CONFIG_VERSION.read().unwrap();

    if config_version >= 0 {
        cmd.set_version(config_version);
    } else if let Ok(v) = std::env::var("rocketmq.remoting.version") {
        if let Ok(value) = v.parse::<i32>() {
            cmd.set_version(value);
            *CONFIG_VERSION.write().unwrap() = value;
        }
    }
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
    remark: Option<String>,

    #[serde(rename = "extFields")]
    ext_fields: Option<HashMap<String, String>>,

    #[serde(skip)]
    body: Option<Bytes>,
    #[serde(skip)]
    suspended: bool,
    #[serde(skip)]
    command_custom_header: Option<Box<dyn CommandCustomHeader + Send + 'static>>,
    #[serde(rename = "serializeTypeCurrentRPC")]
    serialize_type: SerializeType,
}

impl Default for RemotingCommand {
    fn default() -> Self {
        let opaque = OPAQUE_COUNTER.fetch_add(1, Ordering::SeqCst);
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
            serialize_type: SerializeType::JSON,
        }
    }
}

impl RemotingCommand {
    pub(crate) const RPC_TYPE: i32 = 0;
    pub(crate) const RPC_ONEWAY: i32 = 1;
}

impl RemotingCommand {
    pub fn create_request_command(
        code: impl Into<i32>,
        header: impl CommandCustomHeader + Send + 'static,
    ) -> Self {
        let mut command = Self::default()
            .set_code(code.into())
            .set_command_custom_header(Some(Box::new(header)));
        set_cmd_version(&mut command);
        command
    }

    pub fn create_remoting_command(code: impl Into<i32>) -> Self {
        let command = Self::default();
        command.set_code(code.into())
    }

    pub fn get_and_add() -> i32 {
        OPAQUE_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    pub fn set_cmd_version(self) -> Self {
        self
    }

    pub fn create_response_command_with_code(code: impl Into<i32>) -> Self {
        Self::default().set_code(code).mark_response_type()
    }

    pub fn create_response_command() -> Self {
        Self::default()
            .set_code(RemotingSysResponseCode::Success)
            .mark_response_type()
    }

    pub fn create_response_command_with_header(
        header: impl CommandCustomHeader + Send + 'static,
    ) -> Self {
        Self::default()
            .set_code(RemotingSysResponseCode::Success)
            .set_command_custom_header(Some(Box::new(header)))
            .mark_response_type()
    }

    pub fn set_command_custom_header(
        mut self,
        command_custom_header: Option<Box<dyn CommandCustomHeader + Send + 'static>>,
    ) -> Self {
        self.command_custom_header = command_custom_header;
        if let Some(cch) = &self.command_custom_header {
            let option = cch.to_map();

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
        self
    }

    pub fn set_code(mut self, code: impl Into<i32>) -> Self {
        self.code = code.into();
        self
    }
    pub fn set_language(mut self, language: LanguageCode) -> Self {
        self.language = language;
        self
    }
    pub fn set_version(&mut self, version: i32) {
        self.version = version;
    }
    pub fn set_opaque(mut self, opaque: i32) -> Self {
        self.opaque = opaque;
        self
    }
    pub fn set_flag(mut self, flag: i32) -> Self {
        self.flag = flag;
        self
    }
    pub fn set_remark(mut self, remark: Option<String>) -> Self {
        self.remark = remark;
        self
    }
    pub fn set_ext_fields(mut self, ext_fields: HashMap<String, String>) -> Self {
        self.ext_fields = Some(ext_fields);
        self
    }

    pub fn set_body(mut self, body: Option<impl Into<Bytes>>) -> Self {
        if let Some(value) = body {
            self.body = Some(value.into());
        }
        self
    }
    pub fn set_suspended(mut self, suspended: bool) -> Self {
        self.suspended = suspended;
        self
    }
    pub fn set_serialize_type(mut self, serialize_type: SerializeType) -> Self {
        self.serialize_type = serialize_type;
        self
    }

    pub fn mark_response_type(mut self) -> Self {
        let mark = 1 << Self::RPC_TYPE;
        self.flag |= mark;
        self
    }

    pub fn mark_oneway_rpc(mut self) -> Self {
        let mark = 1 << Self::RPC_ONEWAY;
        self.flag |= mark;
        self
    }

    pub fn get_serialize_type(&self) -> SerializeType {
        self.serialize_type
    }

    pub fn header_encode(&self) -> Option<Bytes> {
        self.command_custom_header.as_ref().and_then(|cch| {
            cch.to_map()
                .as_ref()
                .map(|val| Bytes::from(serde_json::to_vec(val).unwrap()))
        })
    }

    pub fn fast_header_encode(&self) -> Option<Bytes> {
        let st = serde_json::to_string(self).unwrap();
        Some(Bytes::from(st))
    }

    pub fn get_body(&self) -> Option<Bytes> {
        self.body.as_ref().cloned()
    }

    pub fn mark_serialize_type(header_length: i32, protocol_type: SerializeType) -> i32 {
        (protocol_type.get_code() as i32) << 24 | (header_length & 0x00FFFFFF)
    }

    pub fn get_header_length(size: usize) -> usize {
        size & 0xFFFFFF
    }

    pub fn code(&self) -> i32 {
        self.code
    }
    pub fn language(&self) -> &LanguageCode {
        &self.language
    }
    pub fn version(&self) -> i32 {
        self.version
    }
    pub fn opaque(&self) -> i32 {
        self.opaque
    }
    pub fn flag(&self) -> i32 {
        self.flag
    }
    pub fn remark(&self) -> &Option<String> {
        &self.remark
    }
    pub fn ext_fields(&self) -> &Option<HashMap<String, String>> {
        &self.ext_fields
    }
    pub fn body(&self) -> &Option<Bytes> {
        &self.body
    }
    pub fn suspended(&self) -> bool {
        self.suspended
    }
    pub fn command_custom_header(&self) -> &Option<Box<dyn CommandCustomHeader + Send + 'static>> {
        &self.command_custom_header
    }
    pub fn serialize_type(&self) -> SerializeType {
        self.serialize_type
    }

    pub fn decode_command_custom_header<T>(&self) -> Option<T>
    where
        T: FromMap<Target = T>,
    {
        match self.ext_fields {
            None => None,
            Some(ref header) => T::from(header),
        }
    }

    pub fn is_response_type(&self) -> bool {
        let bits = 1 << Self::RPC_TYPE;
        (self.flag & bits) == bits
    }

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
            .set_remark(Some("remark".to_string()));

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
