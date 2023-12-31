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
        Arc,
    },
};

use bytes::Bytes;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use super::SerializeType;
use crate::{
    code::response_code::RemotingSysResponseCode,
    protocol::{
        command_custom_header::{CommandCustomHeader, FromMap},
        LanguageCode,
    },
};

lazy_static! {
    static ref OPAQUE_COUNTER: Arc<AtomicI32> = Arc::new(AtomicI32::new(0));
}

#[derive(Serialize, Deserialize)]
pub struct RemotingCommand {
    code: i32,
    language: LanguageCode,
    version: i32,
    opaque: i32,
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

/*impl<'de> Deserialize<'de> for RemotingCommand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RemotingCommandVisitor;

        impl<'de> Visitor<'de> for RemotingCommandVisitor {
            type Value = RemotingCommand;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct RemotingCommand")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                // Fetch the current value of the counter
                let current_opaque = OPAQUE_COUNTER.fetch_add(1, Ordering::SeqCst);
                // Deserialize the RemotingCommand struct fields
                let code = map.next_entry()?;
                let language = map.next_entry()?;
                let version = map.next_entry()?;
                let opaque = Ok((String::from("opaque"), current_opaque));
                let flag = map.next_entry()?;
                let remark = map.next_entry()?;
                let ext_fields = map.next_entry()?;
                let body = map.next_entry()?;
                let suspended = map.next_entry()?;
                let command_custom_header = map.next_entry()?;
                let serialize_type = map.next_entry()?;

                // Build the RemotingCommand struct
                let remoting_command = RemotingCommand {
                    code: code?,
                    language: language?,
                    version: version?,
                    opaque: opaque?,
                    flag: flag?,
                    remark: remark?,
                    ext_fields: ext_fields?,
                    body: body?,
                    suspended: suspended?,
                    command_custom_header: command_custom_header?,
                    serialize_type: serialize_type?,
                };

                Ok(remoting_command)
            }
        }

        // Deserialize the RemotingCommand struct
        deserializer.deserialize_map(RemotingCommandVisitor)
    }
}*/

impl RemotingCommand {
    pub fn create_remoting_command(code: i32) -> Self {
        let command = Self::default();
        command.set_code(code)
    }

    pub fn get_and_add() -> i32 {
        OPAQUE_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    pub fn set_cmd_version(self) -> Self {
        self
    }

    pub fn create_response_command_with_code(code: i32) -> Self {
        Self::default().set_code(code).set_flag(0x01)
    }

    pub fn create_response_command() -> Self {
        Self::default()
            .set_code(RemotingSysResponseCode::Success as i32)
            .set_flag(0x01)
    }

    pub fn create_response_command_with_header(
        header: impl CommandCustomHeader + Send + 'static,
    ) -> Self {
        Self::default()
            .set_code(RemotingSysResponseCode::Success as i32)
            .set_command_custom_header(Some(Box::new(header)))
            .set_flag(0x01)
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

    pub fn set_code(mut self, code: i32) -> Self {
        self.code = code;
        self
    }
    pub fn set_language(mut self, language: LanguageCode) -> Self {
        self.language = language;
        self
    }
    pub fn set_version(mut self, version: i32) -> Self {
        self.version = version;
        self
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

    pub fn set_body(mut self, body: Option<Bytes>) -> Self {
        self.body = body;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remoting_command() {
        let command = RemotingCommand::create_remoting_command(1)
            .set_code(1)
            .set_language(LanguageCode::JAVA)
            .set_version(1)
            .set_opaque(1)
            .set_flag(1)
            .set_ext_fields(HashMap::new())
            .set_remark(Some("remark".to_string()));

        assert_eq!(
            "{\"code\":1,\"language\":\"JAVA\",\"version\":1,\"opaque\":1,\"flag\":1,\"remark\":\"\
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
