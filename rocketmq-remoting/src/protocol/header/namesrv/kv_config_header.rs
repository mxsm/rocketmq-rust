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

use cheetah_string::CheetahString;
use rocketmq_macros::RequestHeaderCodec;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Debug, Clone, Deserialize, Serialize, Default, RequestHeaderCodec)]
pub struct PutKVConfigRequestHeader {
    #[required]
    pub namespace: CheetahString,

    #[required]
    pub key: CheetahString,

    #[required]
    pub value: CheetahString,
}

impl PutKVConfigRequestHeader {
    /// Creates a new instance of `PutKVConfigRequestHeader`.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace.
    /// * `key` - The key.
    /// * `value` - The value.
    pub fn new(
        namespace: impl Into<CheetahString>,
        key: impl Into<CheetahString>,
        value: impl Into<CheetahString>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GetKVConfigRequestHeader {
    pub namespace: CheetahString,
    pub key: CheetahString,
}

impl GetKVConfigRequestHeader {
    const KEY: &'static str = "key";
    const NAMESPACE: &'static str = "namespace";

    pub fn new(namespace: impl Into<CheetahString>, key: impl Into<CheetahString>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

impl CommandCustomHeader for GetKVConfigRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([
            (
                CheetahString::from_static_str(GetKVConfigRequestHeader::NAMESPACE),
                self.namespace.clone(),
            ),
            (
                CheetahString::from_static_str(GetKVConfigRequestHeader::KEY),
                self.key.clone(),
            ),
        ]))
    }
}

impl FromMap for GetKVConfigRequestHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = GetKVConfigRequestHeader;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(GetKVConfigRequestHeader {
            namespace: map
                .get(&CheetahString::from_static_str(
                    GetKVConfigRequestHeader::NAMESPACE,
                ))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss namespace field".to_string(),
                ))?,
            key: map
                .get(&CheetahString::from_static_str(
                    GetKVConfigRequestHeader::KEY,
                ))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss key field".to_string(),
                ))?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GetKVConfigResponseHeader {
    pub value: Option<CheetahString>,
}

impl GetKVConfigResponseHeader {
    const VALUE: &'static str = "value";

    pub fn new(value: Option<CheetahString>) -> Self {
        Self { value }
    }
}

impl CommandCustomHeader for GetKVConfigResponseHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        if let Some(ref value) = self.value {
            return Some(HashMap::from([(
                CheetahString::from_static_str(GetKVConfigResponseHeader::VALUE),
                value.clone(),
            )]));
        }
        None
    }
}

impl FromMap for GetKVConfigResponseHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = GetKVConfigResponseHeader;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(GetKVConfigResponseHeader {
            value: map
                .get(&CheetahString::from_static_str(
                    GetKVConfigResponseHeader::VALUE,
                ))
                .cloned(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DeleteKVConfigRequestHeader {
    pub namespace: CheetahString,
    pub key: CheetahString,
}

impl DeleteKVConfigRequestHeader {
    const KEY: &'static str = "key";
    const NAMESPACE: &'static str = "namespace";

    pub fn new(namespace: impl Into<CheetahString>, key: impl Into<CheetahString>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

impl CommandCustomHeader for DeleteKVConfigRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([
            (
                CheetahString::from_static_str(DeleteKVConfigRequestHeader::NAMESPACE),
                self.namespace.clone(),
            ),
            (
                CheetahString::from_static_str(DeleteKVConfigRequestHeader::KEY),
                self.key.clone(),
            ),
        ]))
    }
}

impl FromMap for DeleteKVConfigRequestHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = DeleteKVConfigRequestHeader;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(DeleteKVConfigRequestHeader {
            namespace: map
                .get(&CheetahString::from_static_str(
                    DeleteKVConfigRequestHeader::NAMESPACE,
                ))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss namespace field".to_string(),
                ))?,
            key: map
                .get(&CheetahString::from_static_str(
                    DeleteKVConfigRequestHeader::KEY,
                ))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss key field".to_string(),
                ))?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GetKVListByNamespaceRequestHeader {
    pub namespace: CheetahString,
}

impl GetKVListByNamespaceRequestHeader {
    const NAMESPACE: &'static str = "namespace";

    pub fn new(namespace: impl Into<CheetahString>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }
}

impl CommandCustomHeader for GetKVListByNamespaceRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str(GetKVListByNamespaceRequestHeader::NAMESPACE),
            self.namespace.clone(),
        )]))
    }
}

impl FromMap for GetKVListByNamespaceRequestHeader {
    type Error = crate::remoting_error::RemotingError;

    type Target = GetKVListByNamespaceRequestHeader;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error> {
        Ok(GetKVListByNamespaceRequestHeader {
            namespace: map
                .get(&CheetahString::from_static_str(
                    GetKVListByNamespaceRequestHeader::NAMESPACE,
                ))
                .cloned()
                .ok_or(Self::Error::RemotingCommandError(
                    "Miss namespace field".to_string(),
                ))?,
        })
    }
}
