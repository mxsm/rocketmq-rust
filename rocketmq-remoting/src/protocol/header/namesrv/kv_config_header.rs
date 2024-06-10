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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PutKVConfigRequestHeader {
    pub namespace: String,
    pub key: String,
    pub value: String,
}

impl PutKVConfigRequestHeader {
    const KEY: &'static str = "key";
    const NAMESPACE: &'static str = "namespace";
    const VALUE: &'static str = "value";

    /// Creates a new instance of `PutKVConfigRequestHeader`.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace.
    /// * `key` - The key.
    /// * `value` - The value.
    pub fn new(
        namespace: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
            value: value.into(),
        }
    }
}

impl CommandCustomHeader for PutKVConfigRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        Some(HashMap::from([
            (
                PutKVConfigRequestHeader::NAMESPACE.to_string(),
                self.namespace.clone(),
            ),
            (PutKVConfigRequestHeader::KEY.to_string(), self.key.clone()),
            (
                PutKVConfigRequestHeader::VALUE.to_string(),
                self.value.clone(),
            ),
        ]))
    }
}

impl FromMap for PutKVConfigRequestHeader {
    type Target = PutKVConfigRequestHeader;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(PutKVConfigRequestHeader {
            namespace: map.get(PutKVConfigRequestHeader::NAMESPACE).cloned()?,
            key: map.get(PutKVConfigRequestHeader::KEY).cloned()?,
            value: map.get(PutKVConfigRequestHeader::VALUE).cloned()?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GetKVConfigRequestHeader {
    pub namespace: String,
    pub key: String,
}

impl GetKVConfigRequestHeader {
    const KEY: &'static str = "key";
    const NAMESPACE: &'static str = "namespace";

    pub fn new(namespace: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

impl CommandCustomHeader for GetKVConfigRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        Some(HashMap::from([
            (
                GetKVConfigRequestHeader::NAMESPACE.to_string(),
                self.namespace.clone(),
            ),
            (GetKVConfigRequestHeader::KEY.to_string(), self.key.clone()),
        ]))
    }
}

impl FromMap for GetKVConfigRequestHeader {
    type Target = GetKVConfigRequestHeader;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(GetKVConfigRequestHeader {
            namespace: map.get(GetKVConfigRequestHeader::NAMESPACE).cloned()?,
            key: map.get(GetKVConfigRequestHeader::KEY).cloned()?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GetKVConfigResponseHeader {
    pub value: Option<String>,
}

impl GetKVConfigResponseHeader {
    const VALUE: &'static str = "value";

    pub fn new(value: Option<String>) -> Self {
        Self { value }
    }
}

impl CommandCustomHeader for GetKVConfigResponseHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        if let Some(ref value) = self.value {
            return Some(HashMap::from([(
                GetKVConfigResponseHeader::VALUE.to_string(),
                value.clone(),
            )]));
        }
        None
    }
}

impl FromMap for GetKVConfigResponseHeader {
    type Target = GetKVConfigResponseHeader;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(GetKVConfigResponseHeader {
            value: map.get(GetKVConfigResponseHeader::VALUE).cloned(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DeleteKVConfigRequestHeader {
    pub namespace: String,
    pub key: String,
}

impl DeleteKVConfigRequestHeader {
    const KEY: &'static str = "key";
    const NAMESPACE: &'static str = "namespace";

    pub fn new(namespace: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

impl CommandCustomHeader for DeleteKVConfigRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        Some(HashMap::from([
            (
                DeleteKVConfigRequestHeader::NAMESPACE.to_string(),
                self.namespace.clone(),
            ),
            (
                DeleteKVConfigRequestHeader::KEY.to_string(),
                self.key.clone(),
            ),
        ]))
    }
}

impl FromMap for DeleteKVConfigRequestHeader {
    type Target = DeleteKVConfigRequestHeader;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(DeleteKVConfigRequestHeader {
            namespace: map.get(DeleteKVConfigRequestHeader::NAMESPACE).cloned()?,
            key: map.get(DeleteKVConfigRequestHeader::KEY).cloned()?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct GetKVListByNamespaceRequestHeader {
    pub namespace: String,
}

impl GetKVListByNamespaceRequestHeader {
    const NAMESPACE: &'static str = "namespace";

    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }
}

impl CommandCustomHeader for GetKVListByNamespaceRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        Some(HashMap::from([(
            GetKVListByNamespaceRequestHeader::NAMESPACE.to_string(),
            self.namespace.clone(),
        )]))
    }
}

impl FromMap for GetKVListByNamespaceRequestHeader {
    type Target = GetKVListByNamespaceRequestHeader;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(GetKVListByNamespaceRequestHeader {
            namespace: map
                .get(GetKVListByNamespaceRequestHeader::NAMESPACE)
                .cloned()?,
        })
    }
}
