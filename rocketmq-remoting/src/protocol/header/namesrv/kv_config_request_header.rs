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

use serde::{Deserialize, Serialize};

use crate::protocol::command_custom_header::{CommandCustomHeader, FromMap};

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct PutKVConfigRequestHeader {
    pub namespace: String,
    pub key: String,
    pub value: String,
}

impl PutKVConfigRequestHeader {
    const NAMESPACE: &'static str = "namespace";
    const KEY: &'static str = "key";
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
