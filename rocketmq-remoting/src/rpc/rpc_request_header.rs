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

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RpcRequestHeader {
    // the namespace name
    #[serde(rename = "namespace")]
    pub namespace: Option<String>,
    // if the data has been namespaced
    #[serde(rename = "namespaced")]
    pub namespaced: Option<bool>,
    // the abstract remote addr name, usually the physical broker name
    #[serde(rename = "brokerName")]
    pub broker_name: Option<String>,
    // oneway
    #[serde(rename = "oneway")]
    pub oneway: Option<bool>,
}

impl RpcRequestHeader {
    pub const BROKER_NAME: &'static str = "brokerName";
    pub const NAMESPACE: &'static str = "namespace";
    pub const NAMESPACED: &'static str = "namespaced";
    pub const ONEWAY: &'static str = "oneway";

    pub fn new(
        namespace: Option<String>,
        namespaced: Option<bool>,
        broker_name: Option<String>,
        oneway: Option<bool>,
    ) -> Self {
        Self {
            namespace,
            namespaced,
            broker_name,
            oneway,
        }
    }
}

impl FromMap for RpcRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(RpcRequestHeader {
            namespace: map.get(RpcRequestHeader::NAMESPACE).cloned(),
            namespaced: map
                .get(RpcRequestHeader::NAMESPACED)
                .and_then(|s| s.parse::<bool>().ok()),
            broker_name: map.get(RpcRequestHeader::BROKER_NAME).cloned(),
            oneway: map
                .get(RpcRequestHeader::ONEWAY)
                .and_then(|s| s.parse::<bool>().ok()),
        })
    }
}

impl CommandCustomHeader for RpcRequestHeader {
    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::new();
        if let Some(ref namespace) = self.namespace {
            map.insert(Self::NAMESPACE.to_string(), namespace.clone());
        }
        if let Some(namespaced) = self.namespaced {
            map.insert(Self::NAMESPACED.to_string(), namespaced.to_string());
        }
        if let Some(ref broker_name) = self.broker_name {
            map.insert(Self::BROKER_NAME.to_string(), broker_name.clone());
        }
        if let Some(oneway) = self.oneway {
            map.insert(Self::ONEWAY.to_string(), oneway.to_string());
        }
        Some(map)
    }
}
