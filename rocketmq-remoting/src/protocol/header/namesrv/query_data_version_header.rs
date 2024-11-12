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
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryDataVersionRequestHeader {
    pub broker_name: CheetahString,
    pub broker_addr: CheetahString,
    pub cluster_name: CheetahString,
    pub broker_id: u64,
}

impl QueryDataVersionRequestHeader {
    const BROKER_ADDR: &'static str = "brokerAddr";
    const BROKER_ID: &'static str = "brokerId";
    const BROKER_NAME: &'static str = "brokerName";
    const CLUSTER_NAME: &'static str = "clusterName";

    pub fn new(
        broker_name: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
        cluster_name: impl Into<CheetahString>,
        broker_id: u64,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_addr: broker_addr.into(),
            cluster_name: cluster_name.into(),
            broker_id,
        }
    }
}

impl CommandCustomHeader for QueryDataVersionRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([
            (
                CheetahString::from_static_str(Self::BROKER_NAME),
                self.broker_name.clone(),
            ),
            (
                CheetahString::from_static_str(Self::BROKER_ADDR),
                self.broker_addr.clone(),
            ),
            (
                CheetahString::from_static_str(Self::CLUSTER_NAME),
                self.cluster_name.clone(),
            ),
            (
                CheetahString::from_static_str(Self::BROKER_ID),
                CheetahString::from_string(self.broker_id.to_string()),
            ),
        ]))
    }
}

impl FromMap for QueryDataVersionRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(QueryDataVersionRequestHeader {
            broker_name: map
                .get(&CheetahString::from_static_str(
                    QueryDataVersionRequestHeader::BROKER_NAME,
                ))
                .cloned()
                .unwrap_or_default(),
            broker_addr: map
                .get(&CheetahString::from_static_str(
                    QueryDataVersionRequestHeader::BROKER_ADDR,
                ))
                .cloned()
                .unwrap_or_default(),
            cluster_name: map
                .get(&CheetahString::from_static_str(
                    QueryDataVersionRequestHeader::CLUSTER_NAME,
                ))
                .cloned()
                .unwrap_or_default(),
            broker_id: map
                .get(&CheetahString::from_static_str(
                    QueryDataVersionRequestHeader::BROKER_ID,
                ))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct QueryDataVersionResponseHeader {
    changed: bool,
}

impl QueryDataVersionResponseHeader {
    const CHANGED: &'static str = "changed";

    pub fn new(changed: bool) -> Self {
        Self { changed }
    }
}

impl CommandCustomHeader for QueryDataVersionResponseHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str(Self::CHANGED),
            CheetahString::from_string(self.changed.to_string()),
        )]))
    }
}

impl FromMap for QueryDataVersionResponseHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(QueryDataVersionResponseHeader {
            changed: map
                .get(&CheetahString::from_static_str(
                    QueryDataVersionResponseHeader::CHANGED,
                ))
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or(false),
        })
    }
}
