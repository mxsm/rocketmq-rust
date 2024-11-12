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
pub struct WipeWritePermOfBrokerRequestHeader {
    pub broker_name: CheetahString,
}

impl WipeWritePermOfBrokerRequestHeader {
    const BROKER_NAME: &'static str = "brokerName";

    pub fn new(broker_name: impl Into<CheetahString>) -> Self {
        Self {
            broker_name: broker_name.into(),
        }
    }
}

impl CommandCustomHeader for WipeWritePermOfBrokerRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str(Self::BROKER_NAME),
            self.broker_name.clone(),
        )]))
    }
}

impl FromMap for WipeWritePermOfBrokerRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(WipeWritePermOfBrokerRequestHeader {
            broker_name: map
                .get(&CheetahString::from_static_str(
                    WipeWritePermOfBrokerRequestHeader::BROKER_NAME,
                ))
                .cloned()
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct WipeWritePermOfBrokerResponseHeader {
    pub wipe_topic_count: i32,
}

impl WipeWritePermOfBrokerResponseHeader {
    const WIPE_TOPIC_COUNT: &'static str = "wipeTopicCount";

    pub fn new(wipe_topic_count: i32) -> Self {
        Self { wipe_topic_count }
    }
}

impl CommandCustomHeader for WipeWritePermOfBrokerResponseHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str(Self::WIPE_TOPIC_COUNT),
            CheetahString::from_string(self.wipe_topic_count.to_string()),
        )]))
    }
}

impl FromMap for WipeWritePermOfBrokerResponseHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(WipeWritePermOfBrokerResponseHeader {
            wipe_topic_count: map
                .get(&CheetahString::from_static_str(
                    WipeWritePermOfBrokerResponseHeader::WIPE_TOPIC_COUNT,
                ))
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AddWritePermOfBrokerRequestHeader {
    pub broker_name: CheetahString,
}

impl AddWritePermOfBrokerRequestHeader {
    const BROKER_NAME: &'static str = "brokerName";

    pub fn new(broker_name: impl Into<CheetahString>) -> Self {
        Self {
            broker_name: broker_name.into(),
        }
    }
}

impl CommandCustomHeader for AddWritePermOfBrokerRequestHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str(Self::BROKER_NAME),
            self.broker_name.clone(),
        )]))
    }
}

impl FromMap for AddWritePermOfBrokerRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(AddWritePermOfBrokerRequestHeader {
            broker_name: map
                .get(&CheetahString::from_static_str(
                    AddWritePermOfBrokerRequestHeader::BROKER_NAME,
                ))
                .cloned()
                .unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AddWritePermOfBrokerResponseHeader {
    pub add_topic_count: i32,
}

impl AddWritePermOfBrokerResponseHeader {
    const ADD_TOPIC_COUNT: &'static str = "addTopicCount";

    pub fn new(add_topic_count: i32) -> Self {
        Self { add_topic_count }
    }
}

impl CommandCustomHeader for AddWritePermOfBrokerResponseHeader {
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        Some(HashMap::from([(
            CheetahString::from_static_str(Self::ADD_TOPIC_COUNT),
            CheetahString::from_string(self.add_topic_count.to_string()),
        )]))
    }
}

impl FromMap for AddWritePermOfBrokerResponseHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(AddWritePermOfBrokerResponseHeader {
            add_topic_count: map
                .get(&CheetahString::from_static_str(
                    AddWritePermOfBrokerResponseHeader::ADD_TOPIC_COUNT,
                ))
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0),
        })
    }
}
