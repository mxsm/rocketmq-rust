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
use serde::ser::SerializeStruct;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AccessChannel {
    Local,
    Cloud,
}

impl Serialize for AccessChannel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AccessChannel", 1)?;
        match *self {
            AccessChannel::Local => state.serialize_field("AccessChannel", "LOCAL")?,
            AccessChannel::Cloud => state.serialize_field("AccessChannel", "CLOUD")?,
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for AccessChannel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        match s {
            "LOCAL" => Ok(AccessChannel::Local),
            "CLOUD" => Ok(AccessChannel::Cloud),
            _ => Err(serde::de::Error::custom("unknown AccessChannel variant")),
        }
    }
}
