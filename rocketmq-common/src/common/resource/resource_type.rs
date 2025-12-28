//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum ResourceType {
    Unknown = 0,
    Any = 1,
    Cluster = 2,
    Namespace = 3,
    Topic = 4,
    Group = 5,
}

impl ResourceType {
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("Unknown") {
            Some(Self::Unknown)
        } else if name.eq_ignore_ascii_case("Any") {
            Some(Self::Any)
        } else if name.eq_ignore_ascii_case("Cluster") {
            Some(Self::Cluster)
        } else if name.eq_ignore_ascii_case("Namespace") {
            Some(Self::Namespace)
        } else if name.eq_ignore_ascii_case("Topic") {
            Some(Self::Topic)
        } else if name.eq_ignore_ascii_case("Group") {
            Some(Self::Group)
        } else {
            None
        }
    }

    #[inline]
    pub fn code(self) -> u8 {
        self as u8
    }

    #[inline]
    pub fn name(self) -> &'static str {
        match self {
            Self::Unknown => "Unknown",
            Self::Any => "Any",
            Self::Cluster => "Cluster",
            Self::Namespace => "Namespace",
            Self::Topic => "Topic",
            Self::Group => "Group",
        }
    }
}

impl Serialize for ResourceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(self.code())
    }
}

impl<'de> Deserialize<'de> for ResourceType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            0 => Ok(ResourceType::Unknown),
            1 => Ok(ResourceType::Any),
            2 => Ok(ResourceType::Cluster),
            3 => Ok(ResourceType::Namespace),
            4 => Ok(ResourceType::Topic),
            5 => Ok(ResourceType::Group),
            _ => Err(serde::de::Error::custom("invalid ResourceType code")),
        }
    }
}
