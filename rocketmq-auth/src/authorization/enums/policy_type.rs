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
pub enum PolicyType {
    Custom = 1,
    Default = 2,
}

impl PolicyType {
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("Custom") {
            Some(Self::Custom)
        } else if name.eq_ignore_ascii_case("Default") {
            Some(Self::Default)
        } else {
            None
        }
    }

    pub fn code(self) -> u8 {
        self as u8
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Custom => "Custom",
            Self::Default => "Default",
        }
    }
}

impl Serialize for PolicyType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(self.code())
    }
}

impl<'de> Deserialize<'de> for PolicyType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            1 => Ok(PolicyType::Custom),
            2 => Ok(PolicyType::Default),
            _ => Err(serde::de::Error::custom("invalid PolicyType code")),
        }
    }
}
