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
pub enum ResourcePattern {
    Any = 1,
    Literal = 2,
    Prefixed = 3,
}

impl ResourcePattern {
    /// Case-insensitive lookup by name. Accepts either uppercase like Java or mixed case.
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("ANY") {
            Some(Self::Any)
        } else if name.eq_ignore_ascii_case("LITERAL") {
            Some(Self::Literal)
        } else if name.eq_ignore_ascii_case("PREFIXED") {
            Some(Self::Prefixed)
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
            Self::Any => "ANY",
            Self::Literal => "LITERAL",
            Self::Prefixed => "PREFIXED",
        }
    }
}

impl Serialize for ResourcePattern {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(self.code())
    }
}

impl<'de> Deserialize<'de> for ResourcePattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            1 => Ok(ResourcePattern::Any),
            2 => Ok(ResourcePattern::Literal),
            3 => Ok(ResourcePattern::Prefixed),
            _ => Err(serde::de::Error::custom("invalid ResourcePattern code")),
        }
    }
}
