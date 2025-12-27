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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UserType {
    Super = 1,
    Normal = 2,
}

impl UserType {
    #[inline]
    pub fn get_by_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("Super") {
            Some(UserType::Super)
        } else if name.eq_ignore_ascii_case("Normal") {
            Some(UserType::Normal)
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
            UserType::Super => "Super",
            UserType::Normal => "Normal",
        }
    }
}

impl Serialize for UserType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(*self as u8)
    }
}

impl<'de> Deserialize<'de> for UserType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = u8::deserialize(deserializer)?;
        match v {
            1 => Ok(UserType::Super),
            2 => Ok(UserType::Normal),
            _ => Err(serde::de::Error::custom("invalid UserType")),
        }
    }
}
