// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

pub mod command_custom_header;
pub mod header;
pub mod remoting_command;
pub mod rocketmq_serializable;

pub use command_custom_header::CommandCustomHeader;
pub use command_custom_header::FromMap;
pub use remoting_command::RemotingCommand;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum RemotingCommandType {
    REQUEST,
    RESPONSE,
}

impl RemotingCommandType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::REQUEST),
            1 => Some(Self::RESPONSE),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            Self::REQUEST => 0,
            Self::RESPONSE => 1,
        }
    }

    pub fn get_type_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "REQUEST" => Some(Self::REQUEST),
            "RESPONSE" => Some(Self::RESPONSE),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default, Hash, Copy)]
pub enum LanguageCode {
    JAVA,
    CPP,
    DOTNET,
    PYTHON,
    DELPHI,
    ERLANG,
    RUBY,
    OTHER,
    HTTP,
    GO,
    PHP,
    OMS,
    #[default]
    RUST,
}

impl fmt::Display for LanguageCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<LanguageCode> for u8 {
    fn from(code: LanguageCode) -> Self {
        code.get_code()
    }
}

impl From<LanguageCode> for i32 {
    fn from(code: LanguageCode) -> Self {
        code.get_code() as i32
    }
}

impl From<u32> for LanguageCode {
    fn from(code: u32) -> Self {
        u8::try_from(code).map_or(Self::OTHER, Self::from)
    }
}

impl From<i32> for LanguageCode {
    fn from(code: i32) -> Self {
        u8::try_from(code).map_or(Self::OTHER, Self::from)
    }
}

impl From<u8> for LanguageCode {
    fn from(code: u8) -> Self {
        Self::value_of(code).unwrap_or(Self::OTHER)
    }
}

impl LanguageCode {
    pub fn value_of(code: u8) -> Option<Self> {
        Some(match code {
            0 => Self::JAVA,
            1 => Self::CPP,
            2 => Self::DOTNET,
            3 => Self::PYTHON,
            4 => Self::DELPHI,
            5 => Self::ERLANG,
            6 => Self::RUBY,
            7 => Self::OTHER,
            8 => Self::HTTP,
            9 => Self::GO,
            10 => Self::PHP,
            11 => Self::OMS,
            12 => Self::RUST,
            _ => Self::OTHER,
        })
    }

    pub fn get_code(&self) -> u8 {
        *self as u8
    }

    pub fn get_code_from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "JAVA" => Some(Self::JAVA),
            "CPP" => Some(Self::CPP),
            "DOTNET" => Some(Self::DOTNET),
            "PYTHON" => Some(Self::PYTHON),
            "DELPHI" => Some(Self::DELPHI),
            "ERLANG" => Some(Self::ERLANG),
            "RUBY" => Some(Self::RUBY),
            "OTHER" => Some(Self::OTHER),
            "HTTP" => Some(Self::HTTP),
            "GO" => Some(Self::GO),
            "PHP" => Some(Self::PHP),
            "OMS" => Some(Self::OMS),
            "RUST" => Some(Self::RUST),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Deserialize, Serialize)]
pub enum SerializeType {
    JSON,
    ROCKETMQ,
}

impl fmt::Display for SerializeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl SerializeType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::JSON),
            1 => Some(Self::ROCKETMQ),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        *self as u8
    }
}
