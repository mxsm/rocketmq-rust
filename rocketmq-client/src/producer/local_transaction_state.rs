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
use std::fmt::Display;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LocalTransactionState {
    #[default]
    CommitMessage,
    RollbackMessage,
    //java is UNKNOW
    Unknown,
}

impl Serialize for LocalTransactionState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            LocalTransactionState::CommitMessage => "COMMIT_MESSAGE",
            LocalTransactionState::RollbackMessage => "ROLLBACK_MESSAGE",
            LocalTransactionState::Unknown => "UNKNOW",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for LocalTransactionState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StoreTypeVisitor;

        impl serde::de::Visitor<'_> for StoreTypeVisitor {
            type Value = LocalTransactionState;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing SendStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "COMMIT_MESSAGE" => Ok(LocalTransactionState::CommitMessage),
                    "ROLLBACK_MESSAGE" => Ok(LocalTransactionState::RollbackMessage),
                    "UNKNOW" | "UNKNOWN" => Ok(LocalTransactionState::Unknown),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["COMMIT_MESSAGE", "ROLLBACK_MESSAGE", "UNKNOWN", "UNKNOW"],
                    )),
                }
            }
        }
        deserializer.deserialize_str(StoreTypeVisitor)
    }
}

impl Display for LocalTransactionState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LocalTransactionState::CommitMessage => write!(f, "COMMIT_MESSAGE"),
            LocalTransactionState::RollbackMessage => write!(f, "ROLLBACK_MESSAGE"),
            LocalTransactionState::Unknown => write!(f, "UNKNOW"),
        }
    }
}
