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
use serde::Deserializer;

pub mod backend;

#[derive(Debug, Copy, Clone, Default, PartialEq)]
pub enum FlushDiskType {
    SyncFlush,

    #[default]
    AsyncFlush,
}

impl FlushDiskType {
    pub fn get_flush_disk_type(&self) -> &'static str {
        match self {
            FlushDiskType::SyncFlush => "SYNC_FLUSH",
            FlushDiskType::AsyncFlush => "ASYNC_FLUSH",
        }
    }
}

impl<'de> Deserialize<'de> for FlushDiskType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FlushDiskTypeVisitor;

        impl serde::de::Visitor<'_> for FlushDiskTypeVisitor {
            type Value = FlushDiskType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing FlushDiskType")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SYNC_FLUSH" | "SyncFlush" => Ok(FlushDiskType::SyncFlush),
                    "ASYNC_FLUSH" | "AsyncFlush" => Ok(FlushDiskType::AsyncFlush),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["SYNC_FLUSH/SyncFlush", "ASYNC_FLUSH/AsyncFlush"],
                    )),
                }
            }
        }

        deserializer.deserialize_str(FlushDiskTypeVisitor)
    }
}
