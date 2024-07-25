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
use std::fmt;

use serde::Deserialize;
use serde::Deserializer;

#[allow(dead_code)]
#[derive(Debug, Copy, Clone, Default, PartialEq)]
pub enum FlushDiskType {
    SyncFlush,

    #[default]
    AsyncFlush,
}

impl FlushDiskType {
    pub fn get_flush_disk_type(&self) -> &'static str {
        match self {
            FlushDiskType::SyncFlush => "SyncFlush",
            FlushDiskType::AsyncFlush => "AsyncFlush",
        }
    }
}

impl<'de> Deserialize<'de> for FlushDiskType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FlushDiskTypeVisitor;

        impl<'de> serde::de::Visitor<'de> for FlushDiskTypeVisitor {
            type Value = FlushDiskType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing FlushDiskType")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SYNC_FLUSH" => Ok(FlushDiskType::SyncFlush),
                    "ASYNC_FLUSH" => Ok(FlushDiskType::AsyncFlush),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["SyncFlush", "AsyncFlush"],
                    )),
                }
            }
        }

        deserializer.deserialize_str(FlushDiskTypeVisitor)
    }
}
