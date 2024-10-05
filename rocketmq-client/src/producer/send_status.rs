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
use serde::Serialize;
use serde::Serializer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SendStatus {
    #[default]
    SendOk,
    FlushDiskTimeout,
    FlushSlaveTimeout,
    SlaveNotAvailable,
}

impl Serialize for SendStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            SendStatus::SendOk => "SEND_OK",
            SendStatus::FlushDiskTimeout => "FLUSH_DISK_TIMEOUT",
            SendStatus::FlushSlaveTimeout => "FLUSH_SLAVE_TIMEOUT",
            SendStatus::SlaveNotAvailable => "SLAVE_NOT_AVAILABLE",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for SendStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StoreTypeVisitor;

        impl serde::de::Visitor<'_> for StoreTypeVisitor {
            type Value = SendStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing SendStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SEND_OK" => Ok(SendStatus::SendOk),
                    "FLUSH_DISK_TIMEOUT" => Ok(SendStatus::FlushDiskTimeout),
                    "FLUSH_SLAVE_TIMEOUT" => Ok(SendStatus::FlushSlaveTimeout),
                    "SLAVE_NOT_AVAILABLE" => Ok(SendStatus::SlaveNotAvailable),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &[
                            "SEND_OK",
                            "FLUSH_DISK_TIMEOUT",
                            "FLUSH_SLAVE_TIMEOUT",
                            "SLAVE_NOT_AVAILABLE",
                        ],
                    )),
                }
            }
        }
        deserializer.deserialize_str(StoreTypeVisitor)
    }
}
