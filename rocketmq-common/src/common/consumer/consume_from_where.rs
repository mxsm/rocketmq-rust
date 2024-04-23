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

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumeFromWhere {
    ConsumeFromLastOffset,
    ConsumeFromLastOffsetAndFromMinWhenBootFirst,
    ConsumeFromMinOffset,
    ConsumeFromMaxOffset,
    ConsumeFromFirstOffset,
    ConsumeFromTimestamp,
}

impl Serialize for ConsumeFromWhere {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            ConsumeFromWhere::ConsumeFromLastOffset => "CONSUME_FROM_LAST_OFFSET",
            ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst => {
                "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"
            }
            ConsumeFromWhere::ConsumeFromMinOffset => "CONSUME_FROM_MIN_OFFSET",
            ConsumeFromWhere::ConsumeFromMaxOffset => "CONSUME_FROM_MAX_OFFSET",
            ConsumeFromWhere::ConsumeFromFirstOffset => "CONSUME_FROM_FIRST_OFFSET",
            ConsumeFromWhere::ConsumeFromTimestamp => "CONSUME_FROM_TIMESTAMP",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for ConsumeFromWhere {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ConsumeFromWhereVisitor;

        impl<'de> serde::de::Visitor<'de> for ConsumeFromWhereVisitor {
            type Value = ConsumeFromWhere;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing TopicFilterType")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CONSUME_FROM_LAST_OFFSET" => Ok(ConsumeFromWhere::ConsumeFromLastOffset),
                    "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST" => {
                        Ok(ConsumeFromWhere::ConsumeFromLastOffsetAndFromMinWhenBootFirst)
                    }
                    "CONSUME_FROM_MIN_OFFSET" => Ok(ConsumeFromWhere::ConsumeFromMinOffset),
                    "CONSUME_FROM_MAX_OFFSET" => Ok(ConsumeFromWhere::ConsumeFromMaxOffset),
                    "CONSUME_FROM_FIRST_OFFSET" => Ok(ConsumeFromWhere::ConsumeFromFirstOffset),
                    "CONSUME_FROM_TIMESTAMP" => Ok(ConsumeFromWhere::ConsumeFromTimestamp),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &[
                            "CONSUME_FROM_LAST_OFFSET",
                            "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
                            "CONSUME_FROM_MIN_OFFSET",
                            "CONSUME_FROM_MAX_OFFSET",
                            "CONSUME_FROM_FIRST_OFFSET",
                            "CONSUME_FROM_TIMESTAMP",
                        ],
                    )),
                }
            }
        }

        deserializer.deserialize_str(ConsumeFromWhereVisitor)
    }
}
