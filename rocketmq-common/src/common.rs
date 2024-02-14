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

pub use faq::FAQUrl;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub use crate::common::sys_flag::topic_sys_flag as TopicSysFlag;

pub mod attribute;
pub mod boundary_type;
pub mod broker;
pub mod config;
pub mod config_manager;
pub mod constant;
mod faq;
pub mod message;
pub mod mix_all;
pub mod mq_version;
pub mod namesrv;
mod sys_flag;
pub mod topic;

#[derive(Debug, Clone)]
pub enum TopicFilterType {
    SingleTag,
    MultiTag,
}

impl Serialize for TopicFilterType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match self {
            TopicFilterType::SingleTag => "SINGLE_TAG",
            TopicFilterType::MultiTag => "MULTI_TAG",
        };
        serializer.serialize_str(value)
    }
}

impl<'de> Deserialize<'de> for TopicFilterType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TopicFilterTypeVisitor;

        impl<'de> serde::de::Visitor<'de> for TopicFilterTypeVisitor {
            type Value = TopicFilterType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing TopicFilterType")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SINGLE_TAG" => Ok(TopicFilterType::SingleTag),
                    "MULTI_TAG" => Ok(TopicFilterType::MultiTag),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["SingleTag", "MultiTag"],
                    )),
                }
            }
        }

        deserializer.deserialize_str(TopicFilterTypeVisitor)
    }
}

pub struct Pair<T, U> {
    pub left: T,
    pub right: U,
}

impl<T, U> Pair<T, U> {
    pub fn new(left: T, right: U) -> Self {
        Self { left, right }
    }
}
