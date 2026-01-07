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
use std::fmt::Debug;
use std::fmt::Display;
use std::str::FromStr;

pub use faq::FAQUrl;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
pub mod chain;
pub use crate::common::sys_flag::topic_sys_flag as TopicSysFlag;
pub mod attribute;
pub mod base;
pub mod boundary_type;
pub mod broker;
pub mod compression;
pub mod config;
pub mod config_manager;
pub mod constant;
pub mod consumer;
pub mod controller;
mod faq;
pub mod filter;
pub mod future;
pub mod hasher;
pub mod key_builder;
pub mod macros;
pub mod message;
pub mod mix_all;
pub mod mq_version;
pub mod namesrv;
pub mod pop_ack_constants;

pub mod running;
pub mod server;
pub mod statistics;
pub mod stats;
pub mod sys_flag;

pub mod action;
pub mod metrics;
pub mod resource;
pub mod system_clock;
pub mod thread;
pub mod topic;

#[derive(Clone, Default, Eq, PartialEq, Copy)]
pub enum TopicFilterType {
    #[default]
    SingleTag,
    MultiTag,
}

impl Display for TopicFilterType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TopicFilterType::SingleTag => write!(f, "SINGLE_TAG"),
            TopicFilterType::MultiTag => write!(f, "MULTI_TAG"),
        }
    }
}

impl Debug for TopicFilterType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TopicFilterType::SingleTag => write!(f, "SINGLE_TAG"),
            TopicFilterType::MultiTag => write!(f, "MULTI_TAG"),
        }
    }
}

impl From<&str> for TopicFilterType {
    fn from(s: &str) -> TopicFilterType {
        match s {
            "SINGLE_TAG" => TopicFilterType::SingleTag,
            "MULTI_TAG" => TopicFilterType::MultiTag,
            _ => TopicFilterType::SingleTag,
        }
    }
}

impl From<String> for TopicFilterType {
    fn from(s: String) -> TopicFilterType {
        TopicFilterType::from(s.as_str())
    }
}

impl From<i32> for TopicFilterType {
    fn from(i: i32) -> TopicFilterType {
        match i {
            0 => TopicFilterType::SingleTag,
            1 => TopicFilterType::MultiTag,
            _ => TopicFilterType::SingleTag,
        }
    }
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

        impl serde::de::Visitor<'_> for TopicFilterTypeVisitor {
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
                    _ => Err(serde::de::Error::unknown_variant(value, &["SingleTag", "MultiTag"])),
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
