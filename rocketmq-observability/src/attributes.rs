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

use crate::config::ObservabilityConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub key: &'static str,
    pub value: String,
}

pub fn base_attributes(config: &ObservabilityConfig) -> Vec<Attribute> {
    let mut attributes = vec![
        Attribute::new("cluster", &config.cluster),
        Attribute::new("node_type", &config.node_type),
    ];

    if !config.node_id.is_empty() {
        attributes.push(Attribute::new("node_id", &config.node_id));
    }

    attributes
}

impl Attribute {
    pub fn new(key: &'static str, value: impl Into<String>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }
}
