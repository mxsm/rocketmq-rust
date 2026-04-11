// Copyright 2026 The RocketMQ Rust Authors
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

//! Broker-related request and result models.

use cheetah_string::CheetahString;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;

use crate::core::admin::AdminBuilder;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trim_required_cheetah(field: &'static str, value: impl Into<String>) -> RocketMQResult<CheetahString> {
    let value = value.into();
    let value = value.trim();
    if value.is_empty() {
        return Err(ToolsError::validation_error(field, format!("{field} must not be empty")).into());
    }
    Ok(CheetahString::from(value))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerTarget {
    BrokerAddr(CheetahString),
    ClusterName(CheetahString),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigQueryRequest {
    target: BrokerTarget,
    key_pattern: Option<String>,
    namesrv_addr: Option<String>,
}

impl BrokerConfigQueryRequest {
    pub fn try_new(
        broker_addr: Option<String>,
        cluster_name: Option<String>,
        key_pattern: Option<String>,
    ) -> RocketMQResult<Self> {
        let broker_addr = trim_optional_string(broker_addr);
        let cluster_name = trim_optional_string(cluster_name);
        let key_pattern = trim_optional_string(key_pattern);

        if let Some(pattern) = &key_pattern {
            Regex::new(pattern).map_err(|error| {
                ToolsError::validation_error("keyPattern", format!("invalid key regex pattern '{pattern}': {error}"))
            })?;
        }

        let target = match (broker_addr, cluster_name) {
            (Some(addr), None) => BrokerTarget::BrokerAddr(trim_required_cheetah("brokerAddr", addr)?),
            (None, Some(cluster)) => BrokerTarget::ClusterName(trim_required_cheetah("clusterName", cluster)?),
            (None, None) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "either brokerAddr or clusterName must be provided",
                )
                .into());
            }
            (Some(_), Some(_)) => {
                return Err(ToolsError::validation_error(
                    "target",
                    "brokerAddr and clusterName cannot be provided together",
                )
                .into());
            }
        };

        Ok(Self {
            target,
            key_pattern,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn target(&self) -> &BrokerTarget {
        &self.target
    }

    pub fn key_pattern(&self) -> Option<&str> {
        self.key_pattern.as_deref()
    }

    pub fn key_pattern_regex(&self) -> RocketMQResult<Option<Regex>> {
        self.key_pattern()
            .map(|pattern| {
                Regex::new(pattern).map_err(|error| {
                    ToolsError::validation_error(
                        "keyPattern",
                        format!("invalid key regex pattern '{pattern}': {error}"),
                    )
                    .into()
                })
            })
            .transpose()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrokerConfigSectionTarget {
    Broker(CheetahString),
    Master(CheetahString),
    Slave {
        master_addr: CheetahString,
        slave_addr: CheetahString,
    },
    NoMaster,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigEntry {
    pub key: CheetahString,
    pub value: CheetahString,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigSection {
    pub target: BrokerConfigSectionTarget,
    pub entries: Vec<BrokerConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerConfigQueryResult {
    pub sections: Vec<BrokerConfigSection>,
    pub key_pattern: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_config_query_request_requires_one_target() {
        let broker_request =
            BrokerConfigQueryRequest::try_new(Some(" 127.0.0.1:10911 ".into()), None, Some(" ^flush.* ".into()))
                .unwrap();
        assert_eq!(
            broker_request.target(),
            &BrokerTarget::BrokerAddr(CheetahString::from("127.0.0.1:10911"))
        );
        assert_eq!(broker_request.key_pattern(), Some("^flush.*"));

        let cluster_request = BrokerConfigQueryRequest::try_new(None, Some(" DefaultCluster ".into()), None).unwrap();
        assert_eq!(
            cluster_request.target(),
            &BrokerTarget::ClusterName(CheetahString::from("DefaultCluster"))
        );

        assert!(BrokerConfigQueryRequest::try_new(None, None, None).is_err());
        assert!(
            BrokerConfigQueryRequest::try_new(Some("127.0.0.1:10911".into()), Some("DefaultCluster".into()), None)
                .is_err()
        );
    }

    #[test]
    fn broker_config_query_request_rejects_invalid_key_pattern() {
        let result = BrokerConfigQueryRequest::try_new(Some("127.0.0.1:10911".into()), None, Some("[".into()));
        assert!(result.is_err());
    }

    #[test]
    fn broker_config_query_request_keeps_namesrv_optional() {
        let request = BrokerConfigQueryRequest::try_new(Some("127.0.0.1:10911".into()), None, None)
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".into()));

        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    }
}
