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

//! NameServer-related types

use std::collections::HashMap;

use cheetah_string::CheetahString;
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

fn parse_namesrv_addrs(namesrv_addr: Option<&str>) -> Vec<CheetahString> {
    namesrv_addr
        .unwrap_or_default()
        .split(';')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
        .map(CheetahString::from)
        .collect()
}

/// NameServer configuration item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigItem {
    pub key: String,
    pub value: String,
    pub description: Option<String>,
}

/// Broker permission status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerPermission {
    pub broker_name: String,
    pub perm: i32,
    pub has_write_perm: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamesrvConfigQueryRequest {
    namesrv_addr: String,
    namesrv_addrs: Vec<CheetahString>,
}

impl NamesrvConfigQueryRequest {
    pub fn try_new(namesrv_addr: Option<String>) -> RocketMQResult<Self> {
        let namesrv_addr = trim_optional_string(namesrv_addr)
            .ok_or_else(|| ToolsError::validation_error("namesrvAddr", "namesrvAddr must be provided"))?;
        let namesrv_addrs = parse_namesrv_addrs(Some(&namesrv_addr));
        if namesrv_addrs.is_empty() {
            return Err(ToolsError::validation_error("namesrvAddr", "namesrvAddr must be provided").into());
        }

        Ok(Self {
            namesrv_addr,
            namesrv_addrs,
        })
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        Some(self.namesrv_addr.as_str())
    }

    pub fn namesrv_addrs(&self) -> Vec<CheetahString> {
        self.namesrv_addrs.clone()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        AdminBuilder::new().namesrv_addr(self.namesrv_addr.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamesrvConfigQueryResult {
    pub configs: HashMap<CheetahString, HashMap<CheetahString, CheetahString>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamesrvConfigUpdateRequest {
    properties: HashMap<CheetahString, CheetahString>,
    namesrv_addr: Option<String>,
}

impl NamesrvConfigUpdateRequest {
    pub fn try_new(
        key: impl Into<String>,
        value: impl Into<String>,
        namesrv_addr: Option<String>,
    ) -> RocketMQResult<Self> {
        let key = trim_required_cheetah("key", key)?;
        let value = trim_required_cheetah("value", value)?;
        let mut properties = HashMap::with_capacity(1);
        properties.insert(key, value);

        Ok(Self {
            properties,
            namesrv_addr: trim_optional_string(namesrv_addr),
        })
    }

    pub fn properties(&self) -> &HashMap<CheetahString, CheetahString> {
        &self.properties
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn namesrv_addrs(&self) -> Option<Vec<CheetahString>> {
        self.namesrv_addr().map(|addr| parse_namesrv_addrs(Some(addr)))
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamesrvConfigUpdateResult {
    pub properties: HashMap<CheetahString, CheetahString>,
    pub namesrv_addrs: Option<Vec<CheetahString>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvConfigUpdateRequest {
    namespace: CheetahString,
    key: CheetahString,
    value: CheetahString,
    namesrv_addr: Option<String>,
}

impl KvConfigUpdateRequest {
    pub fn try_new(
        namespace: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> RocketMQResult<Self> {
        Ok(Self {
            namespace: trim_required_cheetah("namespace", namespace)?,
            key: trim_required_cheetah("key", key)?,
            value: trim_required_cheetah("value", value)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn namespace(&self) -> &CheetahString {
        &self.namespace
    }

    pub fn key(&self) -> &CheetahString {
        &self.key
    }

    pub fn value(&self) -> &CheetahString {
        &self.value
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
pub struct KvConfigDeleteRequest {
    namespace: CheetahString,
    key: CheetahString,
    namesrv_addr: Option<String>,
}

impl KvConfigDeleteRequest {
    pub fn try_new(namespace: impl Into<String>, key: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            namespace: trim_required_cheetah("namespace", namespace)?,
            key: trim_required_cheetah("key", key)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn namespace(&self) -> &CheetahString {
        &self.namespace
    }

    pub fn key(&self) -> &CheetahString {
        &self.key
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvConfigUpdateResult {
    pub namespace: CheetahString,
    pub key: CheetahString,
    pub value: Option<CheetahString>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WritePermRequest {
    broker_name: CheetahString,
    namesrv_addr: Option<String>,
}

impl WritePermRequest {
    pub fn try_new(broker_name: impl Into<String>) -> RocketMQResult<Self> {
        Ok(Self {
            broker_name: trim_required_cheetah("brokerName", broker_name)?,
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn namesrv_addrs(&self) -> Vec<CheetahString> {
        parse_namesrv_addrs(self.namesrv_addr())
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritePermResult {
    pub broker_name: CheetahString,
    pub entries: Vec<WritePermResultEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritePermResultEntry {
    pub namesrv_addr: CheetahString,
    pub affected_count: Option<i32>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namesrv_config_query_request_requires_namesrv_and_splits_addresses() {
        let request =
            NamesrvConfigQueryRequest::try_new(Some(" 127.0.0.1:9876 ; 127.0.0.2:9876 ".to_string())).unwrap();

        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876 ; 127.0.0.2:9876"));
        assert_eq!(
            request.namesrv_addrs(),
            vec![
                CheetahString::from("127.0.0.1:9876"),
                CheetahString::from("127.0.0.2:9876")
            ]
        );
        assert!(NamesrvConfigQueryRequest::try_new(None).is_err());
    }

    #[test]
    fn namesrv_config_update_request_trims_key_value_and_addresses() {
        let request =
            NamesrvConfigUpdateRequest::try_new(" deleteWhen ", " 04 ", Some(" 127.0.0.1:9876 ; ".to_string()))
                .unwrap();

        assert_eq!(
            request.properties().get(&CheetahString::from("deleteWhen")).unwrap(),
            "04"
        );
        assert_eq!(
            request.namesrv_addrs(),
            Some(vec![CheetahString::from("127.0.0.1:9876")])
        );
        assert!(NamesrvConfigUpdateRequest::try_new(" ", "04", None).is_err());
    }

    #[test]
    fn kv_config_request_validates_namespace_key_and_value() {
        let update = KvConfigUpdateRequest::try_new(" ns ", " key ", " value ")
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));
        let delete = KvConfigDeleteRequest::try_new(" ns ", " key ").unwrap();

        assert_eq!(update.namespace().as_str(), "ns");
        assert_eq!(update.key().as_str(), "key");
        assert_eq!(update.value().as_str(), "value");
        assert_eq!(update.namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(delete.key().as_str(), "key");
        assert!(KvConfigUpdateRequest::try_new("ns", "key", " ").is_err());
        assert!(KvConfigDeleteRequest::try_new(" ", "key").is_err());
    }

    #[test]
    fn write_perm_request_trims_broker_and_namesrv() {
        let request = WritePermRequest::try_new(" broker-a ")
            .unwrap()
            .with_optional_namesrv_addr(Some(" 127.0.0.1:9876;127.0.0.2:9876 ".to_string()));

        assert_eq!(request.broker_name().as_str(), "broker-a");
        assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876;127.0.0.2:9876"));
        assert_eq!(
            request.namesrv_addrs(),
            vec![
                CheetahString::from("127.0.0.1:9876"),
                CheetahString::from("127.0.0.2:9876")
            ]
        );
        assert!(WritePermRequest::try_new(" ").is_err());
    }
}
