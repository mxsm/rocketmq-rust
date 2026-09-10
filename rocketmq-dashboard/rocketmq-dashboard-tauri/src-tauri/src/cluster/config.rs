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

use crate::audit::types::{AuditReceipt, Summary};
use crate::error::{DashboardError, DashboardResult};
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::core::dashboard::{
    DashboardAdmin, DashboardBrokerConfigUpdateRequest, DashboardBrokerInfo, DashboardBrokerTarget,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct BrokerConfigUpdateRequest {
    pub(crate) cluster_name: String,
    pub(crate) broker_name: String,
    pub(crate) broker_id: u64,
    pub(crate) broker_addr: String,
    pub(crate) entries: BTreeMap<String, String>,
}

impl BrokerConfigUpdateRequest {
    pub(crate) fn validate(&self) -> DashboardResult<()> {
        if self.cluster_name.trim().is_empty()
            || self.broker_name.trim().is_empty()
            || self.broker_addr.trim().is_empty()
        {
            return Err(DashboardError::Validation(
                "An explicit Broker identity is required.".into(),
            ));
        }
        if self.entries.is_empty()
            || self.entries.iter().any(|(key, value)| {
                key.is_empty()
                    || key
                        .chars()
                        .any(|ch| ch.is_whitespace() || "=:# !\\".contains(ch) || ch.is_control())
                    || value.chars().any(|ch| matches!(ch, '\r' | '\n' | '\0'))
            })
        {
            return Err(DashboardError::Validation(
                "Provide changed configuration keys with single-line string values.".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConfigReadBack {
    Confirmed,
    Different,
    Unavailable,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BrokerConfigUpdateResult {
    pub(crate) broker_addr: String,
    pub(crate) written: bool,
    pub(crate) changed_keys: Vec<String>,
    pub(crate) read_back: ConfigReadBack,
    pub(crate) entries: Option<BTreeMap<String, String>>,
}
impl AuditReceipt for BrokerConfigUpdateResult {
    fn summary(&self) -> Summary {
        Summary::count(usize::from(self.written), usize::from(!self.written))
    }
}

pub(super) trait BrokerConfigAccess {
    async fn brokers(&self) -> DashboardResult<Vec<DashboardBrokerInfo>>;
    async fn write(&self, request: &DashboardBrokerConfigUpdateRequest) -> DashboardResult<()>;
    async fn read(&self, target: &DashboardBrokerTarget) -> DashboardResult<BTreeMap<String, String>>;
}
impl BrokerConfigAccess for AdminSession {
    async fn brokers(&self) -> DashboardResult<Vec<DashboardBrokerInfo>> {
        Ok(self.dashboard_list_brokers().await?.items)
    }
    async fn write(&self, request: &DashboardBrokerConfigUpdateRequest) -> DashboardResult<()> {
        self.dashboard_update_broker_config(request).await?;
        Ok(())
    }
    async fn read(&self, target: &DashboardBrokerTarget) -> DashboardResult<BTreeMap<String, String>> {
        Ok(self.dashboard_broker_config(target).await?.entries)
    }
}

pub(super) async fn update_config(
    admin: &impl BrokerConfigAccess,
    request: BrokerConfigUpdateRequest,
) -> DashboardResult<BrokerConfigUpdateResult> {
    request.validate()?;
    let brokers = admin.brokers().await?;
    if !brokers.iter().any(|broker| {
        broker.cluster_name == request.cluster_name
            && broker.broker_name == request.broker_name
            && broker.broker_id == request.broker_id
            && broker.address == request.broker_addr
    }) {
        return Err(DashboardError::Validation(
            "The selected Broker identity is no longer in the current cluster.".into(),
        ));
    }
    let target = DashboardBrokerTarget {
        broker_name: request.broker_name.clone(),
        broker_addr: Some(request.broker_addr.clone()),
    };
    let changed_keys = request.entries.keys().cloned().collect();
    admin
        .write(&DashboardBrokerConfigUpdateRequest {
            broker_name: request.broker_name,
            broker_addr: Some(request.broker_addr.clone()),
            entries: request.entries.clone(),
        })
        .await?;
    // A failed read cannot reverse an acknowledged write, and must never replay it.
    let (read_back, entries) = match admin.read(&target).await {
        Ok(entries) => {
            let status = if request
                .entries
                .iter()
                .all(|(key, value)| entries.get(key) == Some(value))
            {
                ConfigReadBack::Confirmed
            } else {
                ConfigReadBack::Different
            };
            (status, Some(entries))
        }
        Err(_) => (ConfigReadBack::Unavailable, None),
    };
    Ok(BrokerConfigUpdateResult {
        broker_addr: request.broker_addr,
        written: true,
        changed_keys,
        read_back,
        entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    struct Fake {
        calls: RefCell<Vec<&'static str>>,
        read_fails: bool,
    }
    impl BrokerConfigAccess for Fake {
        async fn brokers(&self) -> DashboardResult<Vec<DashboardBrokerInfo>> {
            self.calls.borrow_mut().push("brokers");
            Ok(vec![DashboardBrokerInfo {
                cluster_name: "cluster".into(),
                broker_name: "broker-a".into(),
                broker_id: 0,
                address: "127.0.0.1:10911".into(),
                role: "MASTER".into(),
                version: "".into(),
                produce_tps: 0.0,
                consume_tps: 0.0,
                runtime_entries: BTreeMap::new(),
                runtime_error: None,
            }])
        }
        async fn write(&self, request: &DashboardBrokerConfigUpdateRequest) -> DashboardResult<()> {
            self.calls.borrow_mut().push("write");
            assert_eq!(request.broker_addr.as_deref(), Some("127.0.0.1:10911"));
            assert_eq!(
                request.entries,
                BTreeMap::from([("brokerPermission".into(), "6".into())])
            );
            Ok(())
        }
        async fn read(&self, _: &DashboardBrokerTarget) -> DashboardResult<BTreeMap<String, String>> {
            self.calls.borrow_mut().push("read");
            if self.read_fails {
                Err(DashboardError::Internal("read unavailable"))
            } else {
                Ok(BTreeMap::from([("brokerPermission".into(), "6".into())]))
            }
        }
    }
    fn request() -> BrokerConfigUpdateRequest {
        BrokerConfigUpdateRequest {
            cluster_name: "cluster".into(),
            broker_name: "broker-a".into(),
            broker_id: 0,
            broker_addr: "127.0.0.1:10911".into(),
            entries: BTreeMap::from([("brokerPermission".into(), "6".into())]),
        }
    }
    #[tokio::test]
    async fn write_success_and_read_failure_are_independent() {
        for read_fails in [false, true] {
            let admin = Fake {
                calls: RefCell::new(vec![]),
                read_fails,
            };
            let result = update_config(&admin, request()).await.unwrap();
            assert!(result.written);
            assert_eq!(
                result.read_back,
                if read_fails {
                    ConfigReadBack::Unavailable
                } else {
                    ConfigReadBack::Confirmed
                }
            );
            assert_eq!(*admin.calls.borrow(), ["brokers", "write", "read"]);
            assert_eq!(result.summary().outcome, crate::audit::types::Outcome::Success);
        }
    }
    #[tokio::test]
    async fn invalid_patch_and_changed_identity_never_write() {
        let admin = Fake {
            calls: RefCell::new(vec![]),
            read_fails: false,
        };
        let mut invalid = request();
        invalid.entries.insert(" ".into(), "x".into());
        assert!(update_config(&admin, invalid).await.is_err());
        assert!(admin.calls.borrow().is_empty());
        let mut stale = request();
        stale.broker_id = 1;
        assert!(update_config(&admin, stale).await.is_err());
        assert_eq!(*admin.calls.borrow(), ["brokers"]);
        let mut json = serde_json::json!({"clusterName":"cluster", "brokerName":"broker-a", "brokerId":0,
            "brokerAddr":"127.0.0.1:10911", "entries":{"brokerPermission":6}});
        assert!(serde_json::from_value::<BrokerConfigUpdateRequest>(json.clone()).is_err());
        json["entries"] = serde_json::json!({"brokerPermission":"6\ninjected=true"});
        assert!(
            serde_json::from_value::<BrokerConfigUpdateRequest>(json)
                .unwrap()
                .validate()
                .is_err()
        );
    }
}
