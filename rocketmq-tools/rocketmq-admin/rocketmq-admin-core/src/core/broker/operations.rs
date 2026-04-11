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

//! Broker operations - core business logic.

use std::collections::HashMap;

use cheetah_string::CheetahString;
use regex::Regex;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;

use super::types::BrokerConfigEntry;
use super::types::BrokerConfigQueryRequest;
use super::types::BrokerConfigQueryResult;
use super::types::BrokerConfigSection;
use super::types::BrokerConfigSectionTarget;
use super::types::BrokerTarget;
use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::resolver::BrokerAddressResolver;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;

pub struct BrokerService;

impl BrokerService {
    pub async fn query_broker_config_by_request(
        request: BrokerConfigQueryRequest,
    ) -> RocketMQResult<BrokerConfigQueryResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::query_broker_config_with_admin(&mut admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_broker_config_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &BrokerConfigQueryRequest,
    ) -> RocketMQResult<BrokerConfigQueryResult> {
        let key_pattern = request.key_pattern_regex()?;
        let mut sections = Vec::new();

        match request.target() {
            BrokerTarget::BrokerAddr(addr) => {
                let entries = Self::get_broker_config_entries(admin, addr, key_pattern.as_ref()).await?;
                sections.push(BrokerConfigSection {
                    target: BrokerConfigSectionTarget::Broker(addr.clone()),
                    entries,
                });
            }
            BrokerTarget::ClusterName(cluster_name) => {
                let cluster_info = admin.examine_broker_cluster_info().await.map_err(|error| {
                    RocketMQError::Internal(format!("BrokerService: failed to examine broker cluster info: {error}"))
                })?;
                let master_and_slave_map =
                    BrokerAddressResolver::fetch_master_and_slave_distinguish(&cluster_info, cluster_name.as_str())?;

                let mut sorted_masters: Vec<_> = master_and_slave_map.keys().cloned().collect();
                sorted_masters.sort();

                for master_addr in sorted_masters {
                    let slave_addrs = master_and_slave_map.get(&master_addr).cloned().unwrap_or_default();
                    if master_addr.as_str() == BrokerAddressResolver::NO_MASTER_PLACEHOLDER {
                        sections.push(BrokerConfigSection {
                            target: BrokerConfigSectionTarget::NoMaster,
                            entries: Vec::new(),
                        });
                    } else {
                        let entries =
                            Self::get_broker_config_entries(admin, &master_addr, key_pattern.as_ref()).await?;
                        sections.push(BrokerConfigSection {
                            target: BrokerConfigSectionTarget::Master(master_addr.clone()),
                            entries,
                        });
                    }

                    let mut sorted_slaves = slave_addrs;
                    sorted_slaves.sort();
                    for slave_addr in sorted_slaves {
                        let entries = Self::get_broker_config_entries(admin, &slave_addr, key_pattern.as_ref()).await?;
                        sections.push(BrokerConfigSection {
                            target: BrokerConfigSectionTarget::Slave {
                                master_addr: master_addr.clone(),
                                slave_addr,
                            },
                            entries,
                        });
                    }
                }
            }
        }

        Ok(BrokerConfigQueryResult {
            sections,
            key_pattern: request.key_pattern().map(ToOwned::to_owned),
        })
    }

    async fn get_broker_config_entries(
        admin: &DefaultMQAdminExt,
        broker_addr: &CheetahString,
        key_pattern: Option<&Regex>,
    ) -> RocketMQResult<Vec<BrokerConfigEntry>> {
        let properties = admin.get_broker_config(broker_addr.clone()).await.map_err(|error| {
            RocketMQError::Internal(format!(
                "BrokerService: failed to get broker config for {}: {}",
                broker_addr, error
            ))
        })?;

        Ok(filter_and_sort_properties(properties, key_pattern))
    }
}

fn filter_and_sort_properties(
    properties: HashMap<CheetahString, CheetahString>,
    key_pattern: Option<&Regex>,
) -> Vec<BrokerConfigEntry> {
    let mut entries = properties
        .into_iter()
        .filter(|(key, _)| key_pattern.is_none_or(|regex| regex.is_match(key.as_str())))
        .map(|(key, value)| BrokerConfigEntry { key, value })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_and_sort_properties_applies_key_pattern() {
        let mut properties = HashMap::new();
        properties.insert(CheetahString::from("brokerRole"), CheetahString::from("ASYNC_MASTER"));
        properties.insert(CheetahString::from("flushDiskType"), CheetahString::from("ASYNC_FLUSH"));
        properties.insert(CheetahString::from("flushInterval"), CheetahString::from("500"));

        let pattern = Regex::new("^flush").unwrap();
        let entries = filter_and_sort_properties(properties, Some(&pattern));

        assert_eq!(
            entries.iter().map(|entry| entry.key.as_str()).collect::<Vec<_>>(),
            vec!["flushDiskType", "flushInterval"]
        );
    }
}
