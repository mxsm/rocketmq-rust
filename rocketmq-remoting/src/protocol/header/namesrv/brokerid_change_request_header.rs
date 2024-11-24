use std::collections::HashMap;

use anyhow::Error;
use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NotifyMinBrokerIdChangeRequestHeader {
    #[serde(rename = "minBrokerId")]
    pub min_broker_id: Option<u64>,

    #[serde(rename = "brokerName")]
    pub broker_name: Option<CheetahString>,

    #[serde(rename = "minBrokerAddr")]
    pub min_broker_addr: Option<CheetahString>,

    #[serde(rename = "offlineBrokerAddr")]
    pub offline_broker_addr: Option<CheetahString>,

    #[serde(rename = "haBrokerAddr")]
    pub ha_broker_addr: Option<CheetahString>,
}

impl NotifyMinBrokerIdChangeRequestHeader {
    const BROKER_NAME: &'static str = "brokerName";
    const HA_BROKER_ADDR: &'static str = "haBrokerAddr";
    const MIN_BROKER_ADDR: &'static str = "minBrokerAddr";
    const MIN_BROKER_ID: &'static str = "minBrokerId";
    const OFFLINE_BROKER_ADDR: &'static str = "offlineBrokerAddr";

    pub fn new(
        min_broker_id: Option<u64>,
        broker_name: Option<CheetahString>,
        min_broker_addr: Option<CheetahString>,
        offline_broker_addr: Option<CheetahString>,
        ha_broker_addr: Option<CheetahString>,
    ) -> Self {
        NotifyMinBrokerIdChangeRequestHeader {
            min_broker_id,
            broker_name,
            min_broker_addr,
            offline_broker_addr,
            ha_broker_addr,
        }
    }
}

impl FromMap for NotifyMinBrokerIdChangeRequestHeader {
    type Target = Self;

    fn from(map: &HashMap<CheetahString, CheetahString>) -> Option<Self::Target> {
        Some(NotifyMinBrokerIdChangeRequestHeader {
            min_broker_id: map
                .get(&CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::MIN_BROKER_ID,
                ))
                .and_then(|s| s.parse::<u64>().ok()),
            broker_name: map
                .get(&CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::BROKER_NAME,
                ))
                .cloned(),
            min_broker_addr: map
                .get(&CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::MIN_BROKER_ADDR,
                ))
                .cloned(),
            offline_broker_addr: map
                .get(&CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::OFFLINE_BROKER_ADDR,
                ))
                .cloned(),
            ha_broker_addr: map
                .get(&CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::HA_BROKER_ADDR,
                ))
                .cloned(),
        })
    }
}

impl CommandCustomHeader for NotifyMinBrokerIdChangeRequestHeader {
    fn check_fields(&self) -> anyhow::Result<(), Error> {
        todo!()
    }

    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>> {
        let mut map = HashMap::<CheetahString, CheetahString>::new();
        if let Some(min_broker_id) = self.min_broker_id {
            map.insert(
                CheetahString::from_static_str(NotifyMinBrokerIdChangeRequestHeader::MIN_BROKER_ID),
                CheetahString::from_string(min_broker_id.to_string()),
            );
        }
        if let Some(ref broker_name) = self.broker_name {
            map.insert(
                CheetahString::from_static_str(NotifyMinBrokerIdChangeRequestHeader::BROKER_NAME),
                broker_name.clone(),
            );
        }
        if let Some(ref min_broker_addr) = self.min_broker_addr {
            map.insert(
                CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::MIN_BROKER_ADDR,
                ),
                min_broker_addr.clone(),
            );
        }

        if let Some(ref ha_broker_addr) = self.ha_broker_addr {
            map.insert(
                CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::HA_BROKER_ADDR,
                ),
                ha_broker_addr.clone(),
            );
        }

        if let Some(ref offline_broker_addr) = self.offline_broker_addr {
            map.insert(
                CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::OFFLINE_BROKER_ADDR,
                ),
                offline_broker_addr.clone(),
            );
        }

        if let Some(ref ha_broker_addr) = self.ha_broker_addr {
            map.insert(
                CheetahString::from_static_str(
                    NotifyMinBrokerIdChangeRequestHeader::HA_BROKER_ADDR,
                ),
                ha_broker_addr.clone(),
            );
        }

        Some(map)
    }
}
