use std::collections::HashMap;

use anyhow::Error;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NotifyMinBrokerIdChangeRequestHeader {
    #[serde(rename = "minBrokerId")]
    pub min_broker_id: Option<i64>,

    #[serde(rename = "brokerName")]
    pub broker_name: Option<String>,

    #[serde(rename = "minBrokerAddr")]
    pub min_broker_addr: Option<String>,

    #[serde(rename = "offlineBrokerAddr")]
    pub offline_broker_addr: Option<String>,

    #[serde(rename = "haBrokerAddr")]
    pub ha_broker_addr: Option<String>,
}

impl NotifyMinBrokerIdChangeRequestHeader {
    const BROKER_NAME: &'static str = "brokerName";
    const HA_BROKER_ADDR: &'static str = "haBrokerAddr";
    const MIN_BROKER_ADDR: &'static str = "minBrokerAddr";
    const MIN_BROKER_ID: &'static str = "minBrokerId";
    const OFFLINE_BROKER_ADDR: &'static str = "offlineBrokerAddr";

    pub fn new(
        min_broker_id: Option<i64>,
        broker_name: Option<String>,
        min_broker_addr: Option<String>,
        offline_broker_addr: Option<String>,
        ha_broker_addr: Option<String>,
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

    fn from(map: &HashMap<String, String>) -> Option<Self::Target> {
        Some(NotifyMinBrokerIdChangeRequestHeader {
            min_broker_id: map
                .get(NotifyMinBrokerIdChangeRequestHeader::MIN_BROKER_ID)
                .and_then(|s| s.parse::<i64>().ok()),
            broker_name: map
                .get(NotifyMinBrokerIdChangeRequestHeader::BROKER_NAME)
                .map(|s| s.to_string()),
            min_broker_addr: map
                .get(NotifyMinBrokerIdChangeRequestHeader::MIN_BROKER_ADDR)
                .map(|s| s.to_string()),
            offline_broker_addr: map
                .get(NotifyMinBrokerIdChangeRequestHeader::OFFLINE_BROKER_ADDR)
                .map(|s| s.to_string()),
            ha_broker_addr: map
                .get(NotifyMinBrokerIdChangeRequestHeader::HA_BROKER_ADDR)
                .map(|s| s.to_string()),
        })
    }
}

impl CommandCustomHeader for NotifyMinBrokerIdChangeRequestHeader {
    fn check_fields(&self) -> anyhow::Result<(), Error> {
        todo!()
    }

    fn to_map(&self) -> Option<HashMap<String, String>> {
        let mut map = HashMap::<String, String>::new();
        if let Some(min_broker_id) = self.min_broker_id {
            map.insert(
                NotifyMinBrokerIdChangeRequestHeader::MIN_BROKER_ID.to_string(),
                min_broker_id.to_string(),
            );
        }
        if let Some(ref broker_name) = self.broker_name {
            map.insert(
                NotifyMinBrokerIdChangeRequestHeader::BROKER_NAME.to_string(),
                broker_name.clone(),
            );
        }
        if let Some(ref min_broker_addr) = self.min_broker_addr {
            map.insert(
                NotifyMinBrokerIdChangeRequestHeader::MIN_BROKER_ADDR.to_string(),
                min_broker_addr.clone(),
            );
        }

        if let Some(ref ha_broker_addr) = self.ha_broker_addr {
            map.insert(
                NotifyMinBrokerIdChangeRequestHeader::HA_BROKER_ADDR.to_string(),
                ha_broker_addr.clone(),
            );
        }

        if let Some(ref offline_broker_addr) = self.offline_broker_addr {
            map.insert(
                NotifyMinBrokerIdChangeRequestHeader::OFFLINE_BROKER_ADDR.to_string(),
                offline_broker_addr.clone(),
            );
        }

        if let Some(ref ha_broker_addr) = self.ha_broker_addr {
            map.insert(
                NotifyMinBrokerIdChangeRequestHeader::HA_BROKER_ADDR.to_string(),
                ha_broker_addr.to_string(),
            );
        }

        Some(map)
    }
}
