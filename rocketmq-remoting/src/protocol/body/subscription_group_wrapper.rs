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

use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use crate::protocol::DataVersion;

#[derive(Debug, Clone)]
pub struct SubscriptionGroupWrapper {
    pub subscription_group_table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>>,
    pub data_version: DataVersion,
}

// Custom serialization to handle Arc inside DashMap
impl Serialize for SubscriptionGroupWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("SubscriptionGroupWrapper", 2)?;

        // Serialize DashMap by converting Arc values to direct values
        let table: std::collections::HashMap<CheetahString, SubscriptionGroupConfig> = self
            .subscription_group_table
            .iter()
            .map(|entry| (entry.key().clone(), (**entry.value()).clone()))
            .collect();
        state.serialize_field("subscriptionGroupTable", &table)?;
        state.serialize_field("dataVersion", &self.data_version)?;
        state.end()
    }
}

// Custom deserialization to wrap values in Arc
impl<'de> Deserialize<'de> for SubscriptionGroupWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use std::fmt;

        use serde::de::MapAccess;
        use serde::de::Visitor;
        use serde::de::{self};

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "camelCase")]
        enum Field {
            SubscriptionGroupTable,
            DataVersion,
        }

        struct SubscriptionGroupWrapperVisitor;

        impl<'de> Visitor<'de> for SubscriptionGroupWrapperVisitor {
            type Value = SubscriptionGroupWrapper;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct SubscriptionGroupWrapper")
            }

            fn visit_map<V>(self, mut map: V) -> Result<SubscriptionGroupWrapper, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut subscription_group_table: Option<
                    std::collections::HashMap<CheetahString, SubscriptionGroupConfig>,
                > = None;
                let mut data_version: Option<DataVersion> = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::SubscriptionGroupTable => {
                            if subscription_group_table.is_some() {
                                return Err(de::Error::duplicate_field("subscriptionGroupTable"));
                            }
                            subscription_group_table = Some(map.next_value()?);
                        }
                        Field::DataVersion => {
                            if data_version.is_some() {
                                return Err(de::Error::duplicate_field("dataVersion"));
                            }
                            data_version = Some(map.next_value()?);
                        }
                    }
                }

                let subscription_group_table =
                    subscription_group_table.ok_or_else(|| de::Error::missing_field("subscriptionGroupTable"))?;
                let data_version = data_version.ok_or_else(|| de::Error::missing_field("dataVersion"))?;

                // Convert HashMap to DashMap with Arc-wrapped values
                let dash_map = DashMap::new();
                for (key, value) in subscription_group_table {
                    dash_map.insert(key, Arc::new(value));
                }

                Ok(SubscriptionGroupWrapper {
                    subscription_group_table: dash_map,
                    data_version,
                })
            }
        }

        const FIELDS: &[&str] = &["subscriptionGroupTable", "dataVersion"];
        deserializer.deserialize_struct("SubscriptionGroupWrapper", FIELDS, SubscriptionGroupWrapperVisitor)
    }
}

impl Default for SubscriptionGroupWrapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionGroupWrapper {
    pub fn new() -> Self {
        SubscriptionGroupWrapper {
            subscription_group_table: DashMap::with_capacity(1024),
            data_version: DataVersion::default(),
        }
    }

    pub fn get_subscription_group_table(&self) -> &DashMap<CheetahString, Arc<SubscriptionGroupConfig>> {
        &self.subscription_group_table
    }

    pub fn set_subscription_group_table(&mut self, table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>>) {
        self.subscription_group_table = table;
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }

    pub fn set_data_version(&mut self, version: DataVersion) {
        self.data_version = version;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;

    #[test]
    fn new_creates_wrapper_with_default_values() {
        let wrapper = SubscriptionGroupWrapper::new();

        assert_eq!(wrapper.subscription_group_table.len(), 0);
        assert!(wrapper.data_version.timestamp <= DataVersion::default().timestamp);
    }

    #[test]
    fn get_subscription_group_table_returns_reference() {
        let wrapper = SubscriptionGroupWrapper::new();
        wrapper
            .subscription_group_table
            .insert("test_group".into(), Arc::new(SubscriptionGroupConfig::default()));

        let table = wrapper.get_subscription_group_table();
        assert_eq!(table.len(), 1);
        assert!(table.contains_key("test_group"));
    }
}
