// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper as Canonical;
use rocketmq_protocol::protocol::DataVersion;

#[derive(Debug, Clone)]
pub struct SubscriptionGroupWrapper {
    pub subscription_group_table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>>,
    pub forbidden_table: DashMap<CheetahString, HashMap<CheetahString, i32>>,
    pub data_version: DataVersion,
}

impl Default for SubscriptionGroupWrapper {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionGroupWrapper {
    pub fn new() -> Self {
        Self {
            subscription_group_table: DashMap::with_capacity(1024),
            forbidden_table: DashMap::with_capacity(1024),
            data_version: DataVersion::default(),
        }
    }
}

impl Serialize for SubscriptionGroupWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Canonical::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SubscriptionGroupWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Canonical::deserialize(deserializer).map(Self::from)
    }
}

impl From<&SubscriptionGroupWrapper> for Canonical {
    fn from(value: &SubscriptionGroupWrapper) -> Self {
        Self {
            subscription_group_table: value
                .subscription_group_table
                .iter()
                .map(|entry| (entry.key().clone(), (**entry.value()).clone()))
                .collect(),
            forbidden_table: value
                .forbidden_table
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect(),
            data_version: value.data_version.clone(),
        }
    }
}

impl From<Canonical> for SubscriptionGroupWrapper {
    fn from(value: Canonical) -> Self {
        Self {
            subscription_group_table: value
                .subscription_group_table
                .into_iter()
                .map(|(key, config)| (key, Arc::new(config)))
                .collect(),
            forbidden_table: value.forbidden_table.into_iter().collect(),
            data_version: value.data_version,
        }
    }
}

impl SubscriptionGroupWrapper {
    pub fn get_subscription_group_table(&self) -> &DashMap<CheetahString, Arc<SubscriptionGroupConfig>> {
        &self.subscription_group_table
    }

    pub fn get_subscription_group_table_mut(&mut self) -> &DashMap<CheetahString, Arc<SubscriptionGroupConfig>> {
        &self.subscription_group_table
    }

    pub fn set_subscription_group_table(&mut self, table: DashMap<CheetahString, Arc<SubscriptionGroupConfig>>) {
        self.subscription_group_table = table;
    }

    pub fn forbidden_table(&self) -> &DashMap<CheetahString, HashMap<CheetahString, i32>> {
        &self.forbidden_table
    }

    pub fn set_forbidden_table(&mut self, table: DashMap<CheetahString, HashMap<CheetahString, i32>>) {
        self.forbidden_table = table;
    }

    pub fn data_version(&self) -> &DataVersion {
        &self.data_version
    }

    pub fn set_data_version(&mut self, version: DataVersion) {
        self.data_version = version;
    }
}
