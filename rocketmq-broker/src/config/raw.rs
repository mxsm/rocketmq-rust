// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::Path;

use cheetah_string::CheetahString;
use config::Config;
use config::File;
use rocketmq_observability::LoggingOverrides;
use rocketmq_observability::ReloadConfig;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use serde::Deserialize;

use super::broker_config::BrokerConfig;
use super::error::BrokerConfigError;

/// Deserialization-only representation of one canonical broker configuration file.
///
/// The canonical schema has explicit `[broker]`, `[store]`, and `[logging]`
/// sections. This type cannot be passed to the running broker; callers must
/// validate it into [`super::validated::ValidatedBrokerConfig`].
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RawBrokerConfig {
    broker: BrokerConfig,
    store: MessageStoreConfig,
    logging: RawLoggingConfig,
    log_filter: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawLoggingConfig {
    filter: Option<String>,
    reload: RawReloadConfig,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawReloadConfig {
    enabled: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct CanonicalOwnershipMarkers {
    broker: BrokerOwnershipMarkers,
    store: StoreOwnershipMarkers,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct BrokerOwnershipMarkers {
    broker_server_config: ServerOwnershipMarkers,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct ServerOwnershipMarkers {
    listen_port: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct StoreOwnershipMarkers {
    enable_controller_mode: Option<bool>,
    duplication_enable: Option<bool>,
}

impl CanonicalOwnershipMarkers {
    fn validate(self) -> Result<(), BrokerConfigError> {
        if self.broker.broker_server_config.listen_port.is_some() {
            return Err(BrokerConfigError::invalid(
                super::error::ConfigSection::Network,
                "broker.brokerServerConfig.listenPort",
                "is derived from broker.listenPort and must not be configured separately",
            ));
        }
        if self.store.enable_controller_mode.is_some() {
            return Err(BrokerConfigError::invalid(
                super::error::ConfigSection::HighAvailability,
                "store.enableControllerMode",
                "is derived from broker.enableControllerMode and must not be configured separately",
            ));
        }
        if self.store.duplication_enable.is_some() {
            return Err(BrokerConfigError::invalid(
                super::error::ConfigSection::HighAvailability,
                "store.duplicationEnable",
                "is derived from broker.duplicationEnable and must not be configured separately",
            ));
        }
        Ok(())
    }
}

impl RawBrokerConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, BrokerConfigError> {
        let path = path.as_ref();
        let loaded = Config::builder()
            .add_source(File::from(path))
            .build()
            .map_err(|source| BrokerConfigError::Load {
                path: path.to_path_buf(),
                source,
            })?;
        let raw = loaded
            .clone()
            .try_deserialize()
            .map_err(|source| BrokerConfigError::Load {
                path: path.to_path_buf(),
                source,
            })?;
        let ownership: CanonicalOwnershipMarkers =
            loaded.try_deserialize().map_err(|source| BrokerConfigError::Load {
                path: path.to_path_buf(),
                source,
            })?;
        ownership.validate()?;
        Ok(raw)
    }

    #[must_use]
    pub fn from_parts(broker: BrokerConfig, store: MessageStoreConfig) -> Self {
        Self {
            broker,
            store,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn broker(&self) -> &BrokerConfig {
        &self.broker
    }

    #[must_use]
    pub fn store(&self) -> &MessageStoreConfig {
        &self.store
    }

    #[must_use]
    pub fn logging_overrides(&self) -> LoggingOverrides {
        LoggingOverrides {
            logging: rocketmq_observability::LoggingOverrideConfig {
                filter: self.logging.filter.clone(),
                reload: ReloadConfig {
                    enabled: self.logging.reload.enabled,
                },
            },
            log_filter: self.log_filter.clone(),
        }
    }

    pub fn set_name_server_addresses(&mut self, addresses: impl Into<CheetahString>) {
        self.broker.namesrv_addr = Some(addresses.into());
    }

    pub(crate) fn into_normalized_parts(mut self) -> (BrokerConfig, MessageStoreConfig, LoggingOverrides) {
        normalize_config_parts(&mut self.broker, &mut self.store);
        let logging = self.logging_overrides();
        (self.broker, self.store, logging)
    }
}

pub(super) fn normalize_config_parts(broker: &mut BrokerConfig, store: &mut MessageStoreConfig) {
    broker.broker_server_config.listen_port = broker.listen_port;
    store.enable_controller_mode = broker.enable_controller_mode;
    store.duplication_enable = broker.duplication_enable;
}
