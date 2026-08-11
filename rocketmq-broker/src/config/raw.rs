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
use std::sync::Once;

use cheetah_string::CheetahString;
use config::Config;
use config::File;
use rocketmq_observability::LoggingOverrides;
use rocketmq_observability::ReloadConfig;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StoreCompatibilityProfile;
use serde::Deserialize;
use tracing::warn;

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
    #[serde(skip)]
    store_markers: StoreOwnershipMarkers,
    #[serde(skip)]
    warn_implicit_store_profile: bool,
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

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct CanonicalOwnershipMarkers {
    broker: BrokerOwnershipMarkers,
    store: StoreOwnershipMarkers,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct BrokerOwnershipMarkers {
    broker_server_config: ServerOwnershipMarkers,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct ServerOwnershipMarkers {
    listen_port: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct StoreOwnershipMarkers {
    enable_controller_mode: Option<bool>,
    duplication_enable: Option<bool>,
    compatibility_profile: Option<StoreCompatibilityProfile>,
    max_recovery_commit_log_files: Option<usize>,
    flush_commit_log_least_pages: Option<i32>,
    commit_commit_log_least_pages: Option<i32>,
    flush_consume_queue_least_pages: Option<usize>,
    flush_consume_queue_thorough_interval: Option<usize>,
    flush_disk_type: Option<rocketmq_store::FlushDiskType>,
    slave_timeout: Option<usize>,
    transient_store_pool_size: Option<usize>,
    min_in_sync_replicas: Option<usize>,
    ha_max_time_slave_not_catchup: Option<usize>,
    all_ack_in_sync_state_set: Option<bool>,
}

impl CanonicalOwnershipMarkers {
    fn validate(&self) -> Result<(), BrokerConfigError> {
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

impl StoreOwnershipMarkers {
    fn all_explicit(store: &MessageStoreConfig) -> Self {
        Self {
            enable_controller_mode: Some(store.enable_controller_mode),
            duplication_enable: Some(store.duplication_enable),
            compatibility_profile: Some(store.compatibility_profile),
            max_recovery_commit_log_files: Some(store.max_recovery_commit_log_files),
            flush_commit_log_least_pages: Some(store.flush_commit_log_least_pages),
            commit_commit_log_least_pages: Some(store.commit_commit_log_least_pages),
            flush_consume_queue_least_pages: Some(store.flush_consume_queue_least_pages),
            flush_consume_queue_thorough_interval: Some(store.flush_consume_queue_thorough_interval),
            flush_disk_type: Some(store.flush_disk_type),
            slave_timeout: Some(store.slave_timeout),
            transient_store_pool_size: Some(store.transient_store_pool_size),
            min_in_sync_replicas: Some(store.min_in_sync_replicas),
            ha_max_time_slave_not_catchup: Some(store.ha_max_time_slave_not_catchup),
            all_ack_in_sync_state_set: Some(store.all_ack_in_sync_state_set),
        }
    }
}

fn apply_store_profile_defaults(store: &mut MessageStoreConfig, markers: &StoreOwnershipMarkers) {
    if store.compatibility_profile.is_legacy() {
        return;
    }
    let preset = MessageStoreConfig::for_compatibility_profile(store.compatibility_profile);
    macro_rules! apply_when_omitted {
        ($field:ident) => {
            if markers.$field.is_none() {
                store.$field = preset.$field;
            }
        };
    }
    apply_when_omitted!(max_recovery_commit_log_files);
    apply_when_omitted!(flush_commit_log_least_pages);
    apply_when_omitted!(commit_commit_log_least_pages);
    apply_when_omitted!(flush_consume_queue_least_pages);
    apply_when_omitted!(flush_consume_queue_thorough_interval);
    apply_when_omitted!(flush_disk_type);
    apply_when_omitted!(slave_timeout);
    apply_when_omitted!(transient_store_pool_size);
    apply_when_omitted!(min_in_sync_replicas);
    apply_when_omitted!(ha_max_time_slave_not_catchup);
    apply_when_omitted!(all_ack_in_sync_state_set);
}

fn warn_implicit_legacy_profile_once() {
    static WARNING: Once = Once::new();
    WARNING.call_once(|| {
        warn!(
            "store.compatibilityProfile is omitted; preserving LEGACY_RUST defaults for this release. Set \
             JAVA_5_5 or DURABILITY_STRICT explicitly after reviewing durability and RPO behavior"
        );
    });
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
        let mut raw: Self = loaded
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
        raw.warn_implicit_store_profile = ownership.store.compatibility_profile.is_none();
        raw.store_markers = ownership.store;
        Ok(raw)
    }

    #[must_use]
    pub fn from_parts(broker: BrokerConfig, store: MessageStoreConfig) -> Self {
        let store_markers = StoreOwnershipMarkers::all_explicit(&store);
        Self {
            broker,
            store,
            store_markers,
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
        if self.warn_implicit_store_profile {
            warn_implicit_legacy_profile_once();
        }
        apply_store_profile_defaults(&mut self.store, &self.store_markers);
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
