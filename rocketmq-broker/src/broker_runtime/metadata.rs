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

use super::*;
pub(super) struct BrokerMetadata {
    pub(super) configuration_error: Option<String>,
    #[cfg(feature = "rocksdb_store")]
    pub(super) rocksdb_config_managers: Option<BrokerRocksDbConfigManagers>,
}

impl BrokerMetadata {
    pub(super) fn new(
        configuration_error: Option<String>,
        #[cfg(feature = "rocksdb_store")] rocksdb_config_managers: Option<BrokerRocksDbConfigManagers>,
    ) -> Self {
        Self {
            configuration_error,
            #[cfg(feature = "rocksdb_store")]
            rocksdb_config_managers,
        }
    }
}

impl BrokerRuntime {
    /// Load the original configuration data from the corresponding configuration files
    /// located under the `${HOME}\config` directory.
    ///
    /// This function initializes broker metadata by loading several manager components
    /// in sequence:
    /// - Topic configuration manager
    /// - Topic queue mapping manager
    /// - Consumer offset manager
    /// - Subscription group manager
    /// - Consumer filter manager
    /// - Consumer order information manager
    ///
    /// The loaders are invoked in order and combined using logical AND. If all loaders
    /// return `true`, the function returns `true`. If any loader fails (returns `false`),
    /// the whole initialization is considered failed and the function returns `false`.
    pub(super) async fn initialize_metadata(&self) -> Result<(), BrokerStartupError> {
        info!("======Starting initialize metadata========");
        if let Some(Err(error)) = self.composition.state.metadata_io.as_ref() {
            return Err(BrokerStartupError::Initialization {
                component: "metadata_io_actor",
                detail: error.to_string(),
            });
        }
        match self.composition.state.topic_config_coordinator().load().await {
            Ok(true) => {}
            Ok(false) => {
                return Err(BrokerStartupError::MetadataLoad {
                    component: "topic_config",
                });
            }
            Err(error) => {
                return Err(BrokerStartupError::Initialization {
                    component: "topic_config",
                    detail: error.to_string(),
                });
            }
        }
        for (component, loaded) in [
            (
                "topic_queue_mapping",
                self.composition.state.topic_queue_mapping_manager().load(),
            ),
            (
                "consumer_offset",
                self.composition.state.consumer_offset_manager().load(),
            ),
            (
                "subscription_group",
                self.composition.state.subscription_group_manager().load(),
            ),
            (
                "consumer_filter",
                self.composition.state.consumer_filter_manager().load(),
            ),
            (
                "consumer_order_info",
                self.composition.state.consumer_order_info_manager().load(),
            ),
        ] {
            if !loaded {
                return Err(BrokerStartupError::MetadataLoad { component });
            }
        }
        Ok(())
    }

    pub(super) async fn update_namesrv_addr(&mut self) {
        self.composition.state.update_namesrv_addr_inner().await;
    }

    /// Register broker to name remoting_server
    pub(crate) async fn register_broker_all(
        &mut self,
        check_order_config: bool,
        oneway: bool,
        force_register: bool,
    ) -> Result<BrokerRegistrationStatus, BrokerRegistrationError> {
        self.composition
            .state
            .build_registration_runtime()
            .register_broker_all(check_order_config, oneway, force_register)
            .await
    }
}
