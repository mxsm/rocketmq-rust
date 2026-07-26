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

use arc_swap::ArcSwap;
use rocketmq_model::common::broker::broker_role::BrokerRole;

use crate::config::broker_config::BrokerConfig;
use crate::config::error::BrokerConfigError;
use crate::config::transaction::ConfigUpdateTransaction;
use crate::config::validated::ConfigGeneration;
use crate::config::validated::ValidatedBrokerConfig;
use rocketmq_store::MessageStoreConfig;

/// An atomically published broker and message-store configuration generation.
///
/// Keeping both configurations in one generation prevents an admin update from
/// overwriting a controller role transition (or vice versa) with a stale copy of
/// the other configuration.
pub(crate) struct BrokerRuntimeConfigGeneration {
    id: ConfigGeneration,
    config: Arc<ValidatedBrokerConfig>,
}

impl BrokerRuntimeConfigGeneration {
    pub(crate) const fn id(&self) -> ConfigGeneration {
        self.id
    }

    pub(crate) fn validated(&self) -> &Arc<ValidatedBrokerConfig> {
        &self.config
    }

    pub(crate) fn broker(&self) -> &Arc<BrokerConfig> {
        // The generation owns the immutable validated envelope. Each legacy
        // capability receives only the narrow Arc it already understands.
        self.config.broker_arc_ref()
    }

    pub(crate) fn store(&self) -> &Arc<MessageStoreConfig> {
        self.config.store_arc_ref()
    }
}

#[derive(Clone)]
pub(crate) struct BrokerRuntimeConfigState {
    current: Arc<ArcSwap<BrokerRuntimeConfigGeneration>>,
}

impl BrokerRuntimeConfigState {
    pub(crate) fn new(config: Arc<ValidatedBrokerConfig>) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(BrokerRuntimeConfigGeneration {
                id: ConfigGeneration::INITIAL,
                config,
            })),
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<BrokerRuntimeConfigGeneration> {
        self.current.load_full()
    }

    pub(crate) fn broker_snapshot(&self) -> Arc<BrokerConfig> {
        Arc::clone(self.snapshot().broker())
    }

    pub(crate) fn store_snapshot(&self) -> Arc<MessageStoreConfig> {
        Arc::clone(self.snapshot().store())
    }

    pub(crate) fn commit(
        &self,
        transaction: ConfigUpdateTransaction,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        let expected = transaction.expected_generation();
        let current = self.snapshot();
        if current.id() != expected {
            return Err(BrokerConfigError::GenerationConflict {
                expected: expected.value(),
                actual: current.id().value(),
            });
        }
        let next_id = expected.checked_next().ok_or(BrokerConfigError::GenerationExhausted)?;

        let next = Arc::new(BrokerRuntimeConfigGeneration {
            id: next_id,
            config: Arc::new(transaction.into_candidate()),
        });
        let previous = self.current.compare_and_swap(&current, Arc::clone(&next));
        if Arc::ptr_eq(&previous, &current) {
            return Ok(next);
        }

        Err(BrokerConfigError::GenerationConflict {
            expected: expected.value(),
            actual: previous.id().value(),
        })
    }

    pub(crate) fn replace_broker(
        &self,
        broker: BrokerConfig,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.replace_with(|current| current.with_broker_candidate(broker.clone()))
    }

    pub(crate) fn replace_store(
        &self,
        store: MessageStoreConfig,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.replace_with(|current| current.with_store_candidate(store.clone()))
    }

    pub(crate) fn apply_role(
        &self,
        broker_id: u64,
        broker_role: BrokerRole,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.replace_with(|current| {
            let mut broker = current.broker().clone();
            broker.broker_identity.broker_id = broker_id;
            let mut store = current.store().clone();
            store.broker_role = broker_role;
            current.with_candidates(broker, store)
        })
    }

    pub(crate) fn apply_data_read_ahead(
        &self,
        enabled: bool,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.replace_with(|current| {
            let mut store = current.store().clone();
            store.data_read_ahead_enable = enabled;
            current.with_store_candidate(store)
        })
    }

    fn replace_with(
        &self,
        mut build_candidate: impl FnMut(&ValidatedBrokerConfig) -> Result<ValidatedBrokerConfig, BrokerConfigError>,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        loop {
            let current = self.snapshot();
            let candidate = build_candidate(current.validated().as_ref())?;
            let transaction = ConfigUpdateTransaction::replacement(current.id(), candidate);
            match self.commit(transaction) {
                Ok(generation) => return Ok(generation),
                Err(BrokerConfigError::GenerationConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Barrier;

    use crate::config::broker_config::BrokerConfig;
    use crate::config::error::BrokerConfigError;
    use crate::config::transaction::ConfigUpdateTransaction;
    use crate::config::validated::ConfigGeneration;
    use crate::config::validated::ValidatedBrokerConfig;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::broker::broker_role::BrokerRole;
    use rocketmq_store::MessageStoreConfig;

    use super::BrokerRuntimeConfigState;

    fn runtime_config_state(broker: BrokerConfig, store: MessageStoreConfig) -> BrokerRuntimeConfigState {
        let config =
            ValidatedBrokerConfig::try_from_parts(broker, store).expect("test broker configuration should be valid");
        BrokerRuntimeConfigState::new(Arc::new(config))
    }

    #[test]
    fn independent_updates_preserve_the_other_configuration() {
        let broker = BrokerConfig {
            listen_port: 10912,
            ..BrokerConfig::default()
        };
        let store = MessageStoreConfig {
            ha_listen_port: 10913,
            ..MessageStoreConfig::default()
        };
        let state = runtime_config_state(broker, store);

        let mut next_broker = state.broker_snapshot().as_ref().clone();
        next_broker.listen_port = 20912;
        state
            .replace_broker(next_broker)
            .expect("broker replacement should be valid");
        assert_eq!(state.store_snapshot().ha_listen_port, 10913);

        let mut next_store = state.store_snapshot().as_ref().clone();
        next_store.ha_listen_port = 20913;
        state
            .replace_store(next_store)
            .expect("store replacement should be valid");
        assert_eq!(state.broker_snapshot().listen_port, 20912);
    }

    #[test]
    fn role_generation_preserves_unrelated_fields() {
        let broker = BrokerConfig {
            listen_port: 30912,
            ..BrokerConfig::default()
        };
        let store = MessageStoreConfig {
            ha_listen_port: 30913,
            ..MessageStoreConfig::default()
        };
        let state = runtime_config_state(broker, store);

        let generation = state
            .apply_role(7, BrokerRole::Slave)
            .expect("slave role should produce a valid generation");

        assert_eq!(generation.broker().broker_identity.broker_id, 7);
        assert_eq!(generation.broker().listen_port, 30912);
        assert_eq!(generation.store().broker_role, BrokerRole::Slave);
        assert_eq!(generation.store().ha_listen_port, 30913);
    }

    #[test]
    fn read_ahead_generation_preserves_unrelated_fields() {
        let broker = BrokerConfig {
            listen_port: 30912,
            ..BrokerConfig::default()
        };
        let store = MessageStoreConfig {
            ha_listen_port: 30913,
            ..MessageStoreConfig::default()
        };
        let state = runtime_config_state(broker, store);

        let generation = state
            .apply_data_read_ahead(true)
            .expect("read-ahead update should produce a valid generation");

        assert_eq!(generation.broker().listen_port, 30912);
        assert!(generation.store().data_read_ahead_enable);
        assert_eq!(generation.store().ha_listen_port, 30913);
    }

    #[test]
    fn concurrent_broker_and_store_updates_do_not_lose_either_side() {
        let state = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let barrier = Arc::new(Barrier::new(3));

        let broker_state = state.clone();
        let broker_barrier = Arc::clone(&barrier);
        let broker_thread = std::thread::spawn(move || {
            broker_barrier.wait();
            for listen_port in 20_000..20_100 {
                let mut broker = broker_state.broker_snapshot().as_ref().clone();
                broker.listen_port = listen_port;
                broker_state
                    .replace_broker(broker)
                    .expect("concurrent broker update should remain valid");
            }
        });

        let store_state = state.clone();
        let store_barrier = Arc::clone(&barrier);
        let store_thread = std::thread::spawn(move || {
            store_barrier.wait();
            for ha_listen_port in 30_000..30_100 {
                let mut store = store_state.store_snapshot().as_ref().clone();
                store.ha_listen_port = ha_listen_port;
                store_state
                    .replace_store(store)
                    .expect("concurrent store update should remain valid");
            }
        });

        barrier.wait();
        broker_thread.join().expect("broker updater should finish");
        store_thread.join().expect("store updater should finish");

        let generation = state.snapshot();
        assert_eq!(generation.broker().listen_port, 20_099);
        assert_eq!(generation.store().ha_listen_port, 30_099);
        assert_eq!(generation.id().value(), ConfigGeneration::INITIAL.value() + 200);
    }

    #[test]
    fn stale_transaction_is_rejected_without_replacing_the_committed_generation() {
        let state = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let baseline = state.snapshot();
        let first_patch = HashMap::from([(
            CheetahString::from_static_str("maxClientEventCount"),
            CheetahString::from_static_str("101"),
        )]);
        let stale_patch = HashMap::from([(
            CheetahString::from_static_str("maxClientEventCount"),
            CheetahString::from_static_str("102"),
        )]);
        let first = ConfigUpdateTransaction::from_broker_patch(baseline.id(), baseline.validated(), &first_patch)
            .expect("first patch should validate");
        let stale = ConfigUpdateTransaction::from_broker_patch(baseline.id(), baseline.validated(), &stale_patch)
            .expect("stale patch should validate before publication");

        let committed = state.commit(first).expect("first transaction should commit");
        let error = match state.commit(stale) {
            Ok(_) => panic!("stale transaction must not replace a newer generation"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            BrokerConfigError::GenerationConflict { expected: 1, actual: 2 }
        ));
        assert_eq!(committed.id().value(), 2);
        assert_eq!(state.snapshot().id().value(), 2);
        assert_eq!(state.broker_snapshot().max_client_event_count, 101);
    }

    #[test]
    fn invalid_patch_preserves_generation_and_snapshot() {
        let state = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let baseline = state.snapshot();
        let invalid_patch = HashMap::from([(
            CheetahString::from_static_str("maxClientEventCount"),
            CheetahString::from_static_str("0"),
        )]);

        let error =
            match ConfigUpdateTransaction::from_broker_patch(baseline.id(), baseline.validated(), &invalid_patch) {
                Ok(_) => panic!("invalid patch must not produce a publishable transaction"),
                Err(error) => error,
            };

        assert!(matches!(
            error,
            BrokerConfigError::InvalidProperty {
                key,
                value,
                expected: "a positive integer"
            } if key == "maxClientEventCount" && value == "0"
        ));
        let current = state.snapshot();
        assert!(Arc::ptr_eq(&baseline, &current));
        assert_eq!(current.id(), ConfigGeneration::INITIAL);
    }
}
