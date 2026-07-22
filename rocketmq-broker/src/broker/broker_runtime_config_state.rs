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
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_store::config::message_store_config::MessageStoreConfig;

/// An atomically published broker and message-store configuration generation.
///
/// Keeping both configurations in one generation prevents an admin update from
/// overwriting a controller role transition (or vice versa) with a stale copy of
/// the other configuration.
pub(crate) struct BrokerRuntimeConfigGeneration {
    broker: Arc<BrokerConfig>,
    store: Arc<MessageStoreConfig>,
}

impl BrokerRuntimeConfigGeneration {
    pub(crate) fn broker(&self) -> &Arc<BrokerConfig> {
        &self.broker
    }

    pub(crate) fn store(&self) -> &Arc<MessageStoreConfig> {
        &self.store
    }
}

#[derive(Clone)]
pub(crate) struct BrokerRuntimeConfigState {
    current: Arc<ArcSwap<BrokerRuntimeConfigGeneration>>,
}

impl BrokerRuntimeConfigState {
    pub(crate) fn new(broker: Arc<BrokerConfig>, store: Arc<MessageStoreConfig>) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(BrokerRuntimeConfigGeneration { broker, store })),
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

    pub(crate) fn update_broker(&self, broker: BrokerConfig) -> Arc<BrokerRuntimeConfigGeneration> {
        let broker = Arc::new(broker);
        self.current.rcu(|current| {
            Arc::new(BrokerRuntimeConfigGeneration {
                broker: Arc::clone(&broker),
                store: Arc::clone(current.store()),
            })
        });
        self.snapshot()
    }

    pub(crate) fn update_store(&self, store: MessageStoreConfig) -> Arc<BrokerRuntimeConfigGeneration> {
        let store = Arc::new(store);
        self.current.rcu(|current| {
            Arc::new(BrokerRuntimeConfigGeneration {
                broker: Arc::clone(current.broker()),
                store: Arc::clone(&store),
            })
        });
        self.snapshot()
    }

    pub(crate) fn apply_role(&self, broker_id: u64, broker_role: BrokerRole) -> Arc<BrokerRuntimeConfigGeneration> {
        self.current.rcu(|current| {
            let mut broker = current.broker().as_ref().clone();
            broker.broker_identity.broker_id = broker_id;
            let mut store = current.store().as_ref().clone();
            store.broker_role = broker_role;
            Arc::new(BrokerRuntimeConfigGeneration {
                broker: Arc::new(broker),
                store: Arc::new(store),
            })
        });
        self.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::broker::broker_role::BrokerRole;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::BrokerRuntimeConfigState;

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
        let state = BrokerRuntimeConfigState::new(Arc::new(broker), Arc::new(store));

        let mut next_broker = state.broker_snapshot().as_ref().clone();
        next_broker.listen_port = 20912;
        state.update_broker(next_broker);
        assert_eq!(state.store_snapshot().ha_listen_port, 10913);

        let mut next_store = state.store_snapshot().as_ref().clone();
        next_store.ha_listen_port = 20913;
        state.update_store(next_store);
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
        let state = BrokerRuntimeConfigState::new(Arc::new(broker), Arc::new(store));

        let generation = state.apply_role(7, BrokerRole::Slave);

        assert_eq!(generation.broker().broker_identity.broker_id, 7);
        assert_eq!(generation.broker().listen_port, 30912);
        assert_eq!(generation.store().broker_role, BrokerRole::Slave);
        assert_eq!(generation.store().ha_listen_port, 30913);
    }

    #[test]
    fn concurrent_broker_and_store_updates_do_not_lose_either_side() {
        let state = BrokerRuntimeConfigState::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        );
        let barrier = Arc::new(Barrier::new(3));

        let broker_state = state.clone();
        let broker_barrier = Arc::clone(&barrier);
        let broker_thread = std::thread::spawn(move || {
            broker_barrier.wait();
            for listen_port in 20_000..20_100 {
                let mut broker = broker_state.broker_snapshot().as_ref().clone();
                broker.listen_port = listen_port;
                broker_state.update_broker(broker);
            }
        });

        let store_state = state.clone();
        let store_barrier = Arc::clone(&barrier);
        let store_thread = std::thread::spawn(move || {
            store_barrier.wait();
            for ha_listen_port in 30_000..30_100 {
                let mut store = store_state.store_snapshot().as_ref().clone();
                store.ha_listen_port = ha_listen_port;
                store_state.update_store(store);
            }
        });

        barrier.wait();
        broker_thread.join().expect("broker updater should finish");
        store_thread.join().expect("store updater should finish");

        let generation = state.snapshot();
        assert_eq!(generation.broker().listen_port, 20_099);
        assert_eq!(generation.store().ha_listen_port, 30_099);
    }
}
