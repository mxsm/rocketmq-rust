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

use rocketmq_observability::LoggingOverrides;
use rocketmq_observability::ObservabilityOverrides;
use rocketmq_store::MessageStoreConfig;

use super::broker_config::BrokerConfig;
use super::error::BrokerConfigError;
use super::raw::normalize_config_parts;
use super::raw::RawBrokerConfig;
use super::sections::ValidatedConfigSections;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConfigGeneration(u64);

impl ConfigGeneration {
    pub const INITIAL: Self = Self(1);

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

/// Fully normalized and validated configuration consumed by the broker runtime.
///
/// This type deliberately does not implement `Deserialize`. Configuration
/// files must cross the validation boundary through [`RawBrokerConfig`].
#[derive(Clone, Debug)]
pub struct ValidatedBrokerConfig {
    broker: Arc<BrokerConfig>,
    store: Arc<MessageStoreConfig>,
    logging: LoggingOverrides,
    observability: ObservabilityOverrides,
    sections: ValidatedConfigSections,
}

impl ValidatedBrokerConfig {
    pub fn try_from_parts(broker: BrokerConfig, store: MessageStoreConfig) -> Result<Self, BrokerConfigError> {
        Self::try_from(RawBrokerConfig::from_parts(broker, store))
    }

    fn normalize_and_validate(
        mut broker: BrokerConfig,
        mut store: MessageStoreConfig,
        logging: LoggingOverrides,
        observability: ObservabilityOverrides,
    ) -> Result<Self, BrokerConfigError> {
        normalize_config_parts(&mut broker, &mut store);
        let sections = ValidatedConfigSections::validate(&broker, &store)?;
        Ok(Self {
            broker: Arc::new(broker),
            store: Arc::new(store),
            logging,
            observability,
            sections,
        })
    }

    pub(crate) fn with_broker_candidate(&self, broker: BrokerConfig) -> Result<Self, BrokerConfigError> {
        Self::normalize_and_validate(
            broker,
            self.store.as_ref().clone(),
            self.logging.clone(),
            self.observability.clone(),
        )
    }

    pub(crate) fn with_store_candidate(&self, store: MessageStoreConfig) -> Result<Self, BrokerConfigError> {
        Self::normalize_and_validate(
            self.broker.as_ref().clone(),
            store,
            self.logging.clone(),
            self.observability.clone(),
        )
    }

    pub(crate) fn with_candidates(
        &self,
        broker: BrokerConfig,
        store: MessageStoreConfig,
    ) -> Result<Self, BrokerConfigError> {
        Self::normalize_and_validate(broker, store, self.logging.clone(), self.observability.clone())
    }

    #[must_use]
    pub fn broker(&self) -> &BrokerConfig {
        self.broker.as_ref()
    }

    #[must_use]
    pub fn broker_arc(&self) -> Arc<BrokerConfig> {
        Arc::clone(&self.broker)
    }

    pub(crate) fn broker_arc_ref(&self) -> &Arc<BrokerConfig> {
        &self.broker
    }

    #[must_use]
    pub fn store(&self) -> &MessageStoreConfig {
        self.store.as_ref()
    }

    #[must_use]
    pub fn store_arc(&self) -> Arc<MessageStoreConfig> {
        Arc::clone(&self.store)
    }

    pub(crate) fn store_arc_ref(&self) -> &Arc<MessageStoreConfig> {
        &self.store
    }

    #[must_use]
    pub fn logging(&self) -> &LoggingOverrides {
        &self.logging
    }

    #[must_use]
    pub fn observability(&self) -> &ObservabilityOverrides {
        &self.observability
    }

    #[must_use]
    pub fn sections(&self) -> &ValidatedConfigSections {
        &self.sections
    }
}

impl TryFrom<RawBrokerConfig> for ValidatedBrokerConfig {
    type Error = BrokerConfigError;

    fn try_from(raw: RawBrokerConfig) -> Result<Self, Self::Error> {
        let (broker, store, logging, observability) = raw.into_normalized_parts();
        Self::normalize_and_validate(broker, store, logging, observability)
    }
}

impl Default for ValidatedBrokerConfig {
    fn default() -> Self {
        Self::try_from(RawBrokerConfig::default())
            .unwrap_or_else(|error| panic!("default broker configuration must remain valid: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigGeneration;

    #[test]
    fn generation_increment_never_wraps_or_reuses_the_latest_id() {
        assert_eq!(
            ConfigGeneration::INITIAL
                .checked_next()
                .expect("initial generation should advance")
                .value(),
            2
        );
        assert_eq!(ConfigGeneration(u64::MAX).checked_next(), None);
    }
}
