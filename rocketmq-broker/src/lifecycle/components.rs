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

use std::net::SocketAddr;

use super::BrokerStartupError;

/// Components whose startup order is relevant to rollback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrokerComponent {
    Metadata,
    MessageStore,
    Security,
    BrokerOuterApi,
    RequestProcessors,
    NormalListener,
    FastListener,
    BackgroundServices,
    Registration,
}

impl BrokerComponent {
    #[must_use]
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::MessageStore => "message_store",
            Self::Security => "security",
            Self::BrokerOuterApi => "broker_outer_api",
            Self::RequestProcessors => "request_processors",
            Self::NormalListener => "normal_listener",
            Self::FastListener => "fast_listener",
            Self::BackgroundServices => "background_services",
            Self::Registration => "registration",
        }
    }
}

/// Append-only startup record. Rollback always consumes it in reverse completion order.
#[derive(Debug, Default)]
pub(crate) struct StartupJournal {
    completed: Vec<BrokerComponent>,
}

impl StartupJournal {
    pub(crate) fn complete(&mut self, component: BrokerComponent) {
        if self.completed.last().copied() != Some(component) {
            self.completed.push(component);
        }
    }

    #[must_use]
    pub(crate) fn rollback_order(&self) -> Vec<BrokerComponent> {
        self.completed.iter().rev().copied().collect()
    }
}

/// Evidence required before the process lifecycle may publish readiness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerReadiness {
    store_writable: bool,
    normal_listener: Option<SocketAddr>,
    fast_listener: Option<SocketAddr>,
    processors_started: bool,
    security_ready: bool,
    registration_ready: bool,
}

impl BrokerReadiness {
    pub(crate) fn new(
        store_writable: bool,
        normal_listener: SocketAddr,
        fast_listener: SocketAddr,
        processors_started: bool,
        security_ready: bool,
        registration_ready: bool,
    ) -> Self {
        Self {
            store_writable,
            normal_listener: Some(normal_listener),
            fast_listener: Some(fast_listener),
            processors_started,
            security_ready,
            registration_ready,
        }
    }

    pub(crate) fn validate(self) -> Result<Self, BrokerStartupError> {
        let mut missing = Vec::new();
        if !self.store_writable {
            missing.push("message_store_writable");
        }
        if self.normal_listener.is_none() {
            missing.push("normal_listener_bound");
        }
        if self.fast_listener.is_none() {
            missing.push("fast_listener_bound");
        }
        if !self.processors_started {
            missing.push("request_processors_started");
        }
        if !self.security_ready {
            missing.push("security_ready");
        }
        if !self.registration_ready {
            missing.push("registration_ready");
        }
        if missing.is_empty() {
            Ok(self)
        } else {
            Err(BrokerStartupError::Readiness { missing })
        }
    }

    #[must_use]
    pub fn store_writable(&self) -> bool {
        self.store_writable
    }

    #[must_use]
    pub fn normal_listener(&self) -> Option<SocketAddr> {
        self.normal_listener
    }

    #[must_use]
    pub fn fast_listener(&self) -> Option<SocketAddr> {
        self.fast_listener
    }

    #[must_use]
    pub fn processors_started(&self) -> bool {
        self.processors_started
    }

    #[must_use]
    pub fn security_ready(&self) -> bool {
        self.security_ready
    }

    #[must_use]
    pub fn registration_ready(&self) -> bool {
        self.registration_ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_journal_rolls_back_in_reverse_completion_order() {
        let mut journal = StartupJournal::default();
        journal.complete(BrokerComponent::MessageStore);
        journal.complete(BrokerComponent::NormalListener);
        journal.complete(BrokerComponent::FastListener);

        assert_eq!(
            journal.rollback_order(),
            vec![
                BrokerComponent::FastListener,
                BrokerComponent::NormalListener,
                BrokerComponent::MessageStore
            ]
        );
    }

    #[test]
    fn readiness_reports_every_missing_requirement() {
        let readiness = BrokerReadiness {
            store_writable: false,
            normal_listener: None,
            fast_listener: None,
            processors_started: false,
            security_ready: false,
            registration_ready: false,
        };

        let error = readiness.validate().expect_err("incomplete readiness must fail closed");
        let BrokerStartupError::Readiness { missing } = error else {
            panic!("unexpected readiness error: {error}");
        };
        assert_eq!(
            missing,
            vec![
                "message_store_writable",
                "normal_listener_bound",
                "fast_listener_bound",
                "request_processors_started",
                "security_ready",
                "registration_ready"
            ]
        );
    }
}
