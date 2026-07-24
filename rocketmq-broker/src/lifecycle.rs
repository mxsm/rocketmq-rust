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

mod components;

use std::net::SocketAddr;

pub(crate) use components::BrokerComponent;
pub use components::BrokerReadiness;
pub(crate) use components::StartupJournal;

/// Marker for a broker whose configuration has been assembled but whose durable state has not
/// been loaded.
#[derive(Debug, Default)]
pub struct Configured;

/// Marker for a broker whose metadata, Store, security, and request-processing dependencies have
/// been initialized.
#[derive(Debug, Default)]
pub struct Initialized;

/// Marker for a broker that has passed every readiness requirement.
#[derive(Debug)]
pub struct Running {
    readiness: BrokerReadiness,
}

impl Running {
    pub(crate) fn new(readiness: BrokerReadiness) -> Self {
        Self { readiness }
    }

    #[must_use]
    pub fn readiness(&self) -> &BrokerReadiness {
        &self.readiness
    }
}

/// Typed failure for broker initialization, startup, readiness, and rollback.
#[derive(Debug, thiserror::Error)]
pub enum BrokerStartupError {
    #[error("unsupported broker capability `{capability}`: {reason}")]
    UnsupportedCapability {
        capability: &'static str,
        reason: &'static str,
    },
    #[error("failed to load broker metadata component `{component}`")]
    MetadataLoad { component: &'static str },
    #[error("failed to initialize broker component `{component}`: {detail}")]
    Initialization { component: &'static str, detail: String },
    #[error("failed to start broker component `{component}`: {detail}")]
    ComponentStart { component: &'static str, detail: String },
    #[error("remoting listener `{listener}` failed to become ready: {detail}")]
    ListenerStartup { listener: &'static str, detail: String },
    #[error("remoting listener `{listener}` startup acknowledgement was dropped")]
    ListenerStartupDropped { listener: &'static str },
    #[error("broker readiness requirements are incomplete: {missing:?}")]
    Readiness { missing: Vec<&'static str> },
    #[error(
        "broker startup failed and rollback completed with unhealthy components {unhealthy_components:?}: {cause}"
    )]
    RolledBack {
        cause: Box<BrokerStartupError>,
        unhealthy_components: Vec<&'static str>,
    },
}

impl BrokerStartupError {
    pub(crate) fn component_start(component: &'static str, error: impl std::fmt::Display) -> Self {
        Self::ComponentStart {
            component,
            detail: error.to_string(),
        }
    }

    pub(crate) fn listener_startup(
        listener: &'static str,
        result: Result<SocketAddr, rocketmq_error::RocketMQError>,
    ) -> Result<SocketAddr, Self> {
        result.map_err(|error| Self::ListenerStartup {
            listener,
            detail: error.to_string(),
        })
    }
}
