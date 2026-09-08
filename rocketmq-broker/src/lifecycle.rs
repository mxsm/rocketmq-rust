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

use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::fmt;
use std::net::SocketAddr;
use std::panic::Location;

use rocketmq_error::DiagnosticView;
use rocketmq_error::Error as CanonicalError;
use rocketmq_error::ErrorCode;
use rocketmq_error::ErrorContext;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::PublicErrorView;
use rocketmq_error::Sensitive;
use rocketmq_error::ViewContextViolation;

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

/// Closed phase attached to a broker startup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerStartupPhase {
    /// Capability validation before initialization.
    CapabilityValidation,
    /// Durable metadata loading.
    MetadataLoad,
    /// Component initialization.
    Initialization,
    /// Component startup.
    ComponentStart,
    /// Listener startup.
    ListenerStartup,
    /// Listener readiness acknowledgement.
    ListenerReadiness,
    /// Aggregate broker readiness validation.
    Readiness,
    /// Startup rollback.
    Rollback,
}

impl BrokerStartupPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CapabilityValidation => "broker_capability_validation",
            Self::MetadataLoad => "broker_metadata_load",
            Self::Initialization => "broker_initialization",
            Self::ComponentStart => "broker_component_start",
            Self::ListenerStartup => "broker_listener_startup",
            Self::ListenerReadiness => "broker_listener_readiness",
            Self::Readiness => "broker_readiness",
            Self::Rollback => "broker_startup_rollback",
        }
    }
}

/// Opaque, catalog-backed broker initialization and startup failure.
///
/// Dynamic detail is retained only as sensitive local state, and typed causes
/// remain available through [`StdError::source`]. Public formatting and
/// boundary projection are owned by the canonical descriptor.
pub struct BrokerStartupError {
    error: CanonicalError,
    phase: BrokerStartupPhase,
    component: Option<&'static str>,
    private_detail: Option<Sensitive<String>>,
    missing: Vec<&'static str>,
    unhealthy_components: Vec<&'static str>,
}

impl BrokerStartupError {
    pub(crate) fn unsupported_capability(capability: &'static str, reason: &'static str) -> Self {
        let context = ErrorContext::new()
            .with_text(rocketmq_error::fields::KEY, "broker.capability")
            .with_secret_presence(rocketmq_error::fields::VALUE_PRESENT)
            .with_secret_presence(rocketmq_error::fields::REASON_PRESENT);
        Self {
            error: CanonicalError::new(&rocketmq_error::CORE_CONFIGURATION_INVALID).with_context(context),
            phase: BrokerStartupPhase::CapabilityValidation,
            component: Some(capability),
            private_detail: Some(Sensitive::new(reason.to_owned())),
            missing: Vec::new(),
            unhealthy_components: Vec::new(),
        }
    }

    pub(crate) fn metadata_load(component: &'static str) -> Self {
        Self::without_source(BrokerStartupPhase::MetadataLoad, Some(component), None)
    }

    pub(crate) fn initialization(component: &'static str, detail: impl Into<String>) -> Self {
        Self::without_source(BrokerStartupPhase::Initialization, Some(component), Some(detail.into()))
    }

    pub(crate) fn initialization_source(
        component: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(BrokerStartupPhase::Initialization, Some(component), source)
    }

    pub(crate) fn component_start(component: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::with_source(BrokerStartupPhase::ComponentStart, Some(component), source)
    }

    pub(crate) fn component_start_detail(component: &'static str, detail: impl Into<String>) -> Self {
        Self::without_source(BrokerStartupPhase::ComponentStart, Some(component), Some(detail.into()))
    }

    pub(crate) fn listener_startup<E>(listener: &'static str, result: Result<SocketAddr, E>) -> Result<SocketAddr, Self>
    where
        E: StdError + Send + Sync + 'static,
    {
        result.map_err(|error| Self::with_source(BrokerStartupPhase::ListenerStartup, Some(listener), error))
    }

    pub(crate) fn listener_startup_source(
        listener: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(BrokerStartupPhase::ListenerStartup, Some(listener), source)
    }

    pub(crate) fn listener_readiness_source(
        listener: &'static str,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(BrokerStartupPhase::ListenerReadiness, Some(listener), source)
    }

    pub(crate) fn listener_startup_dropped(listener: &'static str) -> Self {
        Self::without_source(BrokerStartupPhase::ListenerReadiness, Some(listener), None)
    }

    pub(crate) fn readiness(missing: Vec<&'static str>) -> Self {
        let mut error = Self::without_source(BrokerStartupPhase::Readiness, None, None);
        error.missing = missing;
        error
    }

    pub(crate) fn rolled_back(cause: Self, unhealthy_components: Vec<&'static str>) -> Self {
        let mut error = Self::with_source(BrokerStartupPhase::Rollback, None, cause);
        error.unhealthy_components = unhealthy_components;
        error
    }

    fn without_source(phase: BrokerStartupPhase, component: Option<&'static str>, detail: Option<String>) -> Self {
        let descriptor = descriptor_for_phase(phase);
        Self {
            error: CanonicalError::new(descriptor).with_context(context_for_phase(phase, false)),
            phase,
            component,
            private_detail: detail.map(Sensitive::new),
            missing: Vec::new(),
            unhealthy_components: Vec::new(),
        }
    }

    fn with_source(
        phase: BrokerStartupPhase,
        component: Option<&'static str>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let descriptor = descriptor_for_phase(phase);
        Self {
            error: CanonicalError::caused_by(descriptor, source).with_context(context_for_phase(phase, true)),
            phase,
            component,
            private_detail: None,
            missing: Vec::new(),
            unhealthy_components: Vec::new(),
        }
    }

    /// Returns the stable catalog descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &'static ErrorDescriptor {
        self.error.descriptor()
    }

    /// Returns the stable dotted catalog code.
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.error.code()
    }

    /// Returns the closed startup phase.
    #[must_use]
    pub const fn phase(&self) -> BrokerStartupPhase {
        self.phase
    }

    /// Returns the affected component, capability, or listener when present.
    #[must_use]
    pub const fn component(&self) -> Option<&'static str> {
        self.component
    }

    /// Returns incomplete readiness requirements.
    #[must_use]
    pub fn missing_requirements(&self) -> &[&'static str] {
        &self.missing
    }

    /// Returns components left unhealthy after rollback.
    #[must_use]
    pub fn unhealthy_components(&self) -> &[&'static str] {
        &self.unhealthy_components
    }

    /// Returns the typed startup failure that triggered rollback, when this is a rollback error.
    #[must_use]
    pub fn rollback_cause(&self) -> Option<&Self> {
        (self.phase == BrokerStartupPhase::Rollback)
            .then(|| self.error.source()?.downcast_ref())
            .flatten()
    }

    /// Returns the first canonical promotion location.
    #[must_use]
    pub const fn location(&self) -> &'static Location<'static> {
        self.error.location()
    }

    /// Returns a catalog-controlled backtrace when one was captured.
    #[must_use]
    pub const fn backtrace(&self) -> Option<&Backtrace> {
        self.error.backtrace()
    }

    /// Builds the descriptor-validated public view.
    ///
    /// # Errors
    ///
    /// Returns [`ViewContextViolation`] if internal context does not satisfy the descriptor schema.
    pub fn public_view(&self) -> Result<PublicErrorView<'_>, ViewContextViolation> {
        self.error.public_view()
    }

    /// Builds the descriptor-validated diagnostic view.
    ///
    /// # Errors
    ///
    /// Returns [`ViewContextViolation`] if internal context does not satisfy the descriptor schema.
    pub fn diagnostic_view(&self) -> Result<DiagnosticView<'_>, ViewContextViolation> {
        self.error.diagnostic_view()
    }
}

fn descriptor_for_phase(phase: BrokerStartupPhase) -> &'static ErrorDescriptor {
    match phase {
        BrokerStartupPhase::CapabilityValidation => &rocketmq_error::CORE_CONFIGURATION_INVALID,
        BrokerStartupPhase::ListenerStartup | BrokerStartupPhase::ListenerReadiness => {
            &rocketmq_error::TRANSPORT_START_FAILED
        }
        BrokerStartupPhase::MetadataLoad
        | BrokerStartupPhase::Initialization
        | BrokerStartupPhase::ComponentStart
        | BrokerStartupPhase::Readiness
        | BrokerStartupPhase::Rollback => &rocketmq_error::CORE_SERVICE_FAILED,
    }
}

fn context_for_phase(phase: BrokerStartupPhase, source_present: bool) -> ErrorContext {
    match phase {
        BrokerStartupPhase::CapabilityValidation => ErrorContext::new()
            .with_text(rocketmq_error::fields::KEY, "broker.capability")
            .with_secret_presence(rocketmq_error::fields::VALUE_PRESENT)
            .with_secret_presence(rocketmq_error::fields::REASON_PRESENT),
        BrokerStartupPhase::ListenerStartup | BrokerStartupPhase::ListenerReadiness => {
            let context = ErrorContext::new().with_text(rocketmq_error::fields::OPERATION_DIAGNOSTIC, phase.as_str());
            if source_present {
                context.with_secret_presence(rocketmq_error::fields::SOURCE_PRESENT)
            } else {
                context
            }
        }
        _ => ErrorContext::new().with_text(rocketmq_error::fields::OPERATION_DIAGNOSTIC, phase.as_str()),
    }
}

impl fmt::Display for BrokerStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, formatter)
    }
}

impl fmt::Debug for BrokerStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerStartupError")
            .field("code", &self.code())
            .field("message", &self.descriptor().public_message())
            .field("phase", &self.phase)
            .field("component", &self.component)
            .field("detail_present", &self.private_detail.is_some())
            .field("missing_count", &self.missing.len())
            .field("unhealthy_component_count", &self.unhealthy_components.len())
            .field("source_present", &self.error.source().is_some())
            .finish()
    }
}

impl StdError for BrokerStartupError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.error.source()
    }
}
