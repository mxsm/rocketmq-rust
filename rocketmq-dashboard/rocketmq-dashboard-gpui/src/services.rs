// Copyright 2025 The RocketMQ Rust Authors
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

//! Narrow service seams used by the first desktop shell.
//!
//! Delivery 01 deliberately has no network, RocketMQ runtime, credential persistence, or provider
//! implementation. These traits make the shell testable without smuggling those capabilities in.

use std::sync::Arc;

use crate::state::UiErrorCode;
use crate::{route::AppRoute, state::UiError};

/// Result of reading the minimum startup configuration and session metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartupSnapshot {
    /// Revision captured when startup began. A newer revision invalidates an older result.
    pub configuration_revision: u64,
    /// Whether the user must authenticate before reaching the main shell.
    pub login_required: bool,
    /// Whether a previously established session is still valid.
    pub has_valid_session: bool,
}

impl StartupSnapshot {
    /// Chooses the first safe route after startup without exposing a session value.
    pub const fn destination(&self) -> AppRoute {
        if self.login_required && !self.has_valid_session {
            AppRoute::Login
        } else {
            AppRoute::Dashboard
        }
    }
}

/// A session presence marker; its value is intentionally never represented in the UI state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionState {
    authenticated: bool,
}

impl SessionState {
    /// Creates an authenticated session marker without retaining a credential.
    pub const fn authenticated() -> Self {
        Self { authenticated: true }
    }

    /// Creates a signed-out session marker.
    pub const fn signed_out() -> Self {
        Self { authenticated: false }
    }

    /// Returns whether navigation may enter the protected shell.
    pub const fn is_authenticated(self) -> bool {
        self.authenticated
    }

    /// Clears the marker while keeping no session material in memory.
    pub fn clear(&mut self) {
        self.authenticated = false;
    }
}

/// Startup reads only local, non-sensitive configuration/session metadata.
pub trait StartupService: Send + Sync {
    /// Returns the state required to choose Login or Dashboard.
    fn bootstrap(&self) -> Result<StartupSnapshot, UiError>;
}

/// Local configuration integration, intentionally represented as explicit user intents.
pub trait ConfigService: Send + Sync {
    /// Opens the configuration location if the host integration is available.
    fn open_config_location(&self) -> Result<(), UiError>;
}

/// Authentication boundary used by the Login and Sign out controls.
pub trait AuthService: Send + Sync {
    /// Authenticates the supplied credentials without retaining them in the shell.
    fn authenticate(&self, username: &str, password: &str) -> Result<SessionState, UiError>;

    /// Invalidates the current session at the eventual backing service.
    fn sign_out(&self) -> Result<(), UiError>;
}

/// The only service aggregation available to the Delivery 01 UI.
#[derive(Clone)]
pub struct AppServices {
    startup: Arc<dyn StartupService>,
    config: Arc<dyn ConfigService>,
    auth: Arc<dyn AuthService>,
}

impl AppServices {
    /// Builds an application service boundary from testable service implementations.
    pub fn new(startup: Arc<dyn StartupService>, config: Arc<dyn ConfigService>, auth: Arc<dyn AuthService>) -> Self {
        Self { startup, config, auth }
    }

    /// Reads the startup state. Implementations must not block the render path.
    pub fn bootstrap(&self) -> Result<StartupSnapshot, UiError> {
        self.startup.bootstrap()
    }

    /// Represents the Open Config user intent without putting file I/O in a view.
    pub fn open_config_location(&self) -> Result<(), UiError> {
        self.config.open_config_location()
    }

    /// Delegates credential verification to an injected implementation.
    pub fn authenticate(&self, username: &str, password: &str) -> Result<SessionState, UiError> {
        self.auth.authenticate(username, password)
    }

    /// Delegates session invalidation to an injected implementation.
    pub fn sign_out(&self) -> Result<(), UiError> {
        self.auth.sign_out()
    }
}

impl Default for AppServices {
    fn default() -> Self {
        Self::new(
            Arc::new(DefaultStartupService),
            Arc::new(CapabilityUnavailableConfigService),
            Arc::new(CapabilityUnavailableAuthService),
        )
    }
}

/// Default startup implementation with no filesystem, network, or persistence capability.
struct DefaultStartupService;

impl StartupService for DefaultStartupService {
    fn bootstrap(&self) -> Result<StartupSnapshot, UiError> {
        Ok(StartupSnapshot {
            configuration_revision: 0,
            login_required: false,
            has_valid_session: false,
        })
    }
}

/// A deterministic fake used by focused startup tests and local shell wiring.
#[cfg(test)]
#[derive(Clone)]
pub struct FakeStartupService {
    result: Result<StartupSnapshot, UiError>,
}

#[cfg(test)]
impl FakeStartupService {
    /// Creates a successful fake startup result.
    pub fn ready(snapshot: StartupSnapshot) -> Self {
        Self { result: Ok(snapshot) }
    }

    /// Creates a failing fake startup result.
    pub fn failed(error: UiError) -> Self {
        Self { result: Err(error) }
    }
}

#[cfg(test)]
impl StartupService for FakeStartupService {
    fn bootstrap(&self) -> Result<StartupSnapshot, UiError> {
        self.result.clone()
    }
}

/// A deterministic auth fake for UI tests. It never stores supplied credentials.
#[cfg(test)]
#[derive(Clone)]
pub struct FakeAuthService {
    result: Result<SessionState, UiError>,
}

#[cfg(test)]
impl FakeAuthService {
    /// Creates a fake successful authentication result.
    pub fn authenticated() -> Self {
        Self {
            result: Ok(SessionState::authenticated()),
        }
    }

    /// Creates a fake authentication failure.
    pub fn failed(error: UiError) -> Self {
        Self { result: Err(error) }
    }
}

#[cfg(test)]
impl AuthService for FakeAuthService {
    fn authenticate(&self, _username: &str, _password: &str) -> Result<SessionState, UiError> {
        self.result.clone()
    }

    fn sign_out(&self) -> Result<(), UiError> {
        Ok(())
    }
}

/// The Delivery 01 configuration implementation intentionally exposes no host operation.
pub struct CapabilityUnavailableConfigService;

impl ConfigService for CapabilityUnavailableConfigService {
    fn open_config_location(&self) -> Result<(), UiError> {
        Err(capability_unavailable("Opening the configuration location"))
    }
}

/// The Delivery 01 authentication implementation intentionally exposes no credential backend.
pub struct CapabilityUnavailableAuthService;

impl AuthService for CapabilityUnavailableAuthService {
    fn authenticate(&self, _username: &str, _password: &str) -> Result<SessionState, UiError> {
        Err(capability_unavailable("Authentication"))
    }

    fn sign_out(&self) -> Result<(), UiError> {
        Err(capability_unavailable("Sign out"))
    }
}

/// Creates a safe explicit signal for capabilities owned by a later delivery.
pub fn capability_unavailable(capability: &str) -> UiError {
    UiError::new(
        format!("{capability} is not available in this delivery."),
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        AppServices, AuthService, FakeAuthService, FakeStartupService, SessionState, StartupService, StartupSnapshot,
    };
    use crate::state::{UiError, UiErrorCode};
    use std::sync::Arc;

    #[test]
    fn startup_destination_never_needs_a_session_value() {
        assert_eq!(
            StartupSnapshot {
                configuration_revision: 8,
                login_required: true,
                has_valid_session: false,
            }
            .destination()
            .format_path(),
            "/login"
        );
        assert_eq!(
            StartupSnapshot {
                configuration_revision: 8,
                login_required: false,
                has_valid_session: false,
            }
            .destination()
            .format_path(),
            "/dashboard"
        );
    }

    #[test]
    fn fake_services_are_injectable_without_a_provider_runtime() {
        let services = AppServices::new(
            Arc::new(FakeStartupService::ready(StartupSnapshot {
                configuration_revision: 1,
                login_required: false,
                has_valid_session: false,
            })),
            Arc::new(super::CapabilityUnavailableConfigService),
            Arc::new(FakeAuthService::authenticated()),
        );

        assert_eq!(
            services
                .bootstrap()
                .map(|snapshot| snapshot.destination().format_path()),
            Ok("/dashboard".to_owned())
        );
        assert_eq!(
            services.authenticate("operator", "not-retained"),
            Ok(SessionState::authenticated())
        );
        assert_eq!(
            services.open_config_location().map_err(|error| error.code()),
            Err(UiErrorCode::CapabilityUnavailable)
        );
    }

    #[test]
    fn startup_failure_is_safe_to_clone_between_task_attempts() {
        let failure = UiError::new("Configuration is unavailable.", UiErrorCode::Configuration, true);
        let fake = FakeStartupService::failed(failure);

        assert!(fake.bootstrap().is_err());
        assert!(fake.bootstrap().is_err());
    }

    #[test]
    fn fake_auth_failure_never_requires_a_real_credential_backend() {
        let auth = FakeAuthService::failed(UiError::new(
            "Authentication was rejected.",
            UiErrorCode::Authentication,
            false,
        ));

        assert!(auth.authenticate("operator", "secret").is_err());
    }
}
