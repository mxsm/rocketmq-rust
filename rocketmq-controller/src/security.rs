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

//! Implementation-neutral security ports injected into the Controller composition boundary.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::MaintenanceAuthorizer;

/// Future returned by a Controller maintenance authenticator.
pub type MaintenanceAuthenticationFuture<'a> = Pin<Box<dyn Future<Output = RocketMQResult<String>> + Send + 'a>>;

/// Future returned while stopping an injected Controller authenticator.
pub type MaintenanceAuthenticationShutdownFuture<'a> = Pin<Box<dyn Future<Output = RocketMQResult<()>> + Send + 'a>>;

/// Verifies one privileged Controller request without exposing an auth implementation.
///
/// The protocol-to-auth adapter is owned by the service composition root. The
/// Controller only consumes the authenticated principal needed by its independent
/// maintenance policy.
pub trait MaintenancePrincipalAuthenticator: Send + Sync {
    /// Verifies the signed request and returns its canonical principal.
    ///
    /// # Errors
    ///
    /// Returns an authentication error when credentials are absent, malformed,
    /// disabled, expired, or rejected by the injected implementation.
    fn authenticate_maintenance_principal<'a>(
        &'a self,
        request: &'a RemotingCommand,
        channel_id: Option<&'a str>,
    ) -> MaintenanceAuthenticationFuture<'a>;

    /// Stops all tasks and resources owned by the injected authenticator.
    ///
    /// # Errors
    ///
    /// Returns an error when the adapter cannot complete a clean shutdown.
    fn shutdown(&self) -> MaintenanceAuthenticationShutdownFuture<'_>;
}

/// Security capabilities supplied by a Controller composition root.
#[derive(Clone)]
pub struct ControllerSecurity {
    authenticator: Arc<dyn MaintenancePrincipalAuthenticator>,
    maintenance_authorizer: Option<Arc<MaintenanceAuthorizer>>,
}

impl ControllerSecurity {
    /// Creates an injected Controller security boundary.
    pub fn new(
        authenticator: Arc<dyn MaintenancePrincipalAuthenticator>,
        maintenance_authorizer: Option<Arc<MaintenanceAuthorizer>>,
    ) -> Self {
        Self {
            authenticator,
            maintenance_authorizer,
        }
    }

    /// Returns the principal authenticator.
    pub fn authenticator(&self) -> &Arc<dyn MaintenancePrincipalAuthenticator> {
        &self.authenticator
    }

    /// Returns the independently validated maintenance authorizer, when configured.
    pub fn maintenance_authorizer(&self) -> Option<&Arc<MaintenanceAuthorizer>> {
        self.maintenance_authorizer.as_ref()
    }
}
