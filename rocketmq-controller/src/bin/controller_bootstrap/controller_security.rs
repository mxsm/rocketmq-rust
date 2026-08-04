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

//! Production authentication composition for the Controller binary.

use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use rocketmq_auth::AuthConfig;
use rocketmq_auth::AuthRuntime;
use rocketmq_auth::AuthRuntimeBuilder;
use rocketmq_controller::ControllerConfig;
use rocketmq_controller::ControllerSecurity;
use rocketmq_controller::MaintenanceAuthenticationFuture;
use rocketmq_controller::MaintenanceAuthenticationShutdownFuture;
use rocketmq_controller::MaintenancePrincipalAuthenticator;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_security_api::MaintenanceAuthorizer;

#[derive(Clone)]
struct ControllerAuthRuntimeAdapter {
    runtime: Arc<AuthRuntime>,
}

impl MaintenancePrincipalAuthenticator for ControllerAuthRuntimeAdapter {
    fn authenticate_maintenance_principal<'a>(
        &'a self,
        request: &'a RemotingCommand,
        channel_id: Option<&'a str>,
    ) -> MaintenanceAuthenticationFuture<'a> {
        Box::pin(async move {
            self.runtime
                .authenticate_maintenance_principal(request, channel_id)
                .await
                .map(String::from)
        })
    }

    fn shutdown(&self) -> MaintenanceAuthenticationShutdownFuture<'_> {
        Box::pin(self.runtime.shutdown())
    }
}

pub(crate) async fn build_controller_security(
    config: &ControllerConfig,
    service_context: &ChildServiceContext,
) -> Result<Option<ControllerSecurity>> {
    if !security_enabled(config) {
        return Ok(None);
    }

    let auth_config = build_auth_config(config);
    let maintenance_authorizer = auth_config
        .maintenance_policy_reference()
        .context("failed to validate Controller maintenance policy reference")?
        .map(|reference| {
            reference
                .load_from(auth_config.auth_config_path.as_str())
                .map(MaintenanceAuthorizer::new)
                .map(Arc::new)
        })
        .transpose()
        .context("failed to load the pinned Controller maintenance policy")?;
    let auth_runtime = AuthRuntimeBuilder::new(auth_config, service_context.component("controller.auth"))
        .build()
        .await
        .context("failed to initialize Controller authentication runtime")?;
    let authenticator = Arc::new(ControllerAuthRuntimeAdapter {
        runtime: Arc::new(auth_runtime),
    });

    Ok(Some(ControllerSecurity::new(authenticator, maintenance_authorizer)))
}

fn security_enabled(config: &ControllerConfig) -> bool {
    config.authentication_enabled || config.authorization_enabled || config.maintenance_enabled
}

fn build_auth_config(config: &ControllerConfig) -> AuthConfig {
    AuthConfig {
        config_name: format!("controller-{}", config.node_id).into(),
        cluster_name: "controller".into(),
        auth_config_path: config.auth_config_path.clone().into(),
        acl_file: config.acl_file.clone().into(),
        authentication_enabled: config.authentication_enabled,
        authorization_enabled: config.authorization_enabled,
        maintenance_enabled: config.maintenance_enabled,
        maintenance_policy_path: config.maintenance_policy_path.clone().into(),
        maintenance_policy_version: config.maintenance_policy_version,
        maintenance_policy_sha256: config.maintenance_policy_sha256.clone().into(),
        ..AuthConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_controller_security_configuration_without_credentials() {
        let mut config = ControllerConfig::default();
        config.node_id = 7;
        config.authentication_enabled = true;
        config.authorization_enabled = true;
        config.auth_config_path = "/var/lib/rocketmq/controller/auth/node-7".to_string();
        config.acl_file = "/var/run/secrets/rocketmq/controller-acl.yml".to_string();
        config.maintenance_enabled = true;
        config.maintenance_policy_path = "/etc/rocketmq/auth/maintenance-policy.json".to_string();
        config.maintenance_policy_version = 3;
        config.maintenance_policy_sha256 = "a".repeat(64);

        let mapped = build_auth_config(&config);

        assert_eq!(mapped.config_name.as_str(), "controller-7");
        assert_eq!(mapped.cluster_name.as_str(), "controller");
        assert_eq!(mapped.auth_config_path.as_str(), config.auth_config_path);
        assert_eq!(mapped.acl_file.as_str(), config.acl_file);
        assert!(mapped.authentication_enabled);
        assert!(mapped.authorization_enabled);
        assert!(mapped.maintenance_enabled);
        assert!(mapped.init_authentication_user.is_empty());
        assert!(mapped.inner_client_authentication_credentials.is_empty());
    }

    #[test]
    fn default_controller_does_not_build_security() {
        assert!(!security_enabled(&ControllerConfig::default()));
    }
}
