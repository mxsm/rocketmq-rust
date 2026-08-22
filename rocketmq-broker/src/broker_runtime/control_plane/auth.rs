// Copyright 2026 The RocketMQ Rust Authors
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

//! Authentication and ACL initialization owned by the Broker control plane.

use super::super::*;

impl BrokerRuntime {
    pub(in crate::broker_runtime) async fn initial_acl(&mut self) -> bool {
        let broker_config = self.composition.state.broker_config();
        let auth_config = build_auth_config(&broker_config);
        let maintenance_authorizer = match auth_config.maintenance_policy_reference() {
            Ok(Some(reference)) => match reference.load_from(auth_config.auth_config_path.as_str()) {
                Ok(policy) => Some(Arc::new(MaintenanceAuthorizer::new(policy))),
                Err(error) => {
                    error!(%error, "Initialize maintenance authorization failed");
                    return false;
                }
            },
            Ok(None) => None,
            Err(error) => {
                error!(%error, "Validate maintenance authorization reference failed");
                return false;
            }
        };
        self.composition.request_pipeline.maintenance_authorizer = maintenance_authorizer;
        if !broker_config.authentication_enabled && !broker_config.authorization_enabled {
            self.composition.request_pipeline.auth_runtime = None;
            let Some(service_context) = self.composition.state.service_context.as_ref() else {
                error!("Initialize auth admin service failed because ChildServiceContext is unavailable");
                return false;
            };
            return match AuthAdminService::new(auth_config, service_context.component("broker.auth-admin")).await {
                Ok(service) => {
                    self.composition.request_pipeline.auth_admin_service = Some(Arc::new(service));
                    true
                }
                Err(error) => {
                    error!("Initialize auth admin service failed: {error}");
                    false
                }
            };
        }

        let auth_context = match self.composition.state.service_context.as_ref() {
            Some(service_context) => service_context.component("broker.auth"),
            None => {
                error!("Initialize auth runtime failed because ChildServiceContext is unavailable");
                return false;
            }
        };
        let auth_runtime_builder = match self.composition.state.metadata_io.as_ref() {
            Some(Ok(metadata_io)) => {
                AuthRuntimeBuilder::new(auth_config, auth_context).with_metadata_io_actor(metadata_io.clone())
            }
            Some(Err(error)) => {
                error!(%error, "Initialize auth runtime failed because metadata I/O actor is unavailable");
                return false;
            }
            None => AuthRuntimeBuilder::new(auth_config, auth_context),
        };
        match auth_runtime_builder.build().await {
            Ok(auth_runtime) => {
                let auth_runtime = Arc::new(auth_runtime);
                if let Some(metrics_manager) = self.composition.state.broker_metrics_manager.as_ref() {
                    let auth_runtime_for_metrics = auth_runtime.clone();
                    metrics_manager
                        .register_auth_observable_gauge(move || Some(auth_runtime_for_metrics.metrics_snapshot()));
                }
                self.composition.request_pipeline.auth_admin_service =
                    Some(Arc::new(AuthAdminService::with_provider_registry_and_config(
                        auth_runtime.provider_registry().clone(),
                        auth_runtime.config().clone(),
                    )));
                self.composition.request_pipeline.auth_runtime = Some(auth_runtime);
                true
            }
            Err(error) => {
                error!("Initialize auth runtime failed: {error}");
                false
            }
        }
    }

    pub(in crate::broker_runtime) fn initial_rpc_hooks(&mut self) -> bool {
        let auth_config = build_auth_config(&self.composition.state.broker_config());
        match AclClientRpcHook::from_auth_config(&auth_config) {
            Ok(Some(rpc_hook)) => {
                self.composition
                    .state
                    .broker_outer_api
                    .register_rpc_hook(rpc_hook.into_rpc_hook());
                true
            }
            Ok(None) => true,
            Err(error) => {
                error!("Initialize broker ACL RPC hook failed: {error}");
                false
            }
        }
    }
}
