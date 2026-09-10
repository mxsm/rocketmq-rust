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

//! NameServer operations - Core business logic

use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_client_rust::{BrokerAdmin as _, RouteAdmin as _};

use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_error::Result as CanonicalResult;

use super::types::KvConfigDeleteRequest;
use super::types::KvConfigUpdateRequest;
use super::types::KvConfigUpdateResult;
use super::types::NamesrvConfigQueryRequest;
use super::types::NamesrvConfigQueryResult;
use super::types::NamesrvConfigUpdateRequest;
use super::types::NamesrvConfigUpdateResult;
use super::types::WritePermRequest;
use super::types::WritePermResult;
use super::types::WritePermResultEntry;
use crate::client_adapter::services::admin::AdminBuilder;

/// NameServer operations service
pub struct NameServerService;

impl NameServerService {
    pub async fn query_namesrv_config(request: NamesrvConfigQueryRequest) -> CanonicalResult<NamesrvConfigQueryResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::get_namesrv_config(&mut admin, request.namesrv_addrs())
            .await
            .map(|configs| NamesrvConfigQueryResult { configs });
        admin.shutdown().await;
        result
    }

    /// Query NameServer configuration using the caller-owned runtime and optional credentials.
    pub async fn query_namesrv_config_by_request_with_credentials(
        request: NamesrvConfigQueryRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<NamesrvConfigQueryResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await?;
        let result = Self::get_namesrv_config(&mut admin, request.namesrv_addrs())
            .await
            .map(|configs| NamesrvConfigQueryResult { configs });
        admin.shutdown().await;
        result
    }

    pub async fn update_namesrv_config_by_request(
        request: NamesrvConfigUpdateRequest,
    ) -> CanonicalResult<NamesrvConfigUpdateResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let properties = request.properties().clone();
        let namesrv_addrs = request.namesrv_addrs();
        let result = Self::update_namesrv_config(&mut admin, properties.clone(), namesrv_addrs.clone())
            .await
            .map(|_| NamesrvConfigUpdateResult {
                properties,
                namesrv_addrs,
            });
        admin.shutdown().await;
        result
    }

    /// Update NameServer configuration using the caller-owned runtime and optional credentials.
    pub async fn update_namesrv_config_by_request_with_credentials(
        request: NamesrvConfigUpdateRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<NamesrvConfigUpdateResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await?;
        let properties = request.properties().clone();
        let namesrv_addrs = request.namesrv_addrs();
        let result = Self::update_namesrv_config(&mut admin, properties.clone(), namesrv_addrs.clone())
            .await
            .map(|_| NamesrvConfigUpdateResult {
                properties,
                namesrv_addrs,
            });
        admin.shutdown().await;
        result
    }

    pub async fn update_kv_config_by_request(request: KvConfigUpdateRequest) -> CanonicalResult<KvConfigUpdateResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::create_or_update_kv_config(
            &mut admin,
            request.namespace().clone(),
            request.key().clone(),
            request.value().clone(),
        )
        .await
        .map(|_| KvConfigUpdateResult {
            namespace: request.namespace().clone(),
            key: request.key().clone(),
            value: Some(request.value().clone()),
        });
        admin.shutdown().await;
        result
    }

    /// Create or update NameServer KV config using the caller-owned runtime and optional credentials.
    pub async fn update_kv_config_by_request_with_credentials(
        request: KvConfigUpdateRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<KvConfigUpdateResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await?;
        let result = Self::create_or_update_kv_config(
            &mut admin,
            request.namespace().clone(),
            request.key().clone(),
            request.value().clone(),
        )
        .await
        .map(|_| KvConfigUpdateResult {
            namespace: request.namespace().clone(),
            key: request.key().clone(),
            value: Some(request.value().clone()),
        });
        admin.shutdown().await;
        result
    }

    pub async fn delete_kv_config_by_request(request: KvConfigDeleteRequest) -> CanonicalResult<KvConfigUpdateResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::delete_kv_config(&mut admin, request.namespace().clone(), request.key().clone())
            .await
            .map(|_| KvConfigUpdateResult {
                namespace: request.namespace().clone(),
                key: request.key().clone(),
                value: None,
            });
        admin.shutdown().await;
        result
    }

    /// Delete NameServer KV config using the caller-owned runtime and optional credentials.
    pub async fn delete_kv_config_by_request_with_credentials(
        request: KvConfigDeleteRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<KvConfigUpdateResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await?;
        let result = Self::delete_kv_config(&mut admin, request.namespace().clone(), request.key().clone())
            .await
            .map(|_| KvConfigUpdateResult {
                namespace: request.namespace().clone(),
                key: request.key().clone(),
                value: None,
            });
        admin.shutdown().await;
        result
    }

    pub async fn add_write_perm_by_request(request: WritePermRequest) -> CanonicalResult<WritePermResult> {
        Self::apply_write_perm_by_request(request, true).await
    }

    /// Add broker write permission using the caller-owned runtime and optional credentials.
    pub async fn add_write_perm_by_request_with_credentials(
        request: WritePermRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<WritePermResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await?;
        let result = Self::apply_write_perm_with_admin(&mut admin, &request, true).await;
        admin.shutdown().await;
        result
    }

    pub async fn wipe_write_perm_by_request(request: WritePermRequest) -> CanonicalResult<WritePermResult> {
        Self::apply_write_perm_by_request(request, false).await
    }

    /// Wipe broker write permission using the caller-owned runtime and optional credentials.
    pub async fn wipe_write_perm_by_request_with_credentials(
        request: WritePermRequest,
        credentials: Option<crate::core::security::AdminCredentials>,
        client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
    ) -> CanonicalResult<WritePermResult> {
        let mut admin = admin_builder_with_credentials(request.admin_builder(), credentials, client_runtime)
            .build_and_start()
            .await?;
        let result = Self::apply_write_perm_with_admin(&mut admin, &request, false).await;
        admin.shutdown().await;
        result
    }

    async fn apply_write_perm_by_request(
        request: WritePermRequest,
        add_perm: bool,
    ) -> CanonicalResult<WritePermResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::apply_write_perm_with_admin(&mut admin, &request, add_perm).await;
        admin.shutdown().await;
        result
    }

    async fn apply_write_perm_with_admin(
        admin: &mut DefaultMQAdminExt,
        request: &WritePermRequest,
        add_perm: bool,
    ) -> CanonicalResult<WritePermResult> {
        let mut namesrv_addrs = request.namesrv_addrs();
        if namesrv_addrs.is_empty() {
            namesrv_addrs = admin.get_name_server_address_list().await;
        }

        let mut entries = Vec::with_capacity(namesrv_addrs.len());
        for namesrv_addr in namesrv_addrs {
            let result = if add_perm {
                Self::add_write_perm_of_broker(admin, namesrv_addr.clone(), request.broker_name().clone()).await
            } else {
                Self::wipe_write_perm_of_broker(admin, namesrv_addr.clone(), request.broker_name().clone()).await
            };

            match result {
                Ok(affected_count) => entries.push(WritePermResultEntry {
                    namesrv_addr,
                    affected_count: Some(affected_count),
                    error: None,
                }),
                Err(error) => entries.push(WritePermResultEntry {
                    namesrv_addr,
                    affected_count: None,
                    error: Some(crate::client_adapter::services::stable_error_message(&error)),
                }),
            }
        }

        Ok(WritePermResult {
            broker_name: request.broker_name().clone(),
            entries,
        })
    }

    /// Get NameServer configurations
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `nameserver_addrs` - List of NameServer addresses
    ///
    /// # Returns
    /// Map of NameServer address to configuration key-value pairs
    pub async fn get_namesrv_config(
        admin: &mut DefaultMQAdminExt,
        nameserver_addrs: Vec<CheetahString>,
    ) -> CanonicalResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        admin.get_name_server_config(nameserver_addrs).await.map_err(|error| {
            crate::client_adapter::services::errors::admin_operation_failed_by("get_namesrv_config", error)
        })
    }

    /// Update NameServer configurations
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `properties` - Configuration properties to update
    /// * `nameserver_addrs` - Optional list of specific NameServer addresses
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn update_namesrv_config(
        admin: &mut DefaultMQAdminExt,
        properties: HashMap<CheetahString, CheetahString>,
        nameserver_addrs: Option<Vec<CheetahString>>,
    ) -> CanonicalResult<()> {
        admin
            .update_name_server_config(properties, nameserver_addrs)
            .await
            .map_err(|error| {
                crate::client_adapter::services::errors::admin_operation_failed_by("update_namesrv_config", error)
            })
    }

    /// Create or update KV config in NameServer
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `namespace` - Config namespace
    /// * `key` - Config key
    /// * `value` - Config value
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn create_or_update_kv_config(
        admin: &mut DefaultMQAdminExt,
        namespace: impl Into<CheetahString>,
        key: impl Into<CheetahString>,
        value: impl Into<CheetahString>,
    ) -> CanonicalResult<()> {
        let namespace = namespace.into();
        let key = key.into();
        let value = value.into();

        admin
            .create_and_update_kv_config(namespace.clone(), key.clone(), value)
            .await
            .map_err(|error| {
                crate::client_adapter::services::errors::admin_operation_failed_by("create_or_update_kv_config", error)
            })
    }

    /// Delete KV config from NameServer
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `namespace` - Config namespace
    /// * `key` - Config key
    ///
    /// # Returns
    /// Result indicating success or failure
    pub async fn delete_kv_config(
        admin: &mut DefaultMQAdminExt,
        namespace: impl Into<CheetahString>,
        key: impl Into<CheetahString>,
    ) -> CanonicalResult<()> {
        let namespace = namespace.into();
        let key = key.into();

        admin
            .delete_kv_config(namespace.clone(), key.clone())
            .await
            .map_err(|error| {
                crate::client_adapter::services::errors::admin_operation_failed_by("delete_kv_config", error)
            })
    }

    /// Add write permission for a broker
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `namesrv_addr` - NameServer address
    /// * `broker_name` - Broker name
    ///
    /// # Returns
    /// Number of affected brokers
    pub async fn add_write_perm_of_broker(
        admin: &mut DefaultMQAdminExt,
        namesrv_addr: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
    ) -> CanonicalResult<i32> {
        let namesrv = namesrv_addr.into();
        let broker = broker_name.into();

        admin
            .add_write_perm_of_broker(namesrv.clone(), broker.clone())
            .await
            .map_err(|error| {
                crate::client_adapter::services::errors::admin_operation_failed_by("add_write_perm_of_broker", error)
            })
    }

    /// Wipe write permission for a broker
    ///
    /// # Arguments
    /// * `admin` - Admin client instance
    /// * `namesrv_addr` - NameServer address
    /// * `broker_name` - Broker name
    ///
    /// # Returns
    /// Number of affected brokers
    pub async fn wipe_write_perm_of_broker(
        admin: &mut DefaultMQAdminExt,
        namesrv_addr: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
    ) -> CanonicalResult<i32> {
        let namesrv = namesrv_addr.into();
        let broker = broker_name.into();

        admin
            .wipe_write_perm_of_broker(namesrv.clone(), broker.clone())
            .await
            .map_err(|error| {
                crate::client_adapter::services::errors::admin_operation_failed_by("wipe_write_perm_of_broker", error)
            })
    }
}

fn admin_builder_with_credentials(
    builder: AdminBuilder,
    credentials: Option<crate::core::security::AdminCredentials>,
    client_runtime: std::sync::Arc<rocketmq_client_rust::ClientRuntime>,
) -> AdminBuilder {
    let builder = builder.client_runtime(client_runtime);
    match credentials {
        Some(hook) => builder.credentials(hook),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namesrv_service_exists() {
        // Verify service can be instantiated
        let _service = NameServerService;
    }
}
