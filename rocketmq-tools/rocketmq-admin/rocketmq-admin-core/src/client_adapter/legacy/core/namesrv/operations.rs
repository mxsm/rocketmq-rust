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
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::RocketMQError;
use crate::core::RocketMQResult;
use crate::core::ToolsError;

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

/// NameServer operations service
pub struct NameServerService;

impl NameServerService {
    pub async fn query_namesrv_config(request: NamesrvConfigQueryRequest) -> RocketMQResult<NamesrvConfigQueryResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let result = Self::get_namesrv_config(&mut admin, request.namesrv_addrs())
            .await
            .map(|configs| NamesrvConfigQueryResult { configs });
        admin.shutdown().await;
        result
    }

    pub async fn update_namesrv_config_by_request(
        request: NamesrvConfigUpdateRequest,
    ) -> RocketMQResult<NamesrvConfigUpdateResult> {
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

    pub async fn update_kv_config_by_request(request: KvConfigUpdateRequest) -> RocketMQResult<KvConfigUpdateResult> {
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

    pub async fn delete_kv_config_by_request(request: KvConfigDeleteRequest) -> RocketMQResult<KvConfigUpdateResult> {
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

    pub async fn add_write_perm_by_request(request: WritePermRequest) -> RocketMQResult<WritePermResult> {
        Self::apply_write_perm_by_request(request, true).await
    }

    pub async fn wipe_write_perm_by_request(request: WritePermRequest) -> RocketMQResult<WritePermResult> {
        Self::apply_write_perm_by_request(request, false).await
    }

    async fn apply_write_perm_by_request(request: WritePermRequest, add_perm: bool) -> RocketMQResult<WritePermResult> {
        let mut admin = request.admin_builder().build_and_start().await?;
        let mut namesrv_addrs = request.namesrv_addrs();
        if namesrv_addrs.is_empty() {
            namesrv_addrs = admin.get_name_server_address_list().await;
        }

        let mut entries = Vec::with_capacity(namesrv_addrs.len());
        for namesrv_addr in namesrv_addrs {
            let result = if add_perm {
                Self::add_write_perm_of_broker(&mut admin, namesrv_addr.clone(), request.broker_name().clone()).await
            } else {
                Self::wipe_write_perm_of_broker(&mut admin, namesrv_addr.clone(), request.broker_name().clone()).await
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
                    error: Some(error.to_string()),
                }),
            }
        }

        admin.shutdown().await;
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
    ) -> RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        admin
            .get_name_server_config(nameserver_addrs)
            .await
            .map_err(|e| RocketMQError::Tools(ToolsError::nameserver_config_invalid(e.to_string())))
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
    ) -> RocketMQResult<()> {
        admin
            .update_name_server_config(properties, nameserver_addrs)
            .await
            .map_err(|e| RocketMQError::Tools(ToolsError::nameserver_config_invalid(e.to_string())))
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
    ) -> RocketMQResult<()> {
        let namespace = namespace.into();
        let key = key.into();
        let value = value.into();

        admin
            .create_and_update_kv_config(namespace.clone(), key.clone(), value)
            .await
            .map_err(|e| {
                RocketMQError::Tools(ToolsError::nameserver_config_invalid(format!(
                    "Failed to create/update KV config [{namespace}:{key}]: {e}"
                )))
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
    ) -> RocketMQResult<()> {
        let namespace = namespace.into();
        let key = key.into();

        admin
            .delete_kv_config(namespace.clone(), key.clone())
            .await
            .map_err(|e| {
                RocketMQError::Tools(ToolsError::nameserver_config_invalid(format!(
                    "Failed to delete KV config [{namespace}:{key}]: {e}"
                )))
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
    ) -> RocketMQResult<i32> {
        let namesrv = namesrv_addr.into();
        let broker = broker_name.into();

        admin
            .add_write_perm_of_broker(namesrv.clone(), broker.clone())
            .await
            .map_err(|e| {
                RocketMQError::Tools(ToolsError::broker_not_found(format!(
                    "Failed to add write permission for broker '{broker}' on NameServer '{namesrv}': {e}"
                )))
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
    ) -> RocketMQResult<i32> {
        let namesrv = namesrv_addr.into();
        let broker = broker_name.into();

        admin
            .wipe_write_perm_of_broker(namesrv.clone(), broker.clone())
            .await
            .map_err(|e| {
                RocketMQError::Tools(ToolsError::broker_not_found(format!(
                    "Failed to wipe write permission for broker '{broker}' on NameServer '{namesrv}': {e}"
                )))
            })
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
