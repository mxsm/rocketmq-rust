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

//! Admin client builder and RAII resource management
//!
//! This module provides ergonomic patterns for managing admin clients:
//! - [`AdminBuilder`] - Fluent builder pattern for client configuration
//! - [`AdminGuard`] - RAII wrapper for automatic resource cleanup

use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::RocketMQResult;

/// Builder for creating and configuring admin clients
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_tools::core::admin::AdminBuilder;
///
/// // Simple usage
/// let admin = AdminBuilder::new()
///     .namesrv_addr("127.0.0.1:9876")
///     .build_and_start()
///     .await?;
///
/// // With custom configuration
/// let admin = AdminBuilder::new()
///     .namesrv_addr("127.0.0.1:9876;127.0.0.1:9877")
///     .instance_name("my-admin-tool")
///     .timeout_millis(5000)
///     .build_and_start()
///     .await?;
/// ```
#[derive(Debug, Clone, Default)]
pub struct AdminBuilder {
    namesrv_addr: Option<String>,
    instance_name: Option<String>,
    timeout_millis: Option<u64>,
    unit_name: Option<String>,
}

impl AdminBuilder {
    /// Create a new builder with default configuration
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set NameServer address
    ///
    /// Supports multiple addresses separated by semicolon:
    /// - Single: `"127.0.0.1:9876"`
    /// - Multiple: `"127.0.0.1:9876;127.0.0.1:9877"`
    #[inline]
    pub fn namesrv_addr(mut self, addr: impl Into<String>) -> Self {
        self.namesrv_addr = Some(addr.into());
        self
    }

    /// Set custom instance name
    ///
    /// If not set, defaults to `"tools-{timestamp}"`
    #[inline]
    pub fn instance_name(mut self, name: impl Into<String>) -> Self {
        self.instance_name = Some(name.into());
        self
    }

    /// Set timeout in milliseconds
    #[inline]
    pub fn timeout_millis(mut self, timeout: u64) -> Self {
        self.timeout_millis = Some(timeout);
        self
    }

    /// Set unit name for namespace isolation
    #[inline]
    pub fn unit_name(mut self, name: impl Into<String>) -> Self {
        self.unit_name = Some(name.into());
        self
    }

    /// Build and start the admin client
    ///
    /// This will:
    /// 1. Create a new DefaultMQAdminExt instance
    /// 2. Apply all configuration
    /// 3. Start the client (establish connections)
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - NameServer address is invalid
    /// - Connection cannot be established
    /// - Network I/O fails
    pub async fn build_and_start(self) -> RocketMQResult<DefaultMQAdminExt> {
        let mut admin = DefaultMQAdminExt::new();

        // Apply NameServer address
        if let Some(addr) = self.namesrv_addr {
            admin.set_namesrv_addr(&addr);
        }

        // Apply instance name (default: "tools-{timestamp}")
        let instance_name = self
            .instance_name
            .unwrap_or_else(|| format!("tools-{}", get_current_millis()));
        admin.client_config_mut().set_instance_name(instance_name.into());

        // Note: timeout_millis and unit_name are stored but not currently applied
        // as the corresponding setter methods are not available in ClientConfig

        // Start the admin client
        admin.start().await?;

        Ok(admin)
    }

    /// Build the admin client with RAII auto-cleanup
    ///
    /// Returns an [`AdminGuard`] that automatically calls shutdown when dropped.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// {
    ///     let admin = AdminBuilder::new()
    ///         .namesrv_addr("127.0.0.1:9876")
    ///         .build_with_guard()
    ///         .await?;
    ///
    ///     // Use admin...
    ///     let clusters = TopicService::get_topic_cluster_list(&admin, "MyTopic").await?;
    /// } // admin automatically cleaned up here
    /// ```
    pub async fn build_with_guard(self) -> RocketMQResult<AdminGuard> {
        let admin = self.build_and_start().await?;
        Ok(AdminGuard::new(admin))
    }
}

/// RAII guard for automatic admin client cleanup
///
/// This wrapper ensures that the admin client is properly shut down when
/// it goes out of scope, preventing resource leaks and dangling connections.
///
/// # Examples
///
/// ```rust,ignore
/// use rocketmq_tools::core::admin::{AdminBuilder, AdminGuard};
///
/// async fn process_topics() -> RocketMQResult<()> {
///     let admin = AdminBuilder::new()
///         .namesrv_addr("127.0.0.1:9876")
///         .build_with_guard()
///         .await?;
///
///     // Use admin through Deref
///     let result = admin.examine_topic_route_info("MyTopic").await?;
///
///     Ok(())
/// } // admin.shutdown() called automatically here
/// ```
pub struct AdminGuard {
    admin: Option<DefaultMQAdminExt>,
    runtime: tokio::runtime::Handle,
}

impl AdminGuard {
    /// Create a new guard wrapping an admin client
    fn new(admin: DefaultMQAdminExt) -> Self {
        Self {
            admin: Some(admin),
            runtime: tokio::runtime::Handle::current(),
        }
    }

    /// Manually shutdown the admin client
    ///
    /// This consumes the guard and prevents the automatic Drop shutdown.
    pub async fn shutdown(mut self) {
        if let Some(mut admin) = self.admin.take() {
            admin.shutdown().await;
        }
    }

    /// Get a reference to the inner admin client
    #[inline]
    pub fn inner(&self) -> &DefaultMQAdminExt {
        self.admin.as_ref().expect("AdminGuard already consumed")
    }

    /// Get a mutable reference to the inner admin client
    #[inline]
    pub fn inner_mut(&mut self) -> &mut DefaultMQAdminExt {
        self.admin.as_mut().expect("AdminGuard already consumed")
    }
}

impl std::ops::Deref for AdminGuard {
    type Target = DefaultMQAdminExt;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl std::ops::DerefMut for AdminGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner_mut()
    }
}

impl Drop for AdminGuard {
    fn drop(&mut self) {
        if let Some(mut admin) = self.admin.take() {
            // Spawn shutdown on the runtime
            // Note: This is best-effort cleanup; errors are ignored
            let runtime = self.runtime.clone();
            runtime.spawn(async move {
                let _ = admin.shutdown().await;
            });
        }
    }
}

// Allow using AdminGuard where DefaultMQAdminExt is expected
impl AsRef<DefaultMQAdminExt> for AdminGuard {
    fn as_ref(&self) -> &DefaultMQAdminExt {
        self.inner()
    }
}

impl AsMut<DefaultMQAdminExt> for AdminGuard {
    fn as_mut(&mut self) -> &mut DefaultMQAdminExt {
        self.inner_mut()
    }
}

/// Helper function to create a simple admin client with just address
///
/// Equivalent to:
/// ```rust,ignore
/// AdminBuilder::new()
///     .namesrv_addr(addr)
///     .build_and_start()
///     .await
/// ```
#[inline]
pub async fn create_admin(namesrv_addr: impl Into<String>) -> RocketMQResult<DefaultMQAdminExt> {
    AdminBuilder::new().namesrv_addr(namesrv_addr).build_and_start().await
}

/// Helper function to create an admin client with RAII guard
///
/// Equivalent to:
/// ```rust,ignore
/// AdminBuilder::new()
///     .namesrv_addr(addr)
///     .build_with_guard()
///     .await
/// ```
#[inline]
pub async fn create_admin_with_guard(namesrv_addr: impl Into<String>) -> RocketMQResult<AdminGuard> {
    AdminBuilder::new().namesrv_addr(namesrv_addr).build_with_guard().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_configuration() {
        let builder = AdminBuilder::new()
            .namesrv_addr("127.0.0.1:9876")
            .instance_name("test-instance")
            .timeout_millis(5000)
            .unit_name("test-unit");

        assert_eq!(builder.namesrv_addr, Some("127.0.0.1:9876".to_string()));
        assert_eq!(builder.instance_name, Some("test-instance".to_string()));
        assert_eq!(builder.timeout_millis, Some(5000));
        assert_eq!(builder.unit_name, Some("test-unit".to_string()));
    }

    #[test]
    fn test_builder_default() {
        let builder = AdminBuilder::default();
        assert!(builder.namesrv_addr.is_none());
        assert!(builder.instance_name.is_none());
        assert!(builder.timeout_millis.is_none());
        assert!(builder.unit_name.is_none());
    }

    #[test]
    fn test_builder_chaining() {
        let builder = AdminBuilder::new().namesrv_addr("addr1").namesrv_addr("addr2"); // Should override

        assert_eq!(builder.namesrv_addr, Some("addr2".to_string()));
    }

    #[test]
    fn test_builder_multiple_namesrv() {
        let builder = AdminBuilder::new().namesrv_addr("127.0.0.1:9876;127.0.0.1:9877;127.0.0.1:9878");

        assert_eq!(
            builder.namesrv_addr,
            Some("127.0.0.1:9876;127.0.0.1:9877;127.0.0.1:9878".to_string())
        );
    }

    #[test]
    fn test_builder_partial_config() {
        let builder = AdminBuilder::new().namesrv_addr("127.0.0.1:9876");

        assert_eq!(builder.namesrv_addr, Some("127.0.0.1:9876".to_string()));
        assert!(builder.instance_name.is_none());
        assert!(builder.timeout_millis.is_none());
    }

    #[test]
    fn test_builder_from_string_types() {
        let owned = String::from("127.0.0.1:9876");
        let builder1 = AdminBuilder::new().namesrv_addr(owned);

        let borrowed = "127.0.0.1:9876";
        let builder2 = AdminBuilder::new().namesrv_addr(borrowed);

        assert_eq!(builder1.namesrv_addr, builder2.namesrv_addr);
    }

    #[test]
    fn test_builder_fluent_api() {
        // Test that all methods return Self for chaining
        let builder = AdminBuilder::new()
            .namesrv_addr("addr")
            .instance_name("name")
            .timeout_millis(1000)
            .unit_name("unit")
            .namesrv_addr("new_addr"); // Can override

        assert_eq!(builder.namesrv_addr, Some("new_addr".to_string()));
    }
}
