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

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_transport::RPCHook;

use crate::admin::default_mq_admin_ext_impl::DefaultMQAdminExtImpl;
#[cfg(all(feature = "admin-read", not(feature = "admin-mutation")))]
use crate::admin::mq_admin_read_ext::MQAdminReadExt;
use crate::base::client_config::ClientConfig;
use crate::runtime::ClientRuntime;
use crate::session::ClientSession;
use crate::session::ClientSessionProvider;

const ADMIN_EXT_GROUP: &str = "admin_ext_group";
const DEFAULT_TIMEOUT_MILLIS: u64 = 5000;

/// Java-style Admin facade that owns a self-wired `DefaultMQAdminExtImpl`.
///
/// `DefaultMQAdminExtImpl` remains the concrete implementation behind the
/// scoped administration capabilities. This facade requires an
/// application-owned [`ClientRuntime`] and dereferences to the implementation
/// for advanced admin operations.
pub struct DefaultMQAdminExt {
    session: ClientSession,
    default_mqadmin_ext_impl: DefaultMQAdminExtImpl,
}

impl DefaultMQAdminExt {
    fn build(
        client_runtime: Arc<ClientRuntime>,
        client_config: ClientConfig,
        admin_ext_group: CheetahString,
        timeout_millis: Duration,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let default_mqadmin_ext_impl = DefaultMQAdminExtImpl::new(
            Arc::clone(&client_runtime),
            rpc_hook,
            timeout_millis,
            client_config,
            admin_ext_group,
        );

        Self {
            session: ClientSession::new(client_runtime),
            default_mqadmin_ext_impl,
        }
    }

    pub fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self::with_admin_ext_group(client_runtime, ADMIN_EXT_GROUP)
    }

    /// Returns the application-owned runtime borrowed by this Admin facade.
    pub fn client_runtime(&self) -> Arc<ClientRuntime> {
        self.session.runtime()
    }

    pub fn with_timeout(client_runtime: Arc<ClientRuntime>, timeout_millis: Duration) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_runtime,
            client_config,
            CheetahString::from_static_str(ADMIN_EXT_GROUP),
            timeout_millis,
            None,
        )
    }

    pub fn with_rpc_hook(client_runtime: Arc<ClientRuntime>, rpc_hook: Arc<dyn RPCHook>) -> Self {
        Self::with_rpc_hook_and_timeout(client_runtime, rpc_hook, Duration::from_millis(DEFAULT_TIMEOUT_MILLIS))
    }

    pub fn with_rpc_hook_and_timeout(
        client_runtime: Arc<ClientRuntime>,
        rpc_hook: Arc<dyn RPCHook>,
        timeout_millis: Duration,
    ) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_runtime,
            client_config,
            CheetahString::from_static_str(ADMIN_EXT_GROUP),
            timeout_millis,
            Some(rpc_hook),
        )
    }

    pub fn with_admin_ext_group(client_runtime: Arc<ClientRuntime>, admin_ext_group: impl Into<CheetahString>) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_runtime,
            client_config,
            admin_ext_group.into(),
            Duration::from_millis(DEFAULT_TIMEOUT_MILLIS),
            None,
        )
    }

    pub fn with_admin_ext_group_and_timeout(
        client_runtime: Arc<ClientRuntime>,
        admin_ext_group: impl Into<CheetahString>,
        timeout_millis: Duration,
    ) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_runtime,
            client_config,
            admin_ext_group.into(),
            timeout_millis,
            None,
        )
    }

    pub fn with_admin_ext_group_and_rpc_hook(
        client_runtime: Arc<ClientRuntime>,
        admin_ext_group: impl Into<CheetahString>,
        rpc_hook: Arc<dyn RPCHook>,
    ) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_runtime,
            client_config,
            admin_ext_group.into(),
            Duration::from_millis(DEFAULT_TIMEOUT_MILLIS),
            Some(rpc_hook),
        )
    }

    pub fn with_admin_ext_group_rpc_hook_and_timeout(
        client_runtime: Arc<ClientRuntime>,
        admin_ext_group: impl Into<CheetahString>,
        rpc_hook: Arc<dyn RPCHook>,
        timeout_millis: Duration,
    ) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_runtime,
            client_config,
            admin_ext_group.into(),
            timeout_millis,
            Some(rpc_hook),
        )
    }

    pub fn set_namesrv_addr(&mut self, name_serv_addr: impl Into<CheetahString>) {
        self.default_mqadmin_ext_impl
            .client_config_mut()
            .set_namesrv_addr(name_serv_addr.into());
    }

    #[inline]
    pub fn is_use_tls(&self) -> bool {
        self.default_mqadmin_ext_impl.is_use_tls()
    }

    #[inline]
    pub fn set_use_tls(&mut self, use_tls: bool) {
        self.default_mqadmin_ext_impl.set_use_tls(use_tls);
    }

    #[inline]
    pub fn client_config(&self) -> &ClientConfig {
        self.default_mqadmin_ext_impl.client_config()
    }

    #[inline]
    pub fn client_config_mut(&mut self) -> &mut ClientConfig {
        self.default_mqadmin_ext_impl.client_config_mut()
    }

    #[inline]
    pub fn inner(&self) -> &DefaultMQAdminExtImpl {
        &self.default_mqadmin_ext_impl
    }

    #[inline]
    pub fn inner_mut(&mut self) -> &mut DefaultMQAdminExtImpl {
        &mut self.default_mqadmin_ext_impl
    }

    #[inline]
    pub fn has_inner(&self) -> bool {
        self.default_mqadmin_ext_impl.has_inner()
    }

    #[cfg(any(feature = "admin-read", feature = "admin-mutation"))]
    pub async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        #[cfg(feature = "admin-mutation")]
        {
            return self.default_mqadmin_ext_impl.start_admin().await;
        }
        #[cfg(all(feature = "admin-read", not(feature = "admin-mutation")))]
        {
            MQAdminReadExt::start(self).await
        }
    }

    #[cfg(any(feature = "admin-read", feature = "admin-mutation"))]
    pub async fn shutdown(&mut self) {
        #[cfg(feature = "admin-mutation")]
        {
            self.default_mqadmin_ext_impl.shutdown_admin().await;
        }
        #[cfg(all(feature = "admin-read", not(feature = "admin-mutation")))]
        {
            MQAdminReadExt::shutdown(self).await;
        }
    }
}

impl ClientSessionProvider for DefaultMQAdminExt {
    fn client_session(&self) -> Option<&ClientSession> {
        Some(&self.session)
    }
}

impl AsRef<DefaultMQAdminExtImpl> for DefaultMQAdminExt {
    fn as_ref(&self) -> &DefaultMQAdminExtImpl {
        &self.default_mqadmin_ext_impl
    }
}

impl AsMut<DefaultMQAdminExtImpl> for DefaultMQAdminExt {
    fn as_mut(&mut self) -> &mut DefaultMQAdminExtImpl {
        &mut self.default_mqadmin_ext_impl
    }
}

impl Deref for DefaultMQAdminExt {
    type Target = DefaultMQAdminExtImpl;

    fn deref(&self) -> &Self::Target {
        &self.default_mqadmin_ext_impl
    }
}

impl DerefMut for DefaultMQAdminExt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.default_mqadmin_ext_impl
    }
}

#[cfg(all(test, feature = "admin-full"))]
mod tests {
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQError;

    use crate::admin::capability::RouteAdmin;

    use super::DefaultMQAdminExt;

    fn test_runtime() -> std::sync::Arc<crate::runtime::ClientRuntime> {
        crate::runtime::test_client_runtime("default-admin-ext-test")
    }

    #[test]
    fn constructors_initialize_owned_inner_impl() {
        let default_admin = DefaultMQAdminExt::new(test_runtime());
        assert!(default_admin.has_inner());
        assert!(default_admin.inner().has_inner());

        let timed_admin = DefaultMQAdminExt::with_timeout(test_runtime(), Duration::from_secs(3));
        assert!(timed_admin.has_inner());

        let grouped_admin = DefaultMQAdminExt::with_admin_ext_group(test_runtime(), "admin-public-api-test");
        assert!(grouped_admin.has_inner());

        let grouped_timed_admin = DefaultMQAdminExt::with_admin_ext_group_and_timeout(
            test_runtime(),
            "admin-public-api-test",
            Duration::from_secs(3),
        );
        assert!(grouped_timed_admin.has_inner());
    }

    #[test]
    fn namesrv_addr_updates_shared_client_config_before_start() {
        let mut admin = DefaultMQAdminExt::new(test_runtime());
        admin.set_namesrv_addr("127.0.0.1:9876");

        assert_eq!(
            admin.client_config().get_namesrv_addr().as_deref(),
            Some("127.0.0.1:9876")
        );
    }

    #[test]
    fn use_tls_updates_shared_admin_client_config_before_start() {
        let mut admin = DefaultMQAdminExt::new(test_runtime());

        assert!(!admin.is_use_tls());
        assert!(!admin.inner().is_use_tls());

        admin.set_use_tls(true);

        assert!(admin.is_use_tls());
        assert!(admin.client_config().is_use_tls());
        assert!(admin.inner().is_use_tls());
    }

    #[tokio::test]
    async fn deref_exposes_impl_admin_trait_methods_without_panicking() {
        let admin = DefaultMQAdminExt::new(test_runtime());

        let error = admin
            .get_kv_config(CheetahString::from("namespace"), CheetahString::from("key"))
            .await
            .expect_err("unstarted admin should return a typed error instead of panicking");

        assert!(matches!(error, RocketMQError::ClientNotStarted));
    }
}
