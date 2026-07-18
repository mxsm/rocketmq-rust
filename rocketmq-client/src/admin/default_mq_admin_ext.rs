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
use rocketmq_remoting::runtime::RPCHook;

use crate::admin::default_mq_admin_ext_impl::DefaultMQAdminExtImpl;
use crate::admin::mq_admin_ext_async::MQAdminExt;
use crate::base::client_config::ClientConfig;

const ADMIN_EXT_GROUP: &str = "admin_ext_group";
const DEFAULT_TIMEOUT_MILLIS: u64 = 5000;

/// Java-style Admin facade that owns a self-wired `DefaultMQAdminExtImpl`.
///
/// `DefaultMQAdminExtImpl` remains the concrete implementation of the
/// `MQAdminExt` trait. This facade provides the Java-like no-argument
/// constructors and lifecycle methods while dereferencing to the implementation
/// for advanced admin operations.
pub struct DefaultMQAdminExt {
    default_mqadmin_ext_impl: DefaultMQAdminExtImpl,
}

impl DefaultMQAdminExt {
    fn build(
        client_config: ClientConfig,
        admin_ext_group: CheetahString,
        timeout_millis: Duration,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let default_mqadmin_ext_impl =
            DefaultMQAdminExtImpl::new(rpc_hook, timeout_millis, client_config, admin_ext_group);

        Self {
            default_mqadmin_ext_impl,
        }
    }

    pub fn new() -> Self {
        Self::with_admin_ext_group(ADMIN_EXT_GROUP)
    }

    pub fn with_timeout(timeout_millis: Duration) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_config,
            CheetahString::from_static_str(ADMIN_EXT_GROUP),
            timeout_millis,
            None,
        )
    }

    pub fn with_rpc_hook(rpc_hook: Arc<dyn RPCHook>) -> Self {
        Self::with_rpc_hook_and_timeout(rpc_hook, Duration::from_millis(DEFAULT_TIMEOUT_MILLIS))
    }

    pub fn with_rpc_hook_and_timeout(rpc_hook: Arc<dyn RPCHook>, timeout_millis: Duration) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_config,
            CheetahString::from_static_str(ADMIN_EXT_GROUP),
            timeout_millis,
            Some(rpc_hook),
        )
    }

    pub fn with_admin_ext_group(admin_ext_group: impl Into<CheetahString>) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_config,
            admin_ext_group.into(),
            Duration::from_millis(DEFAULT_TIMEOUT_MILLIS),
            None,
        )
    }

    pub fn with_admin_ext_group_and_timeout(
        admin_ext_group: impl Into<CheetahString>,
        timeout_millis: Duration,
    ) -> Self {
        let client_config = ClientConfig::new();
        Self::build(client_config, admin_ext_group.into(), timeout_millis, None)
    }

    pub fn with_admin_ext_group_and_rpc_hook(
        admin_ext_group: impl Into<CheetahString>,
        rpc_hook: Arc<dyn RPCHook>,
    ) -> Self {
        let client_config = ClientConfig::new();
        Self::build(
            client_config,
            admin_ext_group.into(),
            Duration::from_millis(DEFAULT_TIMEOUT_MILLIS),
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

    pub async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        MQAdminExt::start(&mut self.default_mqadmin_ext_impl).await
    }

    pub async fn shutdown(&mut self) {
        MQAdminExt::shutdown(&mut self.default_mqadmin_ext_impl).await;
    }
}

impl Default for DefaultMQAdminExt {
    fn default() -> Self {
        Self::new()
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_error::RocketMQError;

    use crate::admin::mq_admin_ext_async::MQAdminExt;

    use super::DefaultMQAdminExt;

    #[test]
    fn constructors_initialize_owned_inner_impl() {
        let default_admin = DefaultMQAdminExt::new();
        assert!(default_admin.has_inner());
        assert!(default_admin.inner().has_inner());

        let timed_admin = DefaultMQAdminExt::with_timeout(Duration::from_secs(3));
        assert!(timed_admin.has_inner());

        let grouped_admin = DefaultMQAdminExt::with_admin_ext_group("admin-public-api-test");
        assert!(grouped_admin.has_inner());

        let grouped_timed_admin =
            DefaultMQAdminExt::with_admin_ext_group_and_timeout("admin-public-api-test", Duration::from_secs(3));
        assert!(grouped_timed_admin.has_inner());
    }

    #[test]
    fn namesrv_addr_updates_shared_client_config_before_start() {
        let mut admin = DefaultMQAdminExt::new();
        admin.set_namesrv_addr("127.0.0.1:9876");

        assert_eq!(
            admin.client_config().get_namesrv_addr().as_deref(),
            Some("127.0.0.1:9876")
        );
    }

    #[test]
    fn use_tls_updates_shared_admin_client_config_before_start() {
        let mut admin = DefaultMQAdminExt::new();

        assert!(!admin.is_use_tls());
        assert!(!admin.inner().is_use_tls());

        admin.set_use_tls(true);

        assert!(admin.is_use_tls());
        assert!(admin.client_config().is_use_tls());
        assert!(admin.inner().is_use_tls());
    }

    #[tokio::test]
    async fn deref_exposes_impl_admin_trait_methods_without_panicking() {
        let admin = DefaultMQAdminExt::new();

        let error = admin
            .get_kv_config(CheetahString::from("namespace"), CheetahString::from("key"))
            .await
            .expect_err("unstarted admin should return a typed error instead of panicking");

        assert!(matches!(error, RocketMQError::ClientNotStarted));
    }
}
