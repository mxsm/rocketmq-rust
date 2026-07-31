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

//! Admin session configuration.

use std::sync::Arc;

use crate::core::clock::Clock;
use crate::core::clock::SystemClock;

#[derive(Clone)]
pub struct AdminBuilder {
    namesrv_addr: Option<String>,
    admin_group: Option<String>,
    instance_name: Option<String>,
    timeout_millis: u64,
    unit_name: Option<String>,
    vip_channel_enabled: bool,
    use_tls: bool,
    clock: Arc<dyn Clock>,
}

impl Default for AdminBuilder {
    fn default() -> Self {
        Self::base()
    }
}

impl AdminBuilder {
    fn base() -> Self {
        Self {
            namesrv_addr: None,
            admin_group: None,
            instance_name: None,
            timeout_millis: 5_000,
            unit_name: None,
            vip_channel_enabled: false,
            use_tls: false,
            clock: Arc::new(SystemClock),
        }
    }

    pub fn new() -> Self {
        Self::base()
    }
}

impl std::fmt::Debug for AdminBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminBuilder")
            .field("namesrv_addr", &self.namesrv_addr)
            .field("admin_group", &self.admin_group)
            .field("instance_name", &self.instance_name)
            .field("timeout_millis", &self.timeout_millis)
            .field("unit_name", &self.unit_name)
            .field("vip_channel_enabled", &self.vip_channel_enabled)
            .field("use_tls", &self.use_tls)
            .field("clock", &"dyn Clock")
            .finish()
    }
}

impl AdminBuilder {
    pub fn namesrv_addr(mut self, addr: impl Into<String>) -> Self {
        self.namesrv_addr = Some(addr.into());
        self
    }

    pub fn admin_group(mut self, group: impl Into<String>) -> Self {
        self.admin_group = Some(group.into());
        self
    }

    pub fn instance_name(mut self, name: impl Into<String>) -> Self {
        self.instance_name = Some(name.into());
        self
    }

    pub fn timeout_millis(mut self, timeout_millis: u64) -> Self {
        self.timeout_millis = timeout_millis;
        self
    }

    pub fn unit_name(mut self, name: impl Into<String>) -> Self {
        self.unit_name = Some(name.into());
        self
    }

    pub fn vip_channel_enabled(mut self, enabled: bool) -> Self {
        self.vip_channel_enabled = enabled;
        self
    }

    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn configured_namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    #[cfg(any(
        feature = "read-client-adapter",
        feature = "mutation-client-adapter",
        feature = "client-adapter",
        test
    ))]
    pub(crate) fn configured_admin_group(&self) -> Option<&str> {
        self.admin_group.as_deref()
    }

    pub fn configured_instance_name(&self) -> Option<&str> {
        self.instance_name.as_deref()
    }

    pub fn configured_timeout_millis(&self) -> u64 {
        self.timeout_millis
    }

    #[cfg(any(
        feature = "read-client-adapter",
        feature = "mutation-client-adapter",
        feature = "client-adapter",
        test
    ))]
    pub(crate) fn configured_unit_name(&self) -> Option<&str> {
        self.unit_name.as_deref()
    }

    #[cfg(any(
        feature = "read-client-adapter",
        feature = "mutation-client-adapter",
        feature = "client-adapter",
        test
    ))]
    pub(crate) fn configured_vip_channel_enabled(&self) -> bool {
        self.vip_channel_enabled
    }

    #[cfg(any(
        feature = "read-client-adapter",
        feature = "mutation-client-adapter",
        feature = "client-adapter",
        test
    ))]
    pub(crate) fn configured_use_tls(&self) -> bool {
        self.use_tls
    }

    pub fn configured_clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::core::clock::Clock;

    use super::AdminBuilder;

    struct FixedClock(u64);

    impl Clock for FixedClock {
        fn now_millis(&self) -> u64 {
            self.0
        }
    }

    #[test]
    fn builder_owns_configuration_without_loading_an_sdk() {
        let builder = AdminBuilder::new()
            .namesrv_addr("127.0.0.1:9876")
            .admin_group("admin-contract")
            .instance_name("contract-only")
            .timeout_millis(8_000)
            .unit_name("unit-a")
            .vip_channel_enabled(true)
            .use_tls(true)
            .clock(Arc::new(FixedClock(42)));

        assert_eq!(builder.configured_namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(builder.configured_admin_group(), Some("admin-contract"));
        assert_eq!(builder.configured_instance_name(), Some("contract-only"));
        assert_eq!(builder.configured_timeout_millis(), 8_000);
        assert_eq!(builder.configured_unit_name(), Some("unit-a"));
        assert!(builder.configured_vip_channel_enabled());
        assert!(builder.configured_use_tls());
        assert_eq!(builder.configured_clock().now_millis(), 42);
    }
}
