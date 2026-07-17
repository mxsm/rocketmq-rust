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

#[cfg(not(feature = "legacy-common-compat"))]
use std::sync::Arc;

#[cfg(not(feature = "legacy-common-compat"))]
use crate::core::clock::Clock;
#[cfg(not(feature = "legacy-common-compat"))]
use crate::core::clock::SystemClock;

#[cfg(feature = "legacy-common-compat")]
pub use crate::client_adapter::legacy::core::admin::*;

#[cfg(all(feature = "client-adapter", not(feature = "legacy-common-compat")))]
pub use crate::client_adapter::lifecycle::AdminGuard;
#[cfg(all(feature = "client-adapter", not(feature = "legacy-common-compat")))]
pub use crate::client_adapter::lifecycle::AdminSession;

#[cfg(not(feature = "legacy-common-compat"))]
#[derive(Clone)]
pub struct AdminBuilder {
    namesrv_addr: Option<String>,
    instance_name: Option<String>,
    timeout_millis: u64,
    unit_name: Option<String>,
    clock: Arc<dyn Clock>,
}

#[cfg(not(feature = "legacy-common-compat"))]
impl Default for AdminBuilder {
    fn default() -> Self {
        Self {
            namesrv_addr: None,
            instance_name: None,
            timeout_millis: 5_000,
            unit_name: None,
            clock: Arc::new(SystemClock),
        }
    }
}

#[cfg(not(feature = "legacy-common-compat"))]
impl std::fmt::Debug for AdminBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdminBuilder")
            .field("namesrv_addr", &self.namesrv_addr)
            .field("instance_name", &self.instance_name)
            .field("timeout_millis", &self.timeout_millis)
            .field("unit_name", &self.unit_name)
            .field("clock", &"dyn Clock")
            .finish()
    }
}

#[cfg(not(feature = "legacy-common-compat"))]
impl AdminBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn namesrv_addr(mut self, addr: impl Into<String>) -> Self {
        self.namesrv_addr = Some(addr.into());
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

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn configured_namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn configured_instance_name(&self) -> Option<&str> {
        self.instance_name.as_deref()
    }

    pub fn configured_timeout_millis(&self) -> u64 {
        self.timeout_millis
    }

    pub fn configured_clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }
}

#[cfg(all(test, not(feature = "legacy-common-compat")))]
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
            .instance_name("contract-only")
            .timeout_millis(8_000)
            .clock(Arc::new(FixedClock(42)));

        assert_eq!(builder.configured_namesrv_addr(), Some("127.0.0.1:9876"));
        assert_eq!(builder.configured_instance_name(), Some("contract-only"));
        assert_eq!(builder.configured_timeout_millis(), 8_000);
        assert_eq!(builder.configured_clock().now_millis(), 42);
    }
}
