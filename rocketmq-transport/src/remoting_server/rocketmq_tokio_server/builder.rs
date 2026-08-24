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

use super::*;

impl<RP> TransportServer<RP> {
    pub fn new(config: Arc<ServerConfig>, service_context: ChildServiceContext) -> Self {
        Self {
            config,
            rpc_hooks: Some(vec![]),
            service_context,
            transport_security: None,
            transport_principal: None,
            admission: None,
            authorized_dispatcher: None,
            telemetry: TransportTelemetry::noop(),
            lifecycle_event_config: LifecycleEventConfig::default(),
            frame_limits: FrameLimits::java_compatibility(),
            proxy_protocol: ProxyProtocolConfig::default(),
            #[cfg(all(test, not(doctest)))]
            test_request_hook: None,
            _phantom_data: std::marker::PhantomData,
        }
    }

    pub fn new_with_service_context(config: Arc<ServerConfig>, service_context: ChildServiceContext) -> Self {
        Self::new(config, service_context)
    }

    /// Creates a remoting server bound to one explicit transport telemetry instance.
    pub fn new_with_telemetry(
        config: Arc<ServerConfig>,
        service_context: ChildServiceContext,
        telemetry: TransportTelemetry,
    ) -> Self {
        Self {
            telemetry,
            ..Self::new(config, service_context)
        }
    }

    /// Replaces the no-op transport recorder before the server starts.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TransportTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Applies one validated frame profile to every accepted connection.
    pub fn try_with_frame_limits(mut self, frame_limits: FrameLimits) -> RocketMQResult<Self> {
        frame_limits.validate()?;
        self.frame_limits = frame_limits;
        Ok(self)
    }

    /// Enables trusted PROXY v1/v2 negotiation before TLS and Remoting decoding.
    pub fn try_with_proxy_protocol(mut self, config: ProxyProtocolConfig) -> RocketMQResult<Self> {
        config.validate()?;
        self.proxy_protocol = config;
        Ok(self)
    }

    pub fn register_rpc_hook(&mut self, hook: Arc<dyn RPCHook>) {
        if let Some(ref mut hooks) = self.rpc_hooks {
            hooks.push(hook);
        } else {
            self.rpc_hooks = Some(vec![hook]);
        }
    }

    /// Installs transport authorization for accepted sessions.
    pub fn with_transport_security(
        mut self,
        transport_security: Arc<TransportSecurity>,
        principal: Option<Principal>,
    ) -> Self {
        self.transport_security = Some(transport_security);
        self.transport_principal = principal;
        self
    }

    #[doc(hidden)]
    pub fn with_admission_controller(mut self, admission: Arc<AdmissionController>) -> Self {
        self.admission = Some(admission);
        self
    }

    /// Installs one dispatcher shared with another trusted entry adapter.
    #[doc(hidden)]
    pub fn with_authorized_dispatcher(mut self, dispatcher: Arc<AuthorizedCommandDispatcher<RP>>) -> Self {
        self.authorized_dispatcher = Some(dispatcher);
        self
    }

    #[cfg(all(test, not(doctest)))]
    pub(super) fn with_test_request_hook(mut self, hook: TestRequestHook) -> Self {
        self.test_request_hook = Some(hook);
        self
    }
}
