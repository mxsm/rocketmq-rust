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

#[cfg(all(test, not(doctest)))]
use super::connection_handler::SessionCommandInterceptor;
use super::lifecycle_events::LifecycleEventConfig;
use super::lifecycle_events::LifecycleEventPublisher;
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ServerSecurityState {
    Unconfigured,
    ExplicitInsecureLoopback,
    Secure,
}

pub(super) struct PreparedServer<RP> {
    pub(super) dispatcher: Arc<AuthorizedCommandDispatcher<RP>>,
    pub(super) capabilities: RemotingServerRunCapabilities,
    pub(super) event_publisher: Option<LifecycleEventPublisher>,
    pub(super) lifecycle_shutdown: CancellationToken,
    pub(super) lifecycle_dispatcher_task: Option<TaskId>,
    pub(super) security_state: ServerSecurityState,
}

pub(super) struct RemotingServerRunCapabilities {
    pub(super) tls_runtime: TlsServerRuntime,
    pub(super) task_group: TaskGroup,
    pub(super) file_region_blocking: BlockingExecutor,
    pub(super) file_transfer_mode: FileTransferMode,
    pub(super) frame_limits: FrameLimits,
    pub(super) process_budget: rocketmq_runtime::ResourceBudget,
    pub(super) transport_security: Option<Arc<TransportSecurity>>,
    pub(super) transport_principal: Option<Principal>,
    pub(super) admission: Option<Arc<AdmissionController>>,
    #[cfg(all(test, not(doctest)))]
    pub(super) command_interceptor: Arc<dyn SessionCommandInterceptor>,
    pub(super) telemetry: TransportTelemetry,
    pub(super) lifecycle_event_config: LifecycleEventConfig,
    pub(super) proxy_protocol: ProxyProtocolConfig,
}
