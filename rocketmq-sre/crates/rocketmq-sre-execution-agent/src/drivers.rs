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

mod admin_core;
mod config;
mod kubernetes;
mod logger_level_ttl;
mod proxy_scale_out_one;

use std::future::Future;
use std::pin::Pin;

use rocketmq_sre_contracts::AgentStepRequest;

pub use admin_core::AdminCoreDriver;
pub use config::ConfigDriver;
pub use config::ConfigWriteClient;
pub use config::LoggerLevelControlClient;
pub use config::LoggerLevelState;
pub use config::LoggerLevelTtlRestore;
pub use config::LoggerLevelTtlWrite;
pub use kubernetes::KubernetesDriver;
pub use kubernetes::ProxyScaleClient;
pub use kubernetes::ProxyScaleOutOneWrite;
pub use kubernetes::ProxyScaleRestore;
pub use kubernetes::ProxyScaleState;
pub use logger_level_ttl::LoggerLevelTtlHandler;
pub use logger_level_ttl::LoggerLevelTtlParameters;
pub use proxy_scale_out_one::ProxyScaleOutOneHandler;
pub use proxy_scale_out_one::ProxyScaleOutOneParameters;

use crate::ExecutionAgentError;
use rocketmq_sre_contracts::AgentReadRequest;
use rocketmq_sre_contracts::AgentReadResult;
use rocketmq_sre_contracts::ReconcileEffectResponse;

/// Sanitized successful driver result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriverDispatchOutcome {
    pub operation_id: String,
    pub outcome_code: String,
    pub sanitized_summary: String,
}

/// Common closed behavior implemented by one of the three typed driver families.
pub trait AgentActionHandler: Send + Sync {
    fn read_state<'a>(&'a self, request: &'a AgentReadRequest) -> DriverFuture<'a, AgentReadResult>;

    fn dispatch<'a>(
        &'a self,
        request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome>;

    fn reconcile<'a>(
        &'a self,
        request: &'a AgentReadRequest,
        operation_id: Option<&str>,
    ) -> DriverFuture<'a, ReconcileEffectResponse>;

    fn compensate<'a>(
        &'a self,
        request: &'a AgentStepRequest,
        operation_id: &'a str,
    ) -> DriverFuture<'a, DriverDispatchOutcome>;
}

pub type DriverFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ExecutionAgentError>> + Send + 'a>>;
