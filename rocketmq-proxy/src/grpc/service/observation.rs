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

use std::time::Duration;
use std::time::Instant;

use rocketmq_runtime::BudgetedQueue;
use rocketmq_runtime::ResourcePermit;
use tonic::Streaming;
use tracing::field;
use tracing::info_span;
use tracing::Span;

use crate::auth::AuthenticatedPrincipal;
use crate::context::ProxyContext;
use crate::proto::v2;

use super::ProxyGrpcService;

#[derive(Clone)]
pub(super) struct RequestObservation {
    context: ProxyContext,
    started_at: Instant,
    span: Span,
}

impl RequestObservation {
    pub(super) fn new(context: ProxyContext) -> Self {
        let span = info_span!(
            "rocketmq_proxy.rpc",
            rpc = context.rpc_name(),
            request_id = %context.request_id(),
            client_id = context.client_id().unwrap_or(""),
            remote_addr = context.remote_addr().unwrap_or(""),
            local_addr = context.local_addr().unwrap_or(""),
            principal = field::Empty,
        );
        Self {
            context,
            started_at: Instant::now(),
            span,
        }
    }

    pub(super) fn record_principal(&mut self, principal: Option<&AuthenticatedPrincipal>) {
        if let Some(principal) = principal {
            self.context.set_authenticated_principal(principal.clone());
            self.span.record("principal", principal.username());
        }
    }

    pub(super) fn context(&self) -> &ProxyContext {
        &self.context
    }

    pub(super) fn span(&self) -> Span {
        self.span.clone()
    }

    pub(super) fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}

pub(super) struct TelemetryStreamState<P> {
    pub(super) service: ProxyGrpcService<P>,
    pub(super) context: ProxyContext,
    pub(super) principal: Option<AuthenticatedPrincipal>,
    pub(super) client_id: String,
    pub(super) _permit: ResourcePermit,
    pub(super) outbound: BudgetedQueue<v2::TelemetryCommand>,
    pub(super) inbound: Streaming<v2::TelemetryCommand>,
    pub(super) done: bool,
}

impl<P> Drop for TelemetryStreamState<P> {
    fn drop(&mut self) {
        self.service.sessions.unbind_telemetry_link(self.client_id.as_str());
    }
}
