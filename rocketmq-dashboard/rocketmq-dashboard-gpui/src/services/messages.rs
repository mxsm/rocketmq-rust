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

//! Cancellable Message reads and application-owned, audited direct consumption.

#[cfg(test)]
#[path = "messages_test_support.rs"]
pub(crate) mod test_support;
#[cfg(test)]
#[path = "messages_tests.rs"]
mod tests;

use super::{AppServices, ServiceFuture, audit::MutationAudit};
use crate::{
    infrastructure::admin_provider::{GpuiAdminProvider, ProviderFailure},
    state::{UiError, UiErrorCode},
};
use rocketmq_dashboard_common::{
    AuditAction, AuditFacts, AuditOutcome, ConnectionScope, MessageConsumeCode, MessageConsumeCommand,
    MessageConsumeReceipt, MessageIdentity, MessagePayload, MessageSearchPage, MessageSearchQuery,
    MessageValidationError, OperationResource,
};
use std::{future::Future, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MessageRequestScope {
    pub revision: u64,
    pub epoch: u64,
}

pub(crate) trait MessageBackend: Send + Sync {
    fn query(
        &self,
        scope: MessageRequestScope,
        query: MessageSearchQuery,
    ) -> ServiceFuture<'_, Result<MessageSearchPage, UiError>>;
    fn payload(
        &self,
        scope: MessageRequestScope,
        identity: MessageIdentity,
    ) -> ServiceFuture<'_, Result<MessagePayload, UiError>>;
    fn consume(
        &self,
        scope: MessageRequestScope,
        command: MessageConsumeCommand,
    ) -> ServiceFuture<'_, Result<MessageConsumeReceipt, UiError>>;
}

pub(super) struct RealMessageBackend {
    provider: Option<Arc<GpuiAdminProvider>>,
}
impl RealMessageBackend {
    pub(super) fn new(provider: Arc<GpuiAdminProvider>) -> Arc<Self> {
        Arc::new(Self {
            provider: Some(provider),
        })
    }
    pub(super) fn unavailable() -> Arc<Self> {
        Arc::new(Self { provider: None })
    }
    fn provider(&self) -> Result<&Arc<GpuiAdminProvider>, UiError> {
        self.provider.as_ref().ok_or_else(|| {
            UiError::new(
                "Message operations are unavailable in this application configuration.",
                UiErrorCode::CapabilityUnavailable,
                false,
            )
        })
    }
}

impl MessageBackend for RealMessageBackend {
    fn query(
        &self,
        scope: MessageRequestScope,
        query: MessageSearchQuery,
    ) -> ServiceFuture<'_, Result<MessageSearchPage, UiError>> {
        Box::pin(async move {
            self.provider()?
                .query_messages(scope.revision, query)
                .await
                .map_err(query_error)
        })
    }
    fn payload(
        &self,
        scope: MessageRequestScope,
        identity: MessageIdentity,
    ) -> ServiceFuture<'_, Result<MessagePayload, UiError>> {
        Box::pin(async move {
            self.provider()?
                .message_payload(scope.revision, identity)
                .await
                .map_err(query_error)
        })
    }
    fn consume(
        &self,
        scope: MessageRequestScope,
        command: MessageConsumeCommand,
    ) -> ServiceFuture<'_, Result<MessageConsumeReceipt, UiError>> {
        Box::pin(async move {
            self.provider()?
                .consume_message(scope.revision, command)
                .await
                .map_err(consume_error)
        })
    }
}

impl AppServices {
    pub(crate) async fn query_messages(
        &self,
        scope: MessageRequestScope,
        query: MessageSearchQuery,
    ) -> Result<MessageSearchPage, UiError> {
        query.validate().map_err(validation_error)?;
        self.require_message_scope()?;
        let backend = Arc::clone(&self.messages);
        self.run_message_query("gpui-service-message-query", async move {
            backend.query(scope, query).await
        })
        .await
    }
    pub(crate) async fn message_payload(
        &self,
        scope: MessageRequestScope,
        identity: MessageIdentity,
    ) -> Result<MessagePayload, UiError> {
        MessageSearchQuery::MessageId {
            topic: identity.topic.clone(),
            message_id: identity.message_id.clone(),
        }
        .validate()
        .map_err(validation_error)?;
        self.require_message_scope()?;
        let backend = Arc::clone(&self.messages);
        self.run_message_query("gpui-service-message-detail", async move {
            backend.payload(scope, identity).await
        })
        .await
    }
    pub(crate) async fn consume_message(
        &self,
        scope: MessageRequestScope,
        command: MessageConsumeCommand,
    ) -> Result<MessageConsumeReceipt, UiError> {
        command.validate().map_err(validation_error)?;
        self.require_message_scope()?;
        let operation = self.accept_operation(
            scope.revision,
            AuditAction::MessageConsumed,
            OperationResource::Message(command.identity.message_id.clone()),
        )?;
        let backend = Arc::clone(&self.messages);
        self.run_mutation("gpui-service-message-consume", operation, async move {
            backend.consume(scope, command).await
        })
        .await
    }
    fn require_message_scope(&self) -> Result<(), UiError> {
        if self.connection_state().config.scope == ConnectionScope::NameServer {
            Ok(())
        } else {
            Err(UiError::new(
                MessageValidationError::Scope.summary(),
                UiErrorCode::CapabilityUnavailable,
                false,
            ))
        }
    }
    async fn run_message_query<T: Send + 'static>(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<T, UiError>> + Send + 'static,
    ) -> Result<T, UiError> {
        match &self.runtime_bridge {
            Some(runtime) => runtime.run(name, future).await,
            None => future.await,
        }
    }
    #[cfg(test)]
    pub(crate) fn with_message_backend(mut self, backend: Arc<dyn MessageBackend>) -> Self {
        self.messages = backend;
        self
    }
}

impl MutationAudit for MessageConsumeReceipt {
    fn audit(&self) -> (AuditOutcome, AuditFacts) {
        if self.is_success() {
            return (
                AuditOutcome::Succeeded,
                AuditFacts {
                    applied_targets: 1,
                    ..Default::default()
                },
            );
        }
        let outcome = match self.code {
            MessageConsumeCode::Unknown | MessageConsumeCode::Success => AuditOutcome::Unknown,
            MessageConsumeCode::Later
            | MessageConsumeCode::Rollback
            | MessageConsumeCode::Commit
            | MessageConsumeCode::ThrewException
            | MessageConsumeCode::ReturnedNull => AuditOutcome::Failed,
        };
        (
            outcome,
            AuditFacts {
                unconfirmed_targets: u32::from(outcome == AuditOutcome::Unknown),
                ..Default::default()
            },
        )
    }
}

pub(crate) fn validation_error(error: MessageValidationError) -> UiError {
    UiError::new(error.summary(), UiErrorCode::Validation, false)
}
fn query_error(error: ProviderFailure) -> UiError {
    let retryable = error.is_retryable();
    let summary = "Unable to read messages from the selected connection.";
    match error.into_operational() {
        Some(source) => UiError::caused_by(summary, UiErrorCode::Connection, retryable, source),
        None => UiError::new(summary, UiErrorCode::Connection, retryable),
    }
}

fn consume_error(error: ProviderFailure) -> UiError {
    if error.is_invalid_request() {
        return UiError::new(
            "Direct consume was rejected before execution. Verify the Message ID, Topic and Consumer target.",
            UiErrorCode::Validation,
            false,
        );
    }
    let summary = "Direct consume is unconfirmed. It may have reached the Consumer; verify before retrying.";
    match error.into_operational() {
        Some(source) => UiError::caused_by(summary, UiErrorCode::Connection, false, source),
        None => UiError::new(summary, UiErrorCode::Connection, false),
    }
}
