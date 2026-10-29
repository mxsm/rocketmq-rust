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

//! Deterministic safe-DTO fake for Consumer and Producer product paths.

use std::{collections::VecDeque, sync::Mutex};

use rocketmq_dashboard_common::{
    ConsumerClients, ConsumerConfigPatchCommand, ConsumerConfigPatchOutcome, ConsumerConfiguration,
    ConsumerCreateCommand, ConsumerDeleteCommand, ConsumerDiagnosticPayload, ConsumerDiagnosticRequest,
    ConsumerIdentity, ConsumerInventory, ConsumerObservation, ConsumerPartialOutcome, ConsumerProgress,
    ProducerConnectionQuery, ProducerConnections, ProducerInventory,
};

use super::{ConsumerBackend, ConsumerRequestScope};
use crate::{
    services::ServiceFuture,
    state::{UiError, UiErrorCode},
};

#[derive(Clone, Default)]
pub(crate) struct ConsumerCalls {
    pub inventory: Vec<ConsumerRequestScope>,
    pub clients: Vec<(ConsumerRequestScope, ConsumerIdentity)>,
    pub progress: Vec<(ConsumerRequestScope, ConsumerIdentity)>,
    pub configuration: Vec<(ConsumerRequestScope, ConsumerIdentity)>,
    pub diagnostic: Vec<(ConsumerRequestScope, ConsumerDiagnosticRequest)>,
    pub producer_inventory: Vec<ConsumerRequestScope>,
    pub producer_connections: Vec<(ConsumerRequestScope, ProducerConnectionQuery)>,
    pub create: Vec<(ConsumerRequestScope, ConsumerCreateCommand)>,
    pub patch: Vec<(ConsumerRequestScope, ConsumerConfigPatchCommand)>,
    pub delete: Vec<(ConsumerRequestScope, ConsumerDeleteCommand)>,
}

#[derive(Default)]
struct Queues {
    inventory: VecDeque<Result<ConsumerInventory, UiError>>,
    clients: VecDeque<Result<ConsumerObservation<ConsumerClients>, UiError>>,
    progress: VecDeque<Result<ConsumerObservation<ConsumerProgress>, UiError>>,
    configuration: VecDeque<Result<ConsumerConfiguration, UiError>>,
    diagnostic: VecDeque<Result<ConsumerDiagnosticPayload, UiError>>,
    producer_inventory: VecDeque<Result<ProducerInventory, UiError>>,
    producer_connections: VecDeque<Result<ConsumerObservation<ProducerConnections>, UiError>>,
    create: VecDeque<Result<ConsumerPartialOutcome, UiError>>,
    patch: VecDeque<Result<ConsumerConfigPatchOutcome, UiError>>,
    delete: VecDeque<Result<ConsumerPartialOutcome, UiError>>,
}

#[derive(Default)]
pub(crate) struct FakeConsumerBackend {
    queues: Mutex<Queues>,
    calls: Mutex<ConsumerCalls>,
    next_create_completion: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    next_inventory_completion: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    next_producer_completion: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    next_patch_result: Mutex<Option<tokio::sync::oneshot::Receiver<Result<ConsumerConfigPatchOutcome, UiError>>>>,
    next_patch_started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

macro_rules! queue_method {
    ($name:ident, $field:ident, $value:ty) => {
        pub fn $name(&self, result: Result<$value, UiError>) {
            self.queues
                .lock()
                .expect("Consumer fake queues")
                .$field
                .push_back(result);
        }
    };
}

impl FakeConsumerBackend {
    pub fn observe_next_patch(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *self.next_patch_started.lock().expect("patch started") = Some(sender);
        receiver
    }
    pub fn queue_pending_patch(&self) -> tokio::sync::oneshot::Sender<Result<ConsumerConfigPatchOutcome, UiError>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *self.next_patch_result.lock().expect("patch result") = Some(receiver);
        sender
    }
    pub fn hold_next_inventory(&self) -> tokio::sync::oneshot::Sender<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *self.next_inventory_completion.lock().expect("inventory completion") = Some(receiver);
        sender
    }
    pub fn hold_next_producer_connections(&self) -> tokio::sync::oneshot::Sender<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *self.next_producer_completion.lock().expect("Producer completion") = Some(receiver);
        sender
    }

    pub fn hold_next_create(&self) -> tokio::sync::oneshot::Sender<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *self.next_create_completion.lock().expect("Consumer create completion") = Some(receiver);
        sender
    }

    queue_method!(queue_inventory, inventory, ConsumerInventory);
    queue_method!(queue_clients, clients, ConsumerObservation<ConsumerClients>);
    queue_method!(queue_progress, progress, ConsumerObservation<ConsumerProgress>);
    queue_method!(queue_configuration, configuration, ConsumerConfiguration);
    queue_method!(queue_diagnostic, diagnostic, ConsumerDiagnosticPayload);
    queue_method!(queue_producer_inventory, producer_inventory, ProducerInventory);
    queue_method!(
        queue_producer_connections,
        producer_connections,
        ConsumerObservation<ProducerConnections>
    );
    queue_method!(queue_create, create, ConsumerPartialOutcome);
    queue_method!(queue_patch, patch, ConsumerConfigPatchOutcome);
    queue_method!(queue_delete, delete, ConsumerPartialOutcome);

    pub fn calls(&self) -> ConsumerCalls {
        self.calls.lock().expect("Consumer fake calls").clone()
    }
}

macro_rules! pop_result {
    ($self:ident, $field:ident, $operation:literal) => {
        $self
            .queues
            .lock()
            .expect("Consumer fake queues")
            .$field
            .pop_front()
            .unwrap_or_else(|| Err(unexpected_call($operation)))
    };
}

impl ConsumerBackend for FakeConsumerBackend {
    fn inventory(&self, scope: ConsumerRequestScope) -> ServiceFuture<'_, Result<ConsumerInventory, UiError>> {
        self.calls.lock().expect("Consumer fake calls").inventory.push(scope);
        let result = pop_result!(self, inventory, "inventory");
        let completion = self
            .next_inventory_completion
            .lock()
            .expect("inventory completion")
            .take();
        Box::pin(async move {
            if let Some(completion) = completion {
                let _ = completion.await;
            }
            result
        })
    }

    fn clients(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ConsumerClients>, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .clients
            .push((scope, group));
        Box::pin(std::future::ready(pop_result!(self, clients, "clients")))
    }

    fn progress(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ConsumerProgress>, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .progress
            .push((scope, group));
        Box::pin(std::future::ready(pop_result!(self, progress, "progress")))
    }

    fn configuration(
        &self,
        scope: ConsumerRequestScope,
        group: ConsumerIdentity,
    ) -> ServiceFuture<'_, Result<ConsumerConfiguration, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .configuration
            .push((scope, group));
        Box::pin(std::future::ready(pop_result!(self, configuration, "configuration")))
    }

    fn diagnostic(
        &self,
        scope: ConsumerRequestScope,
        request: ConsumerDiagnosticRequest,
    ) -> ServiceFuture<'_, Result<ConsumerDiagnosticPayload, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .diagnostic
            .push((scope, request));
        Box::pin(std::future::ready(pop_result!(self, diagnostic, "diagnostic")))
    }

    fn producer_inventory(&self, scope: ConsumerRequestScope) -> ServiceFuture<'_, Result<ProducerInventory, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .producer_inventory
            .push(scope);
        Box::pin(std::future::ready(pop_result!(
            self,
            producer_inventory,
            "Producer inventory"
        )))
    }

    fn producer_connections(
        &self,
        scope: ConsumerRequestScope,
        query: ProducerConnectionQuery,
    ) -> ServiceFuture<'_, Result<ConsumerObservation<ProducerConnections>, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .producer_connections
            .push((scope, query));
        let result = pop_result!(self, producer_connections, "Producer connections");
        let completion = self
            .next_producer_completion
            .lock()
            .expect("Producer completion")
            .take();
        Box::pin(async move {
            if let Some(completion) = completion {
                let _ = completion.await;
            }
            result
        })
    }

    fn create(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerCreateCommand,
    ) -> ServiceFuture<'_, Result<ConsumerPartialOutcome, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .create
            .push((scope, command));
        let completion = self
            .next_create_completion
            .lock()
            .expect("Consumer create completion")
            .take();
        let result = pop_result!(self, create, "create");
        Box::pin(async move {
            if let Some(completion) = completion {
                completion
                    .await
                    .map_err(|_| unexpected_call("cancelled create completion"))?;
            }
            result
        })
    }

    fn patch_configuration(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerConfigPatchCommand,
    ) -> ServiceFuture<'_, Result<ConsumerConfigPatchOutcome, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .patch
            .push((scope, command));
        if let Some(started) = self.next_patch_started.lock().expect("patch started").take() {
            let _ = started.send(());
        }
        let pending = self.next_patch_result.lock().expect("patch result").take();
        let ready = if pending.is_none() {
            Some(pop_result!(self, patch, "configuration patch"))
        } else {
            None
        };
        Box::pin(async move {
            match pending {
                Some(pending) => pending.await.map_err(|_| unexpected_call("cancelled patch"))?,
                None => ready.unwrap_or_else(|| Err(unexpected_call("missing patch"))),
            }
        })
    }

    fn delete(
        &self,
        scope: ConsumerRequestScope,
        command: ConsumerDeleteCommand,
    ) -> ServiceFuture<'_, Result<ConsumerPartialOutcome, UiError>> {
        self.calls
            .lock()
            .expect("Consumer fake calls")
            .delete
            .push((scope, command));
        Box::pin(std::future::ready(pop_result!(self, delete, "delete")))
    }
}

fn unexpected_call(_operation: &str) -> UiError {
    UiError::new(
        "Unexpected Consumer test call.",
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}
