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

//! Deterministic Message responses and admitted-operation gates.

use super::{MessageBackend, MessageRequestScope};
use crate::{
    services::ServiceFuture,
    state::{UiError, UiErrorCode},
};
use rocketmq_dashboard_common::{
    MessageConsumeCommand, MessageConsumeReceipt, MessageIdentity, MessagePayload, MessageSearchPage,
    MessageSearchQuery,
};
use std::{collections::VecDeque, sync::Mutex};

type ResultQueue<T> = Mutex<VecDeque<Result<T, UiError>>>;
type Gate = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
);
#[derive(Clone, Debug, Default)]
pub(crate) struct MessageCalls {
    pub query: Vec<(MessageRequestScope, MessageSearchQuery)>,
    pub payload: Vec<(MessageRequestScope, MessageIdentity)>,
    pub consume: Vec<(MessageRequestScope, MessageConsumeCommand)>,
}
#[derive(Default)]
pub(crate) struct FakeMessageBackend {
    calls: Mutex<MessageCalls>,
    queries: ResultQueue<MessageSearchPage>,
    payloads: ResultQueue<MessagePayload>,
    consumes: ResultQueue<MessageConsumeReceipt>,
    query_gate: Mutex<Option<Gate>>,
    consume_gate: Mutex<Option<Gate>>,
}
impl FakeMessageBackend {
    pub fn calls(&self) -> MessageCalls {
        self.calls.lock().unwrap().clone()
    }
    pub fn queue_query(&self, result: Result<MessageSearchPage, UiError>) {
        self.queries.lock().unwrap().push_back(result);
    }
    pub fn queue_payload(&self, result: Result<MessagePayload, UiError>) {
        self.payloads.lock().unwrap().push_back(result);
    }
    pub fn queue_consume(&self, result: Result<MessageConsumeReceipt, UiError>) {
        self.consumes.lock().unwrap().push_back(result);
    }
    pub fn hold_next_query(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        hold(&self.query_gate)
    }
    pub fn hold_next_consume(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        hold(&self.consume_gate)
    }
}
fn hold(
    slot: &Mutex<Option<Gate>>,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (started, wait_started) = tokio::sync::oneshot::channel();
    let (release, wait_release) = tokio::sync::oneshot::channel();
    let (finished, wait_finished) = tokio::sync::oneshot::channel();
    *slot.lock().unwrap() = Some((started, wait_release, finished));
    (wait_started, release, wait_finished)
}
struct Finished(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for Finished {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
async fn wait(gate: Option<Gate>) {
    if let Some((started, release, finished)) = gate {
        let _finished = Finished(Some(finished));
        let _ = started.send(());
        let _ = release.await;
    }
}
fn pop<T>(queue: &ResultQueue<T>) -> Result<T, UiError> {
    queue.lock().unwrap().pop_front().unwrap_or_else(|| {
        Err(UiError::new(
            "Unexpected Message test call.",
            UiErrorCode::Unknown,
            false,
        ))
    })
}
impl MessageBackend for FakeMessageBackend {
    fn query(
        &self,
        scope: MessageRequestScope,
        query: MessageSearchQuery,
    ) -> ServiceFuture<'_, Result<MessageSearchPage, UiError>> {
        self.calls.lock().unwrap().query.push((scope, query));
        let gate = self.query_gate.lock().unwrap().take();
        let result = pop(&self.queries);
        Box::pin(async move {
            wait(gate).await;
            result
        })
    }
    fn payload(
        &self,
        scope: MessageRequestScope,
        identity: MessageIdentity,
    ) -> ServiceFuture<'_, Result<MessagePayload, UiError>> {
        self.calls.lock().unwrap().payload.push((scope, identity));
        Box::pin(std::future::ready(pop(&self.payloads)))
    }
    fn consume(
        &self,
        scope: MessageRequestScope,
        command: MessageConsumeCommand,
    ) -> ServiceFuture<'_, Result<MessageConsumeReceipt, UiError>> {
        self.calls.lock().unwrap().consume.push((scope, command));
        let gate = self.consume_gate.lock().unwrap().take();
        let result = pop(&self.consumes);
        Box::pin(async move {
            wait(gate).await;
            result
        })
    }
}

pub(crate) fn row() -> rocketmq_dashboard_common::MessageMetadataRow {
    rocketmq_dashboard_common::MessageMetadataRow {
        identity: MessageIdentity {
            topic: "orders".into(),
            message_id: "7F00000100002A9F0000000000000001".into(),
        },
        tags: Some(rocketmq_dashboard_common::MessageTextPreview::bounded("tag-a")),
        keys: Some(rocketmq_dashboard_common::MessageTextPreview::bounded("key-a")),
        born_host: rocketmq_dashboard_common::MessageTextPreview::bounded("127.0.0.1:12345"),
        store_host: rocketmq_dashboard_common::MessageTextPreview::bounded("127.0.0.1:10911"),
        born_timestamp: 100,
        store_timestamp: 200,
        queue_id: 2,
        queue_offset: 33,
        store_size: 100,
        reconsume_times: 0,
        sys_flag: 0,
        flag: 0,
        prepared_transaction_offset: 0,
    }
}
pub(crate) fn page() -> MessageSearchPage {
    MessageSearchPage {
        rows: vec![row()],
        extent: rocketmq_dashboard_common::MessagePageExtent::MoreMayExist,
    }
}
