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

use super::*;
use rocketmq_dashboard_common::{
    ConsumerCapabilities, ConsumerCategory, ConsumerGroupObservation, ConsumerIdentity, ConsumerObservation,
    ConsumerObservationState, ConsumerUnknownReason, DlqRow, MessagePageExtent,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};
type Gate = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
);
#[derive(Clone, Debug, Default)]
pub(crate) struct Calls {
    pub queries: Vec<DlqQuery>,
    pub consumes: Vec<(MessageRequestScope, DlqResendTarget)>,
    pub exports: Vec<(DlqQuery, Vec<MessageIdentity>)>,
}
#[derive(Default)]
pub(crate) struct FakeDlqBackend {
    calls: Mutex<Calls>,
    pub queries: Mutex<VecDeque<Result<DlqPage, UiError>>>,
    pub consumes: Mutex<VecDeque<DlqAttempt>>,
    pub exports: Mutex<VecDeque<Result<DlqCsv, UiError>>>,
    consume_gate: Mutex<Option<Gate>>,
    query_gate: Mutex<Option<Gate>>,
}
impl FakeDlqBackend {
    pub fn calls(&self) -> Calls {
        self.calls.lock().unwrap().clone()
    }
    pub fn hold_consume(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        hold(&self.consume_gate)
    }
    pub fn hold_query(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        hold(&self.query_gate)
    }
}
fn hold(
    slot: &Mutex<Option<Gate>>,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
) {
    let (start, started) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let (finish, finished) = tokio::sync::oneshot::channel();
    *slot.lock().unwrap() = Some((start, wait, finish));
    (started, release, finished)
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
    if let Some((start, release, finish)) = gate {
        let _finished = Finished(Some(finish));
        let _ = start.send(());
        let _ = release.await;
    }
}
impl DlqBackend for FakeDlqBackend {
    fn query(&self, _: MessageRequestScope, query: DlqQuery) -> ServiceFuture<'_, Result<DlqPage, UiError>> {
        self.calls.lock().unwrap().queries.push(query);
        let result = self.queries.lock().unwrap().pop_front().expect("DLQ query response");
        let gate = self.query_gate.lock().unwrap().take();
        Box::pin(async move {
            wait(gate).await;
            result
        })
    }
    fn consume(&self, scope: MessageRequestScope, target: DlqResendTarget) -> ServiceFuture<'_, DlqAttempt> {
        self.calls.lock().unwrap().consumes.push((scope, target));
        let result = self.consumes.lock().unwrap().pop_front().expect("DLQ consume response");
        let gate = self.consume_gate.lock().unwrap().take();
        Box::pin(async move {
            wait(gate).await;
            result
        })
    }
    fn csv(
        &self,
        _: MessageRequestScope,
        query: DlqQuery,
        expected: Vec<MessageIdentity>,
    ) -> ServiceFuture<'_, Result<DlqCsv, UiError>> {
        self.calls.lock().unwrap().exports.push((query, expected));
        let result = self.exports.lock().unwrap().pop_front().expect("DLQ export response");
        Box::pin(async move { result })
    }
}
pub(crate) fn query() -> DlqQuery {
    DlqQuery {
        consumer_group: "group-a".into(),
        begin: 1,
        end: 1000,
        message_id: None,
        page: 1,
        page_size: 20,
    }
}
pub(crate) fn group_inventory() -> ConsumerInventory {
    ConsumerInventory {
        groups: vec![ConsumerGroupObservation {
            identity: ConsumerIdentity::parse("group-a").unwrap(),
            category: ConsumerCategory::Application,
            connection_state: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::NotRequested,
            },
            client_count: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::NotRequested,
            },
            lag: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::NotRequested,
            },
            consume_type: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::NotRequested,
            },
            message_model: ConsumerObservation::Unknown {
                reason: ConsumerUnknownReason::NotRequested,
            },
            targets: Vec::new(),
        }],
        targets: Vec::new(),
        target_observation: ConsumerObservationState::Complete,
        group_observation: ConsumerObservationState::Complete,
        observation: ConsumerObservationState::Complete,
        failures: Vec::new(),
        capabilities: ConsumerCapabilities::for_scope(ConnectionScope::NameServer),
    }
}
pub(crate) fn row(index: u32) -> DlqRow {
    let mut metadata = crate::services::messages::test_support::row();
    metadata.identity.topic = "%DLQ%group-a".into();
    metadata.identity.message_id = format!("7F00000100002A9F{index:016X}");
    let target = DlqResendTarget::resolve(
        "group-a",
        metadata.identity.clone(),
        &BTreeMap::from([
            ("RETRY_TOPIC".into(), "orders".into()),
            ("ORIGIN_MESSAGE_ID".into(), format!("original-{index}")),
        ]),
    );
    DlqRow { metadata, target }
}
pub(crate) fn page() -> DlqPage {
    DlqPage {
        rows: vec![row(1), row(2)],
        extent: MessagePageExtent::MoreMayExist,
    }
}
pub(crate) fn attempt(state: DlqEntryState) -> DlqAttempt {
    DlqAttempt { state, receipt: None }
}
pub(crate) fn csv(body: String) -> DlqCsv {
    DlqCsv::from_rows(vec![rocketmq_dashboard_common::DlqCsvRow {
        message_id: "id".into(),
        topic: "%DLQ%group-a".into(),
        keys: "key".into(),
        tags: "tag".into(),
        store_timestamp: 200,
        queue_id: 0,
        queue_offset: 1,
        body,
    }])
    .unwrap()
}
