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

use std::future::ready;
use std::future::Future;

use bytes::Bytes;
use rocketmq_error::STORAGE_UNAVAILABLE;
use rocketmq_model::message::MessageQueue;
use rocketmq_store_api::AdminStore;
use rocketmq_store_api::DerivedRecordSink;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::OffsetIndex;
use rocketmq_store_api::ReplicationControl;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreLifecycle;
use rocketmq_store_api::StoreOperation;

#[derive(Default)]
struct ContractStore;

impl StoreLifecycle for ContractStore {
    async fn load(&mut self) -> Result<bool, StoreError> {
        ready(Ok(true)).await
    }

    async fn start(&mut self) -> Result<(), StoreError> {
        ready(Ok(())).await
    }

    async fn shutdown(&mut self) -> Result<(), StoreError> {
        ready(Ok(())).await
    }
}

impl MessageAppender<Bytes> for ContractStore {
    type Receipt = usize;

    fn append_message(&mut self, message: Bytes) -> impl Future<Output = Result<Self::Receipt, StoreError>> + Send {
        ready(Ok(message.len()))
    }
}

impl MessageReader for ContractStore {
    type Request = MessageQueue;
    type Output = Option<Bytes>;

    fn read(&self, _request: Self::Request) -> impl Future<Output = Result<Self::Output, StoreError>> + Send {
        ready(Ok(None))
    }
}

impl OffsetIndex for ContractStore {
    type Query = MessageQueue;
    type Output = (i64, i64);

    fn query_offset(&self, _query: &Self::Query) -> Result<Self::Output, StoreError> {
        Ok((0, 1))
    }
}

impl StoreHealth for ContractStore {
    type Snapshot = bool;

    fn health_snapshot(&self) -> Self::Snapshot {
        true
    }
}

impl ReplicationControl for ContractStore {
    type Command = i64;
    type State = i64;

    fn replication_state(&self) -> Self::State {
        0
    }

    fn apply_replication(
        &mut self,
        command: Self::Command,
    ) -> impl Future<Output = Result<Self::State, StoreError>> + Send {
        ready(Ok(command))
    }
}

impl DerivedRecordSink for ContractStore {
    type Record = Bytes;
    type Progress = usize;

    fn append_derived(
        &mut self,
        record: Self::Record,
    ) -> impl Future<Output = Result<Self::Progress, StoreError>> + Send {
        ready(Ok(record.len()))
    }
}

impl AdminStore for ContractStore {
    type Request = ();
    type Response = ();

    fn execute_admin(&mut self, (): Self::Request) -> impl Future<Output = Result<Self::Response, StoreError>> + Send {
        ready(Ok(()))
    }
}

fn assert_capabilities<T>()
where
    T: StoreLifecycle
        + MessageAppender<Bytes>
        + MessageReader
        + OffsetIndex
        + StoreHealth
        + ReplicationControl
        + DerivedRecordSink
        + AdminStore,
{
}

fn assert_static_append_future<T>(store: &mut T) -> impl Future<Output = Result<usize, StoreError>> + Send + '_
where
    T: MessageAppender<Bytes, Receipt = usize>,
{
    store.append_message(Bytes::from_static(b"message"))
}

fn require_send<F: Send>(_: F) {}

fn assert_lifecycle_futures_are_send<S: StoreLifecycle>(store: &mut S) {
    require_send(store.load());
    require_send(store.start());
    require_send(store.shutdown());
}

fn assert_replication_futures_are_send<R: ReplicationControl>(store: &mut R, command: R::Command) {
    let _: R::State = store.replication_state();
    require_send(store.apply_replication(command));
}

fn assert_derived_futures_are_send<S: DerivedRecordSink>(sink: &mut S, record: S::Record) {
    require_send(sink.append_derived(record));
}

fn assert_admin_futures_are_send<S: AdminStore>(store: &mut S, request: S::Request) {
    require_send(store.execute_admin(request));
}

#[test]
fn capabilities_use_store_error_with_static_append_future() {
    assert_capabilities::<ContractStore>();
    let mut store = ContractStore;
    assert_lifecycle_futures_are_send(&mut store);
    assert_replication_futures_are_send(&mut store, 0i64);
    assert_derived_futures_are_send(&mut store, Bytes::from_static(b"derived"));
    assert_admin_futures_are_send(&mut store, ());
    let _future = assert_static_append_future(&mut store);
}

#[test]
fn store_error_uses_catalog_identity_and_closed_operation_vocabulary() {
    let error = StoreError::new(&STORAGE_UNAVAILABLE, StoreOperation::Append);

    assert_eq!(&STORAGE_UNAVAILABLE, error.descriptor());
    assert_eq!(StoreOperation::Append, error.operation());
    assert_eq!("storage.unavailable: Storage is unavailable", error.to_string());
}
