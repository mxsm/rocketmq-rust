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
use rocketmq_model::message::MessageQueue;
use rocketmq_store_api::AdminStore;
use rocketmq_store_api::DerivedRecordSink;
use rocketmq_store_api::MessageAppender;
use rocketmq_store_api::MessageReader;
use rocketmq_store_api::OffsetIndex;
use rocketmq_store_api::ReplicationControl;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreHealth;
use rocketmq_store_api::StoreLifecycle;
use rocketmq_store_api::StoreOperation;

#[derive(Default)]
struct ContractStore;

impl StoreLifecycle for ContractStore {
    type Error = StoreError;

    fn load(&mut self) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        ready(Ok(true))
    }

    fn start(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn shutdown(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }
}

impl MessageAppender<Bytes> for ContractStore {
    type Receipt = usize;
    type Error = StoreError;

    fn append_message(&mut self, message: Bytes) -> impl Future<Output = Result<Self::Receipt, Self::Error>> + Send {
        ready(Ok(message.len()))
    }
}

impl MessageReader for ContractStore {
    type Request = MessageQueue;
    type Output = Option<Bytes>;
    type Error = StoreError;

    fn read(&self, _request: Self::Request) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send {
        ready(Ok(None))
    }
}

impl OffsetIndex for ContractStore {
    type Query = MessageQueue;
    type Output = (i64, i64);
    type Error = StoreError;

    fn query_offset(&self, _query: &Self::Query) -> Result<Self::Output, Self::Error> {
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
    type Error = StoreError;

    fn replication_state(&self) -> Self::State {
        0
    }

    fn apply_replication(
        &mut self,
        command: Self::Command,
    ) -> impl Future<Output = Result<Self::State, Self::Error>> + Send {
        ready(Ok(command))
    }
}

impl DerivedRecordSink for ContractStore {
    type Record = Bytes;
    type Progress = usize;
    type Error = StoreError;

    fn append_derived(
        &mut self,
        record: Self::Record,
    ) -> impl Future<Output = Result<Self::Progress, Self::Error>> + Send {
        ready(Ok(record.len()))
    }
}

impl AdminStore for ContractStore {
    type Request = ();
    type Response = ();
    type Error = StoreError;

    fn execute_admin(&mut self, (): Self::Request) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send {
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
    T: MessageAppender<Bytes, Receipt = usize, Error = StoreError>,
{
    store.append_message(Bytes::from_static(b"message"))
}

#[test]
fn capabilities_are_associated_type_contracts_with_static_append_future() {
    assert_capabilities::<ContractStore>();
    let mut store = ContractStore;
    let _future = assert_static_append_future(&mut store);
}

#[test]
fn store_error_uses_closed_operation_and_neutral_kind_vocabularies() {
    let error = StoreError::new(StoreErrorKind::Unavailable, StoreOperation::Append);

    assert_eq!(StoreErrorKind::Unavailable, error.kind());
    assert_eq!(StoreOperation::Append, error.operation());
    assert_eq!("store operation append failed: unavailable", error.to_string());
}
