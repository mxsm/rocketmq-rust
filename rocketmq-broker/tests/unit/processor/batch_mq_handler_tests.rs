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

//! Regression tests for [`BatchMqHandler`]: a malformed `LOCK_BATCH_MQ` or
//! `UNLOCK_BATCH_MQ` request body must produce a typed error rather than
//! panicking the Broker. See issue #10658.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_protocol::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_store::MessageStoreConfig;

use super::batch_mq_handler::BatchMqHandler;
use crate::broker_runtime::BrokerRuntime;
use crate::config::broker_config::BrokerConfig;

fn temp_test_root(label: &str) -> std::path::PathBuf {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("time should move forward")
        .as_millis();
    std::env::temp_dir().join(format!("rocketmq-rust-batch-mq-{label}-{millis}"))
}

async fn new_test_runtime(label: &str) -> BrokerRuntime {
    let temp_root = temp_test_root(label);
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize().await.is_ok());
    runtime
}

fn cleanup(runtime: &BrokerRuntime) {
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

/// An empty `LOCK_BATCH_MQ` body must yield an error, not a panic.
#[tokio::test]
async fn lock_batch_mq_empty_body_returns_error_without_panic() {
    let runtime = new_test_runtime("lock-empty").await;
    let admin = runtime.admin_runtime_for_test();
    let handler = BatchMqHandler::new();

    let mut request = RemotingCommand::create_request_command(RequestCode::LockBatchMq, EmptyHeader {});
    let result = handler
        .lock_batch_mq(&admin, RequestCode::LockBatchMq, &mut request)
        .await;

    assert!(result.is_err(), "empty lockBatchMQ body must return a typed error");
    cleanup(&runtime);
}

/// An empty `UNLOCK_BATCH_MQ` body must yield an error, not a panic.
#[tokio::test]
async fn unlock_batch_mq_empty_body_returns_error_without_panic() {
    let runtime = new_test_runtime("unlock-empty").await;
    let admin = runtime.admin_runtime_for_test();
    let handler = BatchMqHandler::new();

    let mut request = RemotingCommand::create_request_command(RequestCode::UnlockBatchMq, EmptyHeader {});
    let result = handler
        .unlock_batch_mq(&admin, RequestCode::UnlockBatchMq, &mut request)
        .await;

    assert!(result.is_err(), "empty unlockBatchMQ body must return a typed error");
    cleanup(&runtime);
}

/// A corrupt (non-decodable) `LOCK_BATCH_MQ` body must yield an error, not a panic.
#[tokio::test]
async fn lock_batch_mq_corrupt_body_returns_error() {
    let runtime = new_test_runtime("lock-corrupt").await;
    let admin = runtime.admin_runtime_for_test();
    let handler = BatchMqHandler::new();

    let mut request = RemotingCommand::create_request_command(RequestCode::LockBatchMq, EmptyHeader {})
        .set_body(Bytes::from_static(b"not a valid request body"));
    let result = handler
        .lock_batch_mq(&admin, RequestCode::LockBatchMq, &mut request)
        .await;

    let Err(error) = result else {
        panic!("corrupt lockBatchMQ body must return a typed error");
    };
    assert!(
        std::error::Error::source(error.as_ref()).is_some(),
        "lockBatchMQ decode error must retain its typed source"
    );
    cleanup(&runtime);
}

/// A corrupt (non-decodable) `UNLOCK_BATCH_MQ` body must yield an error, not a panic.
#[tokio::test]
async fn unlock_batch_mq_corrupt_body_returns_error() {
    let runtime = new_test_runtime("unlock-corrupt").await;
    let admin = runtime.admin_runtime_for_test();
    let handler = BatchMqHandler::new();

    let mut request = RemotingCommand::create_request_command(RequestCode::UnlockBatchMq, EmptyHeader {})
        .set_body(Bytes::from_static(b"not a valid request body"));
    let result = handler
        .unlock_batch_mq(&admin, RequestCode::UnlockBatchMq, &mut request)
        .await;

    let Err(error) = result else {
        panic!("corrupt unlockBatchMQ body must return a typed error");
    };
    assert!(
        std::error::Error::source(error.as_ref()).is_some(),
        "unlockBatchMQ decode error must retain its typed source"
    );
    cleanup(&runtime);
}

/// A well-formed `LOCK_BATCH_MQ` body missing `consumerGroup`/`clientId` must
/// yield an error, not a panic on the `unwrap()` of the absent fields.
#[tokio::test]
async fn lock_batch_mq_missing_group_and_client_returns_error() {
    let runtime = new_test_runtime("lock-only-mqset").await;
    let admin = runtime.admin_runtime_for_test();
    let handler = BatchMqHandler::new();

    let body = LockBatchRequestBody {
        consumer_group: None,
        client_id: None,
        only_this_broker: true,
        mq_set: HashSet::new(),
    };
    let encoded = body.encode().expect("encode lock batch request body");
    let mut request = RemotingCommand::create_request_command(RequestCode::LockBatchMq, EmptyHeader {})
        .set_body(Bytes::from(encoded));
    let result = handler
        .lock_batch_mq(&admin, RequestCode::LockBatchMq, &mut request)
        .await;

    assert!(
        result.is_err(),
        "lockBatchMQ with only mqSet set must return a typed error"
    );
    cleanup(&runtime);
}

/// A well-formed `UNLOCK_BATCH_MQ` body missing `consumerGroup`/`clientId` must
/// yield an error, not a panic on the `unwrap()` of the absent fields.
#[tokio::test]
async fn unlock_batch_mq_missing_group_and_client_returns_error() {
    let runtime = new_test_runtime("unlock-only-mqset").await;
    let admin = runtime.admin_runtime_for_test();
    let handler = BatchMqHandler::new();

    let body = UnlockBatchRequestBody {
        consumer_group: None,
        client_id: None,
        only_this_broker: true,
        mq_set: HashSet::new(),
    };
    let encoded = body.encode().expect("encode unlock batch request body");
    let mut request = RemotingCommand::create_request_command(RequestCode::UnlockBatchMq, EmptyHeader {})
        .set_body(Bytes::from(encoded));
    let result = handler
        .unlock_batch_mq(&admin, RequestCode::UnlockBatchMq, &mut request)
        .await;

    assert!(
        result.is_err(),
        "unlockBatchMQ with only mqSet set must return a typed error"
    );
    cleanup(&runtime);
}
