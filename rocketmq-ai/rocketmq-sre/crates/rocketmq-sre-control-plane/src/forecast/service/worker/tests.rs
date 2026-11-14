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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use rocketmq_runtime::RuntimeContext;
use tokio::sync::oneshot;

use super::*;

#[tokio::test]
async fn cancelled_owner_does_not_poll_a_ready_read() {
    let runtime = RuntimeContext::from_current("forecast-cancelled-read");
    let context = runtime.service_context("forecast");
    let group = context.task_group();
    group.cancel();
    let reads = AtomicUsize::new(0);
    let result = owned_read(group, async {
        reads.fetch_add(1, Ordering::SeqCst);
    })
    .await;
    assert!(result.is_none());
    assert_eq!(reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn owned_forecast_read_exits_cooperatively_before_its_source_timeout() {
    let runtime = RuntimeContext::from_current("forecast-read-shutdown");
    let context = runtime.service_context("forecast");
    let group = context.task_group().clone();
    let worker = group.clone();
    let (entered, waiting) = oneshot::channel();
    let exited = Arc::new(AtomicBool::new(false));
    let worker_exited = exited.clone();
    group
        .spawn_service("read-wait", async move {
            let response = owned_read(&worker, async {
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            })
            .await;
            assert!(response.is_none());
            worker_exited.store(true, Ordering::SeqCst);
        })
        .unwrap();
    waiting.await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(2), group.shutdown(Duration::from_secs(1)))
        .await
        .unwrap();
    assert!(exited.load(Ordering::SeqCst));
    assert!(report.is_healthy(), "{}", report.to_json());
}
