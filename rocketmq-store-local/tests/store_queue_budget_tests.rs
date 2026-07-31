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

use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_store_local::base::allocate_mapped_file_service::AllocateMappedFileService;

fn allocation_budget(name: &str, count: usize, bytes: usize) -> ResourceBudget {
    ResourceBudgetTree::new(name, BudgetLimit::new(count, bytes, FullPolicy::Reject))
        .expect("allocation test budget")
        .root()
}

#[tokio::test]
async fn mapped_file_queue_rejects_at_count_limit_and_releases_on_shutdown() {
    let service =
        AllocateMappedFileService::new_with_config(None, false, false, allocation_budget("mapped-count", 1, 4_096));

    service.submit_request_in_background("root/00000000000000000000".to_owned(), 1_024);
    service.submit_request_in_background("root/00000000000000001024".to_owned(), 1_024);

    let saturated = service.queue_snapshot();
    assert_eq!(saturated.current_count, 1);
    assert_eq!(saturated.charged_bytes, 1_024);
    assert_eq!(saturated.rejected_count, 1);

    service.shutdown().await;
    let released = service.queue_snapshot();
    assert_eq!(released.current_count, 0);
    assert_eq!(released.charged_bytes, 0);
    assert_eq!(released.abandoned_count, 1);
}

#[tokio::test]
async fn duplicate_mapped_file_request_reuses_existing_budget_owner() {
    let service =
        AllocateMappedFileService::new_with_config(None, false, false, allocation_budget("mapped-duplicate", 1, 1_024));
    let path = "root/00000000000000000000".to_owned();

    service.submit_request_in_background(path.clone(), 1_024);
    service.submit_request_in_background(path, 1_024);

    let snapshot = service.queue_snapshot();
    assert_eq!(snapshot.current_count, 1);
    assert_eq!(snapshot.charged_bytes, 1_024);
    assert_eq!(snapshot.queued_count, 1);
    assert_eq!(snapshot.rejected_count, 0);

    service.shutdown().await;
    assert_eq!(service.queue_snapshot().current_count, 0);
}

#[tokio::test]
async fn mapped_file_queue_rejects_at_byte_limit_before_count_limit() {
    let service =
        AllocateMappedFileService::new_with_config(None, false, false, allocation_budget("mapped-bytes", 2, 1_200));

    service.submit_request_in_background("root/00000000000000000000".to_owned(), 1_024);
    service.submit_request_in_background("root/00000000000000001024".to_owned(), 512);

    let saturated = service.queue_snapshot();
    assert_eq!(saturated.current_count, 1);
    assert_eq!(saturated.charged_bytes, 1_024);
    assert_eq!(saturated.rejected_count, 1);

    service.shutdown().await;
    assert_eq!(service.queue_snapshot().charged_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn mapped_file_timeout_abandons_table_ownership_and_releases_permit() {
    let service =
        AllocateMappedFileService::new_with_config(None, false, false, allocation_budget("mapped-timeout", 1, 1_024));

    let result = service
        .put_request_and_return_mapped_file("root/00000000000000000000".to_owned(), String::new(), 1_024)
        .await
        .expect("timeout is represented by an empty allocation result");

    assert!(result.is_none());
    let snapshot = service.queue_snapshot();
    assert_eq!(snapshot.current_count, 0);
    assert_eq!(snapshot.charged_bytes, 0);
    assert_eq!(snapshot.abandoned_count, 1);
    assert_eq!(snapshot.queued_count, 1, "the stale heap key owns no permit");

    service.shutdown().await;
    assert_eq!(service.queue_snapshot().queued_count, 0);
}

#[tokio::test]
async fn cancelling_mapped_file_wait_releases_table_and_budget_ownership() {
    let service =
        AllocateMappedFileService::new_with_config(None, false, false, allocation_budget("mapped-cancel", 1, 1_024));

    {
        let pending =
            service.put_request_and_return_mapped_file("root/00000000000000000000".to_owned(), String::new(), 1_024);
        tokio::pin!(pending);
        tokio::select! {
            biased;
            _ = &mut pending => panic!("allocation unexpectedly completed"),
            () = tokio::task::yield_now() => {}
        }
    }

    let snapshot = service.queue_snapshot();
    assert_eq!(snapshot.current_count, 0);
    assert_eq!(snapshot.charged_bytes, 0);
    assert_eq!(snapshot.abandoned_count, 1);
    service.shutdown().await;
}
