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

use std::cell::Cell;

use rocketmq_store_local::consume_queue::root::*;

#[test]
fn consume_queue_root_preserves_adapter_identity_and_mutability() {
    let mut root = ConsumeQueueRoot::new(vec![1, 2]);
    root.push(3);

    assert_eq!(root.adapter(), &[1, 2, 3]);
    assert_eq!(root.into_adapter(), vec![1, 2, 3]);
}

#[test]
fn dispatcher_root_gates_transactions_and_projects_progress() {
    let root = ConsumeQueueDispatchRoot::new(Cell::new(0));
    let mut request = 7;

    assert!(!root.dispatch(false, &mut request, |adapter, _| adapter.set(1)));
    assert!(root.dispatch(true, &mut request, |adapter, value| { adapter.set(*value) }));
    assert_eq!(root.adapter().get(), 7);
    assert_eq!(root.progress_offset(|_| -1), None);
    assert_eq!(root.progress_offset(|_| 99), Some(99));
}

#[test]
fn single_dispatch_retries_until_append_succeeds() {
    let mut calls = Vec::new();
    let outcome = drive_consume_queue_dispatch(
        ConsumeQueueDispatchMode::Single,
        ConsumeQueueDispatchMetadata {
            message_base_offset: -1,
            batch_size: 0,
        },
        true,
        30,
        |attempt| {
            calls.push(attempt);
            attempt == 2
        },
    );

    assert_eq!(outcome, ConsumeQueueDispatchOutcome::Appended { attempts: 3 });
    assert_eq!(calls, vec![0, 1, 2]);
}

#[test]
fn batch_dispatch_rejects_invalid_metadata_without_calling_adapter() {
    let outcome = drive_consume_queue_dispatch(
        ConsumeQueueDispatchMode::Batch,
        ConsumeQueueDispatchMetadata {
            message_base_offset: -1,
            batch_size: 1,
        },
        true,
        30,
        |_| panic!("invalid batch must not reach adapter"),
    );

    assert_eq!(outcome, ConsumeQueueDispatchOutcome::InvalidBatch);
}

#[test]
fn dispatch_distinguishes_not_writeable_and_exhausted() {
    let metadata = ConsumeQueueDispatchMetadata {
        message_base_offset: 0,
        batch_size: 1,
    };
    assert_eq!(
        drive_consume_queue_dispatch(ConsumeQueueDispatchMode::Single, metadata, false, 30, |_| true),
        ConsumeQueueDispatchOutcome::NotWriteable
    );
    assert_eq!(
        drive_consume_queue_dispatch(ConsumeQueueDispatchMode::Batch, metadata, true, 3, |_| false),
        ConsumeQueueDispatchOutcome::Exhausted { attempts: 3 }
    );
}

#[test]
fn find_or_create_skips_construction_on_hit_and_publishes_on_miss() {
    let hit = find_or_create_consume_queue(|| Some(7), || panic!("must not create"), |_| panic!("must not publish"));
    let miss = find_or_create_consume_queue(|| None, || 8, |created| created + 1);

    assert_eq!(hit, 7);
    assert_eq!(miss, 9);
}

#[test]
fn queue_offset_clamp_keeps_results_inside_current_bounds() {
    assert_eq!(clamp_consume_queue_offset(-1, 10, 20), 10);
    assert_eq!(clamp_consume_queue_offset(15, 10, 20), 15);
    assert_eq!(clamp_consume_queue_offset(21, 10, 20), 20);
}
