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

use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::base::pending_request_table::PendingRequestLimits;
use rocketmq_transport::base::pending_request_table::PendingRequestTable;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;
use std::time::Instant;

#[tokio::test]
async fn response_completion_is_exactly_once_and_releases_the_reservation() {
    let table = PendingRequestTable::new();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let guard = table
        .register(7, 3_000, sender)
        .expect("first reservation should succeed");

    assert_eq!(table.len(), 1);
    assert!(table.complete_response(
        7,
        RemotingCommand::create_response_command_with_code(ResponseCode::Success),
    ));
    assert!(!table.complete_response(
        7,
        RemotingCommand::create_response_command_with_code(ResponseCode::SystemError),
    ));

    let response = receiver.await.expect("completion should notify the waiter").unwrap();
    assert_eq!(response.code(), ResponseCode::Success.to_i32());
    assert_eq!(table.len(), 0);
    drop(guard);
    assert_eq!(table.len(), 0);
}

#[tokio::test]
async fn expiring_ten_thousand_requests_completes_every_waiter_and_releases_every_reservation() {
    const REQUESTS: usize = 10_000;

    let table = PendingRequestTable::with_capacity(REQUESTS);
    let mut receivers = Vec::with_capacity(REQUESTS);
    let mut guards = Vec::with_capacity(REQUESTS);
    for opaque in 0..REQUESTS as i32 {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        guards.push(
            table
                .register(opaque, 3_000, sender)
                .expect("unique opaque should reserve successfully"),
        );
        receivers.push(receiver);
    }

    assert_eq!(table.expire_due(Instant::now() + Duration::from_secs(4)), REQUESTS);
    assert_eq!(table.len(), 0);
    for receiver in receivers {
        assert!(matches!(
            receiver.await.expect("timeout should notify every waiter"),
            Err(RocketMQError::Timeout { .. })
        ));
    }
    drop(guards);
}

#[tokio::test]
async fn dropping_guard_completes_waiter_with_typed_cancellation() {
    let table = PendingRequestTable::new();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let guard = table.register(41, 3_000, sender).unwrap();

    drop(guard);

    assert!(matches!(
        receiver.await.expect("drop should complete the waiter"),
        Err(RocketMQError::Network(_))
    ));
    assert!(table.is_empty());
}

#[tokio::test]
async fn retired_opaque_cannot_be_reused_by_a_late_response() {
    let table = PendingRequestTable::new();
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let first = table.register(9, 1, first_sender).unwrap();
    assert_eq!(table.expire_due(Instant::now() + Duration::from_millis(2)), 1);
    assert!(matches!(
        first_receiver.await.unwrap(),
        Err(RocketMQError::Timeout { .. })
    ));
    drop(first);

    let (second_sender, _second_receiver) = tokio::sync::oneshot::channel();
    assert!(table.register(9, 3_000, second_sender).is_err());
    assert!(!table.complete_response(
        9,
        RemotingCommand::create_response_command_with_code(ResponseCode::Success),
    ));
}

#[tokio::test]
async fn admission_permit_is_released_after_completion() {
    let table = PendingRequestTable::with_capacity(1);
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let first = table.register(1, 3_000, first_sender).unwrap();
    let (blocked_sender, _blocked_receiver) = tokio::sync::oneshot::channel();
    assert!(table.register(2, 3_000, blocked_sender).is_err());

    assert!(first.complete(Err(RocketMQError::network_connection_failed("test", "done",))));
    assert!(first_receiver.await.unwrap().is_err());
    let (next_sender, _next_receiver) = tokio::sync::oneshot::channel();
    assert!(table.register(2, 3_000, next_sender).is_ok());
}

#[tokio::test]
async fn close_all_completes_every_waiter_with_a_typed_cause() {
    const REQUESTS: usize = 128;

    let table = PendingRequestTable::new();
    let mut receivers = Vec::with_capacity(REQUESTS);
    let mut guards = Vec::with_capacity(REQUESTS);
    for opaque in 0..REQUESTS as i32 {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        guards.push(
            table
                .register(opaque, 3_000, sender)
                .expect("unique opaque should reserve successfully"),
        );
        receivers.push(receiver);
    }

    assert_eq!(
        table.close_all(|| RocketMQError::network_connection_failed("test-peer", "connection closed")),
        REQUESTS
    );
    assert!(table.is_empty());
    for receiver in receivers {
        assert!(matches!(
            receiver.await.expect("close should notify every waiter"),
            Err(RocketMQError::Network(_))
        ));
    }
    drop(guards);
    assert!(table.is_empty());
}

#[tokio::test]
async fn closing_one_connection_owner_does_not_complete_another_owners_request() {
    let table = PendingRequestTable::new();
    let first_owner = table.new_owner();
    let second_owner = table.new_owner();
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let (second_sender, mut second_receiver) = tokio::sync::oneshot::channel();
    let first_guard = table.register_for_owner(&first_owner, 17, 3_000, first_sender).unwrap();
    let second_guard = table
        .register_for_owner(&second_owner, 17, 3_000, second_sender)
        .unwrap();

    assert_eq!(
        table.close_owner(&first_owner, || {
            RocketMQError::network_connection_failed("first-peer", "connection closed")
        }),
        1
    );
    assert!(matches!(
        first_receiver.await.expect("first owner should be completed"),
        Err(RocketMQError::Network(_))
    ));
    assert!(second_receiver.try_recv().is_err());
    assert_eq!(table.len(), 1);

    assert!(table.complete_response_for_owner(
        &second_owner,
        17,
        RemotingCommand::create_response_command_with_code(ResponseCode::Success),
    ));
    assert_eq!(
        second_receiver.await.unwrap().unwrap().code(),
        ResponseCode::Success.to_i32()
    );
    drop((first_guard, second_guard));
}

#[tokio::test]
async fn timed_out_owner_rejects_reuse_but_rotated_owner_is_safe_from_late_response() {
    let table = PendingRequestTable::new();
    let retired_owner = table.new_owner();
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let first = table.register_for_owner(&retired_owner, 29, 1, first_sender).unwrap();

    assert_eq!(table.expire_due(Instant::now() + Duration::from_millis(2)), 1);
    assert!(matches!(
        first_receiver.await.unwrap(),
        Err(RocketMQError::Timeout { .. })
    ));
    drop(first);
    let (reused_sender, _reused_receiver) = tokio::sync::oneshot::channel();
    assert!(table
        .register_for_owner(&retired_owner, 29, 3_000, reused_sender)
        .is_err());

    let rotated_owner = table.new_owner();
    let (rotated_sender, rotated_receiver) = tokio::sync::oneshot::channel();
    let rotated = table
        .register_for_owner(&rotated_owner, 29, 3_000, rotated_sender)
        .unwrap();
    assert!(!table.complete_response_for_owner(
        &retired_owner,
        29,
        RemotingCommand::create_response_command_with_code(ResponseCode::SystemError),
    ));
    assert!(table.complete_response_for_owner(
        &rotated_owner,
        29,
        RemotingCommand::create_response_command_with_code(ResponseCode::Success),
    ));
    assert_eq!(
        rotated_receiver.await.unwrap().unwrap().code(),
        ResponseCode::Success.to_i32()
    );
    drop(rotated);
}

#[tokio::test]
async fn count_and_byte_admission_are_observable_and_released_on_every_completion_path() {
    let table = PendingRequestTable::with_limits(PendingRequestLimits {
        max_count: 2,
        max_bytes: 8,
    });
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let first = table
        .register_with_bytes(1, 3_000, 6, first_sender)
        .expect("first request fits both budgets");
    assert_eq!(table.usage().count, 1);
    assert_eq!(table.usage().bytes, 6);

    let (byte_blocked_sender, _byte_blocked_receiver) = tokio::sync::oneshot::channel();
    assert!(table.register_with_bytes(2, 3_000, 3, byte_blocked_sender).is_err());
    assert_eq!(table.usage().rejected_bytes, 1);

    assert!(first.complete(Err(RocketMQError::network_connection_failed("test", "send failed",))));
    assert!(first_receiver.await.unwrap().is_err());
    assert_eq!(table.usage().count, 0);
    assert_eq!(table.usage().bytes, 0);

    let (next_sender, next_receiver) = tokio::sync::oneshot::channel();
    let next = table.register_with_bytes(2, 3_000, 8, next_sender).unwrap();
    assert_eq!(
        table.close_all(|| { RocketMQError::network_connection_failed("test", "connection closed") }),
        1
    );
    assert!(next_receiver.await.unwrap().is_err());
    drop(next);
    assert_eq!(table.usage().count, 0);
    assert_eq!(table.usage().bytes, 0);
}

#[test]
fn close_and_registration_are_one_atomic_owner_epoch() {
    const REGISTRATIONS: usize = 64;

    let table = PendingRequestTable::with_capacity(REGISTRATIONS);
    let owner = table.new_owner();
    let barrier = Arc::new(Barrier::new(REGISTRATIONS + 2));
    let mut registrations = Vec::with_capacity(REGISTRATIONS);
    for opaque in 0..REGISTRATIONS as i32 {
        let table = table.clone();
        let owner = owner.clone();
        let barrier = barrier.clone();
        registrations.push(std::thread::spawn(move || {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            barrier.wait();
            (table.register_for_owner(&owner, opaque, 30_000, sender), receiver)
        }));
    }
    let close_table = table.clone();
    let close_owner = owner.clone();
    let close_barrier = barrier.clone();
    let closing = std::thread::spawn(move || {
        close_barrier.wait();
        close_table.close_owner(&close_owner, || {
            RocketMQError::network_connection_failed("race", "owner closed")
        })
    });

    barrier.wait();
    let _ = closing.join().unwrap();
    let successful = registrations
        .into_iter()
        .filter_map(|registration| {
            let (result, receiver) = registration.join().unwrap();
            result.ok().map(|guard| (guard, receiver))
        })
        .collect::<Vec<_>>();

    assert!(table.is_empty(), "no registration may appear after the close snapshot");
    for (guard, mut receiver) in successful {
        assert!(
            receiver.try_recv().is_ok(),
            "every accepted registration must be completed by close"
        );
        drop(guard);
    }
}
