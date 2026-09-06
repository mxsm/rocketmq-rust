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

use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use std::error::Error as _;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;

use super::pending_request_table::PendingRegistrationOutcome;
use super::pending_request_table::PendingRequestCompletion;
use super::pending_request_table::PendingRequestGuard;
use super::pending_request_table::PendingRequestLimits;
use super::pending_request_table::PendingRequestTable;
use crate::deadline::RequestDeadline;

fn registered(outcome: PendingRegistrationOutcome) -> PendingRequestGuard {
    match outcome {
        PendingRegistrationOutcome::Registered(guard) => guard,
        PendingRegistrationOutcome::DeadlineExpired => panic!("registration deadline expired"),
        PendingRegistrationOutcome::SessionClosed => panic!("registration owner closed"),
        PendingRegistrationOutcome::QueueSaturated => panic!("registration queue saturated"),
        PendingRegistrationOutcome::OperationalFailure(_) => panic!("registration failed operationally"),
    }
}

fn is_rejected(outcome: &PendingRegistrationOutcome) -> bool {
    !matches!(outcome, PendingRegistrationOutcome::Registered(_))
}

fn response(completion: PendingRequestCompletion) -> RemotingCommand {
    match completion {
        PendingRequestCompletion::Response(response) => response,
        PendingRequestCompletion::DeadlineExpired => panic!("request deadline expired"),
        PendingRequestCompletion::Cancelled => panic!("request was cancelled"),
        PendingRequestCompletion::SessionClosed => panic!("request session closed"),
        PendingRequestCompletion::OperationalFailure(_) => panic!("request failed operationally"),
    }
}

#[tokio::test]
async fn response_completion_is_exactly_once_and_releases_the_reservation() {
    let table = PendingRequestTable::new();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let guard = registered(table.register(7, RequestDeadline::from_timeout_millis(3_000), sender));

    assert_eq!(table.len(), 1);
    assert!(table.complete_response(
        7,
        RemotingCommand::create_response_command_with_code(ResponseCode::Success),
    ));
    assert!(!table.complete_response(
        7,
        RemotingCommand::create_response_command_with_code(ResponseCode::SystemError),
    ));

    let response = response(receiver.await.expect("completion should notify the waiter"));
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
        guards.push(registered(table.register(
            opaque,
            RequestDeadline::from_timeout_millis(3_000),
            sender,
        )));
        receivers.push(receiver);
    }

    assert_eq!(
        table.expire_due(tokio::time::Instant::now() + Duration::from_secs(4)),
        REQUESTS
    );
    assert_eq!(table.len(), 0);
    for receiver in receivers {
        assert!(matches!(
            receiver.await.expect("timeout should notify every waiter"),
            PendingRequestCompletion::DeadlineExpired
        ));
    }
    drop(guards);
}

#[tokio::test]
async fn dropping_guard_completes_waiter_with_typed_cancellation() {
    let table = PendingRequestTable::new();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let guard = registered(table.register(41, RequestDeadline::from_timeout_millis(3_000), sender));

    drop(guard);

    assert!(matches!(
        receiver.await.expect("drop should complete the waiter"),
        PendingRequestCompletion::Cancelled
    ));
    assert!(table.is_empty());
}

#[tokio::test]
async fn operational_completion_reuses_the_original_canonical_error() {
    let table = PendingRequestTable::new();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let guard = registered(table.register(42, RequestDeadline::from_timeout_millis(3_000), sender));
    let source = crate::error_helpers::response_timeout_caused_by_for_remote(
        "127.0.0.1:9876",
        3_000,
        std::io::Error::other("response wait failed"),
    );

    let returned = guard.expire_with_error(Arc::clone(&source));
    let completed = match receiver.await.expect("operational completion") {
        PendingRequestCompletion::OperationalFailure(error) => error,
        _ => panic!("operational expiration must retain its canonical error"),
    };

    assert!(Arc::ptr_eq(&source, &returned));
    assert!(Arc::ptr_eq(&source, &completed));
    assert_eq!(completed.code(), rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT.code());
    assert!(completed
        .source()
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some_and(|source| source.to_string() == "response wait failed"));
    assert!(table.is_empty());
}

#[tokio::test]
async fn retired_opaque_cannot_be_reused_by_a_late_response() {
    let table = PendingRequestTable::new();
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let first = registered(table.register(9, RequestDeadline::from_timeout_millis(1), first_sender));
    assert_eq!(
        table.expire_due(tokio::time::Instant::now() + Duration::from_millis(2)),
        1
    );
    assert!(matches!(
        first_receiver.await.unwrap(),
        PendingRequestCompletion::DeadlineExpired
    ));
    drop(first);

    let (second_sender, _second_receiver) = tokio::sync::oneshot::channel();
    assert!(is_rejected(&table.register(
        9,
        RequestDeadline::from_timeout_millis(3_000),
        second_sender,
    )));
    assert!(!table.complete_response(
        9,
        RemotingCommand::create_response_command_with_code(ResponseCode::Success),
    ));
}

#[tokio::test]
async fn admission_permit_is_released_after_completion() {
    let table = PendingRequestTable::with_capacity(1);
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let first = registered(table.register(1, RequestDeadline::from_timeout_millis(3_000), first_sender));
    let (blocked_sender, _blocked_receiver) = tokio::sync::oneshot::channel();
    assert!(is_rejected(&table.register(
        2,
        RequestDeadline::from_timeout_millis(3_000),
        blocked_sender,
    )));

    assert!(first.complete(PendingRequestCompletion::SessionClosed));
    assert!(matches!(
        first_receiver.await.unwrap(),
        PendingRequestCompletion::SessionClosed
    ));
    let (next_sender, _next_receiver) = tokio::sync::oneshot::channel();
    assert!(matches!(
        table.register(2, RequestDeadline::from_timeout_millis(3_000), next_sender),
        PendingRegistrationOutcome::Registered(_)
    ));
}

#[tokio::test]
async fn close_all_completes_every_waiter_with_a_typed_cause() {
    const REQUESTS: usize = 128;

    let table = PendingRequestTable::new();
    let mut receivers = Vec::with_capacity(REQUESTS);
    let mut guards = Vec::with_capacity(REQUESTS);
    for opaque in 0..REQUESTS as i32 {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        guards.push(registered(table.register(
            opaque,
            RequestDeadline::from_timeout_millis(3_000),
            sender,
        )));
        receivers.push(receiver);
    }

    assert_eq!(table.close_all(|| PendingRequestCompletion::SessionClosed), REQUESTS);
    assert!(table.is_empty());
    for receiver in receivers {
        assert!(matches!(
            receiver.await.expect("close should notify every waiter"),
            PendingRequestCompletion::SessionClosed
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
    let first_guard = registered(table.register_for_owner(
        &first_owner,
        17,
        RequestDeadline::from_timeout_millis(3_000),
        first_sender,
    ));
    let second_guard = registered(table.register_for_owner(
        &second_owner,
        17,
        RequestDeadline::from_timeout_millis(3_000),
        second_sender,
    ));

    assert_eq!(
        table.close_owner(&first_owner, || PendingRequestCompletion::SessionClosed),
        1
    );
    assert!(matches!(
        first_receiver.await.expect("first owner should be completed"),
        PendingRequestCompletion::SessionClosed
    ));
    assert!(second_receiver.try_recv().is_err());
    assert_eq!(table.len(), 1);

    assert!(table.complete_response_for_owner(
        &second_owner,
        17,
        RemotingCommand::create_response_command_with_code(ResponseCode::Success),
    ));
    assert_eq!(
        response(second_receiver.await.unwrap()).code(),
        ResponseCode::Success.to_i32()
    );
    drop((first_guard, second_guard));
}

#[tokio::test]
async fn timed_out_owner_rejects_reuse_but_rotated_owner_is_safe_from_late_response() {
    let table = PendingRequestTable::new();
    let retired_owner = table.new_owner();
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let first = registered(table.register_for_owner(
        &retired_owner,
        29,
        RequestDeadline::from_timeout_millis(1),
        first_sender,
    ));

    assert_eq!(
        table.expire_due(tokio::time::Instant::now() + Duration::from_millis(2)),
        1
    );
    assert!(matches!(
        first_receiver.await.unwrap(),
        PendingRequestCompletion::DeadlineExpired
    ));
    drop(first);
    let (reused_sender, _reused_receiver) = tokio::sync::oneshot::channel();
    assert!(is_rejected(&table.register_for_owner(
        &retired_owner,
        29,
        RequestDeadline::from_timeout_millis(3_000),
        reused_sender,
    )));

    let rotated_owner = table.new_owner();
    let (rotated_sender, rotated_receiver) = tokio::sync::oneshot::channel();
    let rotated = registered(table.register_for_owner(
        &rotated_owner,
        29,
        RequestDeadline::from_timeout_millis(3_000),
        rotated_sender,
    ));
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
        response(rotated_receiver.await.unwrap()).code(),
        ResponseCode::Success.to_i32()
    );
    drop(rotated);
}

#[tokio::test]
async fn count_and_byte_admission_are_observable_and_released_on_every_completion_path() {
    let table = PendingRequestTable::with_limits(PendingRequestLimits {
        max_count: 2,
        max_bytes: 8,
        admission_rate_per_second: 2,
        max_request_age: Duration::from_secs(3),
    });
    let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
    let first = registered(table.register_with_bytes(1, RequestDeadline::from_timeout_millis(3_000), 6, first_sender));
    assert_eq!(table.usage().count, 1);
    assert_eq!(table.usage().bytes, 6);

    let (byte_blocked_sender, _byte_blocked_receiver) = tokio::sync::oneshot::channel();
    assert!(is_rejected(&table.register_with_bytes(
        2,
        RequestDeadline::from_timeout_millis(3_000),
        3,
        byte_blocked_sender,
    )));
    assert_eq!(table.usage().rejected_bytes, 1);

    assert!(first.complete(PendingRequestCompletion::SessionClosed));
    assert!(matches!(
        first_receiver.await.unwrap(),
        PendingRequestCompletion::SessionClosed
    ));
    assert_eq!(table.usage().count, 0);
    assert_eq!(table.usage().bytes, 0);

    let (next_sender, next_receiver) = tokio::sync::oneshot::channel();
    let next = registered(table.register_with_bytes(2, RequestDeadline::from_timeout_millis(3_000), 8, next_sender));
    assert_eq!(table.close_all(|| PendingRequestCompletion::SessionClosed), 1);
    assert!(matches!(
        next_receiver.await.unwrap(),
        PendingRequestCompletion::SessionClosed
    ));
    drop(next);
    assert_eq!(table.usage().count, 0);
    assert_eq!(table.usage().bytes, 0);
}

#[test]
fn pending_request_deadline_is_capped_by_the_configured_maximum_age() {
    let table = PendingRequestTable::try_with_limits(PendingRequestLimits {
        max_count: 1,
        max_bytes: 1024,
        admission_rate_per_second: 1,
        max_request_age: Duration::from_millis(50),
    })
    .expect("pending request limits");
    let (sender, _receiver) = tokio::sync::oneshot::channel();
    let guard = registered(table.register(1, RequestDeadline::from_timeout_millis(60_000), sender));

    assert_eq!(guard.deadline().budget(), Duration::from_millis(50));
    assert!(guard.deadline().remaining() <= Duration::from_millis(50));
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
            (
                table.register_for_owner(&owner, opaque, RequestDeadline::from_timeout_millis(30_000), sender),
                receiver,
            )
        }));
    }
    let close_table = table.clone();
    let close_owner = owner.clone();
    let close_barrier = barrier.clone();
    let closing = std::thread::spawn(move || {
        close_barrier.wait();
        close_table.close_owner(&close_owner, || PendingRequestCompletion::SessionClosed)
    });

    barrier.wait();
    let _ = closing.join().unwrap();
    let successful = registrations
        .into_iter()
        .filter_map(|registration| {
            let (result, receiver) = registration.join().unwrap();
            match result {
                PendingRegistrationOutcome::Registered(guard) => Some((guard, receiver)),
                PendingRegistrationOutcome::DeadlineExpired
                | PendingRegistrationOutcome::SessionClosed
                | PendingRegistrationOutcome::QueueSaturated
                | PendingRegistrationOutcome::OperationalFailure(_) => None,
            }
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

#[test]
fn timeout_response_and_disconnect_race_completes_once_and_retires_the_owner() {
    const ITERATIONS: i32 = 128;

    for opaque in 0..ITERATIONS {
        let table = PendingRequestTable::new();
        let owner = table.new_owner();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let guard =
            registered(table.register_for_owner(&owner, opaque, RequestDeadline::from_timeout_millis(30_000), sender));
        let barrier = Arc::new(Barrier::new(4));

        let expire_barrier = Arc::clone(&barrier);
        let expiration = std::thread::spawn(move || {
            expire_barrier.wait();
            guard.expire_with_error(crate::error_helpers::response_timeout_caused_by_for_remote(
                "race",
                30_000,
                std::io::Error::other("elapsed"),
            ))
        });

        let response_table = table.clone();
        let response_owner = owner.clone();
        let response_barrier = Arc::clone(&barrier);
        let response = std::thread::spawn(move || {
            response_barrier.wait();
            response_table.complete_response_for_owner(
                &response_owner,
                opaque,
                RemotingCommand::create_response_command_with_code(ResponseCode::Success),
            )
        });

        let disconnect_table = table.clone();
        let disconnect_owner = owner.clone();
        let disconnect_barrier = Arc::clone(&barrier);
        let disconnect = std::thread::spawn(move || {
            disconnect_barrier.wait();
            disconnect_table.close_owner(&disconnect_owner, || PendingRequestCompletion::SessionClosed)
        });

        barrier.wait();
        expiration.join().expect("expiration contender");
        let response_won = response.join().expect("response contender");
        let disconnected = disconnect.join().expect("disconnect contender");
        let result = receiver
            .blocking_recv()
            .expect("one contender must complete the waiter");

        match result {
            PendingRequestCompletion::Response(command) => {
                assert!(response_won);
                assert_eq!(command.code(), ResponseCode::Success.to_i32());
                assert_eq!(disconnected, 0);
            }
            PendingRequestCompletion::OperationalFailure(source)
                if source.code() == rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT.code() =>
            {
                assert!(!response_won);
                assert_eq!(disconnected, 0);
            }
            PendingRequestCompletion::SessionClosed => {
                assert!(!response_won);
                assert_eq!(disconnected, 1);
            }
            PendingRequestCompletion::DeadlineExpired
            | PendingRequestCompletion::Cancelled
            | PendingRequestCompletion::OperationalFailure(_) => {
                panic!("race returned an unrelated pending completion")
            }
        }
        assert!(table.is_empty());
        assert!(!owner.is_accepting());
        assert_eq!(table.usage().count, 0);
        assert_eq!(table.usage().bytes, 0);

        let (next_sender, _next_receiver) = tokio::sync::oneshot::channel();
        assert!(is_rejected(&table.register_for_owner(
            &owner,
            opaque,
            RequestDeadline::from_timeout_millis(30_000),
            next_sender,
        )));
    }
}
