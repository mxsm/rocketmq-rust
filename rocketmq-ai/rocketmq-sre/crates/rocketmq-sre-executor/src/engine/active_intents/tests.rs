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

use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

use super::*;

fn key(cluster: ClusterId, selector: usize) -> IntentKey {
    IntentKey {
        tenant: TenantId::new(),
        cluster,
        selector: format!("{selector:064x}"),
    }
}

#[test]
fn duplicate_and_cluster_takeover_are_blocked_until_the_original_operation_releases() {
    let owner = ActiveIntents::default();
    let first = key(ClusterId::new(), 1);
    let mut second = first.clone();
    second.selector = format!("{:064x}", 2);
    let first_guard = owner.enter_key(first.clone(), "same".to_owned()).unwrap();
    assert!(matches!(
        owner.enter_key(first.clone(), "same".to_owned()),
        Err(ExecutorRequestFailure::Rejected(
            crate::ExecutorRejection::ReconcileBlocked
        ))
    ));
    assert!(matches!(
        owner.enter_key(first.clone(), "changed".to_owned()),
        Err(ExecutorRequestFailure::Rejected(
            crate::ExecutorRejection::PreconditionChanged
        ))
    ));
    // Different requests may share an already active lease without advancing it.
    let second_guard = owner.enter_key(second.clone(), "same".to_owned()).unwrap();
    assert!(owner.takeover_key(first.clone()).is_err());
    assert!(owner.takeover_key(second.clone()).is_err());
    drop(second_guard);
    let takeover = owner.takeover_key(first.clone()).unwrap();
    assert!(owner.enter_key(second.clone(), "same".to_owned()).is_err());
    let unrelated = owner.enter_key(key(ClusterId::new(), 3), "same".to_owned()).unwrap();
    drop(takeover);
    let second_guard = owner.enter_key(second, "same".to_owned()).unwrap();
    drop((first_guard, second_guard, unrelated));
    assert!(owner.enter_key(first, "same".to_owned()).is_ok());
}

#[test]
fn local_capacity_is_bounded_and_errors_do_not_leave_a_reservation() {
    let owner = ActiveIntents::default();
    let cluster = ClusterId::new();
    let guards: Vec<_> = (0..MAX_ACTIVE_INTENTS)
        .map(|i| owner.enter_key(key(cluster, i), "same".to_owned()).unwrap())
        .collect();
    assert!(
        owner
            .enter_key(key(cluster, MAX_ACTIVE_INTENTS), "same".to_owned())
            .is_err()
    );
    assert!(owner.takeover_key(key(cluster, MAX_ACTIVE_INTENTS)).is_err());
    drop(guards);
    let guard = owner.enter_key(key(cluster, 0), "same".to_owned()).unwrap();
    assert!(
        owner.takeover_key(key(cluster, 0)).is_err(),
        "a different tenant is not the original intent"
    );
    drop(guard);
    assert!(owner.0.lock().unwrap().intents.is_empty());
    assert!(owner.0.lock().unwrap().taking_over.is_empty());
}

#[test]
fn dropping_owned_work_releases_intent_and_takeover_even_without_a_waiter() {
    let owner = ActiveIntents::default();
    let first = key(ClusterId::new(), 1);
    let mut work = Box::pin(async {
        let _intent = owner.enter_key(first.clone(), "same".to_owned()).unwrap();
        let _takeover = owner.takeover_key(first.clone()).unwrap();
        std::future::pending::<()>().await;
    });
    assert_eq!(
        work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    );
    assert!(owner.enter_key(first.clone(), "same".to_owned()).is_err());
    drop(work);
    let _intent = owner.enter_key(first.clone(), "same".to_owned()).unwrap();
    assert!(owner.takeover_key(first).is_ok());
}
