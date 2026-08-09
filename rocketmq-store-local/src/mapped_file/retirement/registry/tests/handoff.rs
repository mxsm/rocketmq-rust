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

#[test]
fn foreign_and_wrong_tokens_are_returned_and_cas_conflict_can_retry() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let owner_a = Arc::new(TestOwner(1));
    let owner_b = Arc::new(TestOwner(2));
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner_a), queue.clone()))
        .expect("first identity is registered");
    registry
        .register_published(registration(2, 12, FILE_LENGTH, Arc::clone(&owner_b), queue.clone()))
        .expect("second identity is registered");

    let (token_a, binding_a) = commit_test_intent(
        &registry,
        operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 3, NONCE),
        &owner_a,
        &queue,
    );
    let (_token_b, binding_b) = commit_test_intent(
        &registry,
        operation(
            2,
            12,
            FILE_LENGTH,
            FILE_LENGTH,
            RetirementReason::DeleteLast,
            4,
            OTHER_NONCE,
        ),
        &owner_b,
        &queue,
    );

    let foreign_registry = RetirementRegistry::<TestOwner>::new(store_uuid(), 2);
    let (token_a, error) = foreign_registry
        .prepare_handoff(token_a, &binding_a, &queue)
        .expect_err("a foreign registry cannot consume the token")
        .into_parts();
    assert_eq!(error, RegistryError::ForeignToken);
    let wrong_queue = QueueIdentity::allocate();
    let (token_a, error) = registry
        .prepare_handoff(token_a, &binding_a, &wrong_queue)
        .expect_err("a wrong queue identity cannot consume the token")
        .into_parts();
    assert!(matches!(error, RegistryError::TokenQueueIdentityMismatch { .. }));
    let (token_a, error) = registry
        .prepare_handoff(token_a, &binding_b, &queue)
        .expect_err("a wrong binding cannot consume the token")
        .into_parts();
    assert!(matches!(error, RegistryError::TokenBindingMismatch { .. }));

    let prepared = registry
        .prepare_handoff(token_a, &binding_a, &queue)
        .expect("the exact registry, binding, and queue prepare the handoff");
    assert!(prepared.owner().is_some_and(|owner| Arc::ptr_eq(owner, &owner_a)));
    assert_eq!(prepared.binding(), Some(&binding_a));
    assert!(prepared
        .queue_identity()
        .is_some_and(|identity| identity.same_as(&queue)));
    assert_eq!(registry.retained_identity_count(), 2);

    let token_a = prepared
        .rollback()
        .expect("an ArcSwap CAS conflict returns the original token");
    let prepared = registry
        .prepare_handoff(token_a, &binding_a, &queue)
        .expect("the returned token can retry the exact handoff");
    let capability = prepared
        .commit()
        .expect("only successful ArcSwap CAS finalizes token consumption");
    assert_eq!(capability.binding(), &binding_a);
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
}

#[test]
fn abandoned_prepared_handoff_fences_the_registry_and_retains_identity() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let owner = Arc::new(TestOwner(1));
    let weak_owner = Arc::downgrade(&owner);
    let queue = QueueIdentity::allocate();
    registry
        .register_published(registration(1, 11, 0, Arc::clone(&owner), queue.clone()))
        .expect("identity is registered");
    let (token, binding) = commit_test_intent(
        &registry,
        operation(1, 11, 0, FILE_LENGTH, RetirementReason::TtlExpired, 3, NONCE),
        &owner,
        &queue,
    );
    drop(owner);

    let unwind = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _prepared = registry
            .prepare_handoff(token, &binding, &queue)
            .expect("exact handoff is prepared");
        panic!("exercise armed handoff Drop during unwind");
    }));
    assert!(unwind.is_err());
    assert!(registry.needs_recovery());
    assert!(registry.contains_incarnation(incarnation(1)));
    assert!(registry.is_path_reserved(&canonical_path(0)));
    assert!(weak_owner.upgrade().is_some());
}
