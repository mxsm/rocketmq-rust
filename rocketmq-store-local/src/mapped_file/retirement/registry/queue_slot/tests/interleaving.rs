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
fn independent_tickets_may_interleave_in_the_global_ledger() {
    let registry = RetirementRegistry::new(store_uuid(), 0);
    let queue = ManagedMappedFileQueueGeneration::new_write_disabled();
    let first = Arc::new(TestOwner);
    let second = Arc::new(TestOwner);
    register_managed_owner(&registry, &queue, Arc::clone(&first), 1, 11, 0, 3);
    register_managed_owner(&registry, &queue, Arc::clone(&second), 2, 12, FILE_LENGTH, 4);
    let mut writer =
        ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), [0x31; 16], 4, 1, 1, 0, true, 5)
            .expect("replay cursor is valid");

    let first_reservation = registry
        .prepare_retirement(operation(1, 11, 0, 3), &first, &queue.queue_identity())
        .expect("first intent is prepared");
    let first_binding = first_reservation.binding().clone();
    let first_token = writer
        .append_retirement_intent(first_reservation.begin_append())
        .expect("first intent is durable");
    let first_handoff = queue
        .handoff_retirement(&registry, first_token, &first_binding)
        .expect("first queue handoff succeeds");

    let second_reservation = registry
        .prepare_retirement(operation(2, 12, FILE_LENGTH, 4), &second, &queue.queue_identity())
        .expect("second intent is prepared before the first ticket advances");
    let second_binding = second_reservation.binding().clone();
    let second_token = writer
        .append_retirement_intent(second_reservation.begin_append())
        .expect("second intent is durable");
    let second_handoff = queue
        .handoff_retirement(&registry, second_token, &second_binding)
        .expect("second queue handoff succeeds");

    let first_logical = writer
        .append_logical_removed(first_handoff)
        .expect("the first ticket may advance after another ticket's intent");
    let second_logical = writer
        .append_logical_removed(second_handoff)
        .expect("the second ticket may then advance independently");

    assert_eq!(first_logical.durable_sequence(), 3);
    assert_eq!(second_logical.durable_sequence(), 4);
    assert!(!registry.needs_recovery());
}
