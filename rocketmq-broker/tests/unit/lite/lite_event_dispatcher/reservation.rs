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

use std::collections::HashSet;

use cheetah_string::CheetahString;

use super::LiteEventReservationSnapshot;
use crate::lite::lite_event_dispatcher::LiteEventDispatcher;

#[test]
fn lite_event_batch_reservation_rollback_restores_order_and_original_permits() {
    let dispatcher = LiteEventDispatcher::default();
    let client_id = CheetahString::from_static_str("client-a");
    let group = CheetahString::from_static_str("group-a");
    let first = HashSet::from([
        CheetahString::from_static_str("%LMQ%$parent$child-a"),
        CheetahString::from_static_str("%LMQ%$parent$child-b"),
    ]);
    dispatcher.do_full_dispatch(&client_id, &group, &first);
    let reservation = dispatcher
        .reserve_pending_events(&client_id)
        .expect("pending events should reserve");
    let later = HashSet::from([
        CheetahString::from_static_str("%LMQ%$parent$child-b"),
        CheetahString::from_static_str("%LMQ%$parent$child-c"),
    ]);

    assert_eq!(dispatcher.do_full_dispatch(&client_id, &group, &later), 1);
    assert!(dispatcher.reserve_pending_events(&client_id).is_none());
    drop(reservation);

    assert_eq!(
        dispatcher.pending_events(&client_id),
        vec![
            CheetahString::from_static_str("%LMQ%$parent$child-a"),
            CheetahString::from_static_str("%LMQ%$parent$child-b"),
            CheetahString::from_static_str("%LMQ%$parent$child-c"),
        ]
    );
    assert_eq!(dispatcher.budget_snapshot().current_count, 3);
    assert_eq!(
        dispatcher.reservation_snapshot(),
        LiteEventReservationSnapshot::default()
    );
}

#[test]
fn lite_event_batch_reservation_commit_requeues_with_original_permit_once() {
    let dispatcher = LiteEventDispatcher::default();
    let client_id = CheetahString::from_static_str("client-a");
    let group = CheetahString::from_static_str("group-a");
    let first = HashSet::from([
        CheetahString::from_static_str("%LMQ%$parent$child-a"),
        CheetahString::from_static_str("%LMQ%$parent$child-b"),
    ]);
    dispatcher.do_full_dispatch(&client_id, &group, &first);
    let reservation = dispatcher
        .reserve_pending_events(&client_id)
        .expect("pending events should reserve");
    assert_eq!(reservation.event_count(), 2);
    assert!(reservation.retained_bytes() > 0);
    let snapshot = dispatcher.reservation_snapshot();
    assert_eq!(snapshot.events, 2);
    assert_eq!(snapshot.permits, 2);
    let batch = reservation.commit();
    let duplicate = HashSet::from([CheetahString::from_static_str("%LMQ%$parent$child-b")]);
    assert_eq!(dispatcher.do_full_dispatch(&client_id, &group, &duplicate), 0);

    batch.complete(&HashSet::from([
        CheetahString::from_static_str("%LMQ%$parent$child-a"),
        CheetahString::from_static_str("%LMQ%$parent$child-b"),
    ]));

    assert_eq!(
        dispatcher.pending_events(&client_id),
        vec![
            CheetahString::from_static_str("%LMQ%$parent$child-a"),
            CheetahString::from_static_str("%LMQ%$parent$child-b"),
        ]
    );
    assert_eq!(dispatcher.budget_snapshot().current_count, 2);
    assert_eq!(
        dispatcher.reservation_snapshot(),
        LiteEventReservationSnapshot::default()
    );
}

#[test]
fn lite_event_batch_reservation_drop_after_commit_requeues_every_event() {
    let dispatcher = LiteEventDispatcher::default();
    let client_id = CheetahString::from_static_str("client-a");
    let group = CheetahString::from_static_str("group-a");
    let events = HashSet::from([CheetahString::from_static_str("%LMQ%$parent$child-a")]);
    dispatcher.do_full_dispatch(&client_id, &group, &events);

    let batch = dispatcher
        .reserve_pending_events(&client_id)
        .expect("pending event should reserve")
        .commit();
    assert_eq!(batch.event_count(), 1);
    assert!(batch.retained_bytes() > 0);
    drop(batch);

    assert_eq!(dispatcher.pending_events(&client_id).len(), 1);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    assert_eq!(
        dispatcher.reservation_snapshot(),
        LiteEventReservationSnapshot::default()
    );
}

#[test]
fn lite_event_batch_reservation_redispatch_after_commit_reuses_permit_and_stays_pending() {
    let dispatcher = LiteEventDispatcher::default();
    let client_id = CheetahString::from_static_str("client-a");
    let group = CheetahString::from_static_str("group-a");
    let first = CheetahString::from_static_str("%LMQ%$parent$child-a");
    let second = CheetahString::from_static_str("%LMQ%$parent$child-b");
    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([first.clone(), second.clone()]));
    let batch = dispatcher
        .reserve_pending_events(&client_id)
        .expect("pending events should reserve")
        .commit();

    assert_eq!(
        dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([second.clone()])),
        0,
        "redispatch is deduplicated without acquiring another permit"
    );
    assert_eq!(dispatcher.budget_snapshot().current_count, 2);
    batch.complete(&HashSet::new());

    assert_eq!(dispatcher.pending_events(&client_id), vec![second]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    assert_eq!(
        dispatcher.reservation_snapshot(),
        LiteEventReservationSnapshot::default()
    );
}

#[test]
fn lite_event_terminal_owner_drop_rolls_back_reserved_batch_in_original_order() {
    let dispatcher = LiteEventDispatcher::default();
    let client_id = CheetahString::from_static_str("terminal-reserved");
    let group = CheetahString::from_static_str("group-a");
    let first = CheetahString::from_static_str("%LMQ%$parent$child-a");
    let second = CheetahString::from_static_str("%LMQ%$parent$child-b");
    let later = CheetahString::from_static_str("%LMQ%$parent$child-c");
    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([first.clone(), second.clone()]));
    let reservation = dispatcher
        .reserve_pending_events(&client_id)
        .expect("terminal-owned reserved batch");
    let (execution, terminal) = reservation.into_terminal_ownership();
    drop(execution);
    assert_eq!(
        dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([second.clone(), later.clone()]),),
        1,
        "duplicate reserved event does not acquire a second permit"
    );

    drop(terminal);

    assert_eq!(dispatcher.pending_events(&client_id), vec![first, second, later]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 3);
    assert_eq!(
        dispatcher.reservation_snapshot(),
        LiteEventReservationSnapshot::default()
    );
}

#[test]
fn lite_event_terminal_owner_drop_applies_staged_completion_exactly_once() {
    let dispatcher = LiteEventDispatcher::default();
    let client_id = CheetahString::from_static_str("terminal-committed");
    let group = CheetahString::from_static_str("group-a");
    let first = CheetahString::from_static_str("%LMQ%$parent$child-a");
    let second = CheetahString::from_static_str("%LMQ%$parent$child-b");
    dispatcher.do_full_dispatch(&client_id, &group, &HashSet::from([first.clone(), second.clone()]));
    let reservation = dispatcher
        .reserve_pending_events(&client_id)
        .expect("terminal-owned committed batch");
    let (execution, terminal) = reservation.into_terminal_ownership();
    execution.commit().complete(&HashSet::from([first.clone()]));
    assert!(dispatcher.pending_events(&client_id).is_empty());
    assert_eq!(dispatcher.budget_snapshot().current_count, 2);
    assert!(dispatcher.reservation_snapshot().retained_bytes > 0);

    drop(terminal);

    assert_eq!(dispatcher.pending_events(&client_id), vec![first.clone()]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 1);
    assert_eq!(dispatcher.take_pending_events(&client_id), vec![first]);
    assert_eq!(dispatcher.budget_snapshot().current_count, 0);
    assert_eq!(
        dispatcher.reservation_snapshot(),
        LiteEventReservationSnapshot::default()
    );
}
