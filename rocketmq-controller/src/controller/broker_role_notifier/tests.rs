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

use tokio::sync::mpsc;

use super::actor::Mailbox;
use super::actor::SubmitOutcome;
use super::NotifyKey;
use super::NotifyState;
use super::NotifyTask;

fn key(broker_id: u64) -> NotifyKey {
    NotifyKey {
        cluster_name: "test-cluster".to_string(),
        broker_name: "broker-a".to_string(),
        broker_id,
    }
}

fn state(master_epoch: i32, sync_state_set_epoch: i32) -> NotifyState {
    NotifyState {
        master_broker_id: 1,
        master_epoch,
        sync_state_set_epoch,
        master_address: Some("127.0.0.1:10911".to_string()),
    }
}

#[test]
fn broker_role_notifier_mailbox_is_bounded_by_retained_keys() {
    let (sender, _receiver) = mpsc::channel(2);
    let mut mailbox = Mailbox::new(2);
    mailbox.mark_started();

    assert_eq!(
        mailbox.submit(NotifyTask::new_for_test(key(1), state(1, 1)), false, &sender),
        SubmitOutcome::Accepted
    );
    assert_eq!(
        mailbox.submit(NotifyTask::new_for_test(key(2), state(1, 1)), false, &sender),
        SubmitOutcome::Accepted
    );
    assert_eq!(
        mailbox.submit(NotifyTask::new_for_test(key(3), state(1, 1)), false, &sender),
        SubmitOutcome::Full
    );

    let snapshot = mailbox.snapshot();
    assert_eq!(snapshot.retained_keys, 2);
    assert_eq!(snapshot.rejected_full, 1);
}

#[test]
fn broker_role_notifier_mailbox_coalesces_latest_state_per_key() {
    let (sender, mut receiver) = mpsc::channel(2);
    let mut mailbox = Mailbox::new(2);
    mailbox.mark_started();
    let notify_key = key(1);

    assert_eq!(
        mailbox.submit(
            NotifyTask::new_for_test(notify_key.clone(), state(1, 1)),
            false,
            &sender,
        ),
        SubmitOutcome::Accepted
    );
    assert_eq!(
        mailbox.submit(
            NotifyTask::new_for_test(notify_key.clone(), state(2, 2)),
            false,
            &sender,
        ),
        SubmitOutcome::Replaced
    );
    assert_eq!(
        mailbox.submit(
            NotifyTask::new_for_test(notify_key.clone(), state(2, 2)),
            false,
            &sender,
        ),
        SubmitOutcome::Coalesced
    );
    assert_eq!(
        mailbox.submit(
            NotifyTask::new_for_test(notify_key.clone(), state(1, 2)),
            false,
            &sender,
        ),
        SubmitOutcome::Stale
    );

    assert_eq!(receiver.try_recv(), Ok(notify_key.clone()));
    assert_eq!(mailbox.take(&notify_key).map(|task| task.state), Some(state(2, 2)));
    assert!(receiver.try_recv().is_err(), "replacement must not enqueue another key");
    assert_eq!(mailbox.snapshot().coalesced, 1);
}

#[test]
fn broker_role_notifier_retry_wait_remains_inside_retained_key_bound() {
    let (sender, mut receiver) = mpsc::channel(1);
    let mut mailbox = Mailbox::new(1);
    mailbox.mark_started();
    let notify_key = key(1);
    let task = NotifyTask::new_for_test(notify_key.clone(), state(1, 1));

    assert_eq!(mailbox.submit(task.clone(), false, &sender), SubmitOutcome::Accepted);
    assert_eq!(receiver.try_recv(), Ok(notify_key.clone()));
    assert_eq!(
        mailbox.take(&notify_key).as_ref().map(|task| &task.state),
        Some(&task.state)
    );
    assert_eq!(
        mailbox.submit(
            NotifyTask::new_for_test(notify_key.clone(), state(1, 1)),
            false,
            &sender,
        ),
        SubmitOutcome::Coalesced
    );
    assert!(mailbox.finish(&task, false, std::time::Duration::from_millis(1)));

    let snapshot = mailbox.snapshot();
    assert_eq!(snapshot.retry_waiting_keys, 1);
    assert_eq!(snapshot.retained_keys, 1);
    assert_eq!(
        mailbox.submit(NotifyTask::new_for_test(notify_key, state(1, 1)), false, &sender,),
        SubmitOutcome::Coalesced
    );
    assert_eq!(
        mailbox.submit(NotifyTask::new_for_test(key(2), state(1, 1)), false, &sender),
        SubmitOutcome::Full
    );
}

#[test]
fn broker_role_notifier_reset_invalidates_queued_generation() {
    let (sender, mut receiver) = mpsc::channel(1);
    let mut mailbox = Mailbox::new(1);
    mailbox.mark_started();
    let notify_key = key(1);

    assert_eq!(
        mailbox.submit(
            NotifyTask::new_for_test(notify_key.clone(), state(1, 1)),
            false,
            &sender,
        ),
        SubmitOutcome::Accepted
    );
    mailbox.reset();

    assert_eq!(receiver.try_recv(), Ok(notify_key.clone()));
    assert!(mailbox.take(&notify_key).is_none());
    assert_eq!(mailbox.snapshot().retained_keys, 0);
    assert!(!mailbox.notified_contains(&notify_key));
}
