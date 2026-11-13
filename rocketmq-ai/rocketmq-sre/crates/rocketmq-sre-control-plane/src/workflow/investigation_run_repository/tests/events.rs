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

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn event_pages_are_bounded_contiguous_and_recover_old_future_or_gapped_cursors() {
    let (repo, auth, ctx, request) = fixture().await;
    repo.investigation_run_create(&auth, &ctx, &request, Utc::now())
        .await
        .unwrap();
    for _ in 0..130 {
        let lease = repo
            .investigation_run_claim(&auth, request.run_id, Uuid::new_v4(), 60, Utc::now())
            .await
            .unwrap()
            .unwrap();
        repo.investigation_run_release(&auth, request.run_id, &lease.fence, Utc::now())
            .await
            .unwrap();
    }
    let old = repo
        .investigation_run_events(&auth, request.run_id, 0, 100)
        .await
        .unwrap();
    old.validate().unwrap();
    assert_eq!(old.latest_sequence.get(), 261);
    assert_eq!(old.earliest_sequence.get(), 6);
    assert_eq!(old.recovery_reason, Some(InvestigationCursorRecovery::CursorExpired));
    assert_eq!(
        old.current_snapshot.as_ref().unwrap().status,
        InvestigationRunStatus::Queued
    );
    let start = old.earliest_sequence.get() - 1;
    let mut cursor = start;
    let mut sequences = vec![];
    loop {
        let page = repo
            .investigation_run_events(&auth, request.run_id, cursor, 100)
            .await
            .unwrap();
        let duplicate = repo
            .investigation_run_events(&auth, request.run_id, cursor, 100)
            .await
            .unwrap();
        assert_eq!(page, duplicate);
        assert!(!page.snapshot_required);
        page.validate().unwrap();
        assert!(page.events.len() <= 100);
        sequences.extend(page.events.iter().map(|event| event.sequence.get()));
        cursor = page.next_sequence.get();
        if !page.has_more {
            break;
        }
    }
    assert_eq!(sequences, (start + 1..=261).collect::<Vec<_>>());
    let empty = repo
        .investigation_run_events(&auth, request.run_id, cursor, 100)
        .await
        .unwrap();
    assert!(empty.events.is_empty() && !empty.has_more && !empty.snapshot_required);
    let future = repo
        .investigation_run_events(&auth, request.run_id, u64::MAX, 10)
        .await
        .unwrap();
    assert_eq!(future.recovery_reason, Some(InvestigationCursorRecovery::CursorAhead));
    assert_eq!(future.next_sequence.get(), 261);
    sqlx::query("DELETE FROM investigation_run_events WHERE run_id=$1 AND sequence=$2")
        .bind(request.run_id.as_uuid())
        .bind((start + 2) as i64)
        .execute(&repo.pool)
        .await
        .unwrap();
    let gap = repo
        .investigation_run_events(&auth, request.run_id, start, 100)
        .await
        .unwrap();
    assert_eq!(gap.recovery_reason, Some(InvestigationCursorRecovery::RetainedEventGap));
    assert_eq!(gap.current_snapshot, old.current_snapshot);
    assert!(
        repo.investigation_run_events(&auth, request.run_id, 0, 101)
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn event_sequence_overflow_rolls_back_snapshot_and_receipt_in_one_transaction() {
    let (repo, auth, ctx, lease) = running().await;
    sqlx::query("UPDATE investigation_runs SET event_sequence=$1 WHERE id=$2")
        .bind(i64::MAX)
        .bind(lease.run.id.as_uuid())
        .execute(&repo.pool)
        .await
        .unwrap();
    let pause = command(&lease.run, InvestigationCommand::Pause {}, "sequence-exhausted");
    let error = repo
        .investigation_run_command(&auth, &ctx, &pause, Utc::now())
        .await
        .unwrap_err();
    assert_eq!(error.code(), "counter_exhausted");
    assert_eq!(
        repo.investigation_run_get(&auth, lease.run.id).await.unwrap(),
        lease.run
    );
    let receipt_count: i64 = sqlx::query_scalar("SELECT count(*) FROM investigation_run_commands WHERE run_id=$1")
        .bind(lease.run.id.as_uuid())
        .fetch_one(&repo.pool)
        .await
        .unwrap();
    assert_eq!(receipt_count, 0);
    let page = repo
        .investigation_run_events(&auth, lease.run.id, u64::MAX, 100)
        .await
        .unwrap();
    assert_eq!(page.latest_sequence.get(), i64::MAX as u64);
    assert_eq!(page.recovery_reason, Some(InvestigationCursorRecovery::CursorAhead));
}
