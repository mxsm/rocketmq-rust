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

use chrono::Duration;
use std::sync::atomic::Ordering;

use super::support::*;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; persisted human approval boundary, no new dispatch authority"]
async fn postgres_d21_original_human_approval_stops_at_its_exact_persisted_deadline() {
    let mut fixture = Fixture::new().await;
    fixture.confirm().await;
    fixture.prepare().await;
    let plan = fixture.draft().await;
    let grant = fixture.approve(&plan).await;
    let deadline = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT expires_at FROM approvals WHERE plan_id=$1 AND approval_grant_snapshot IS NOT NULL",
    )
    .bind(plan.id.as_uuid())
    .fetch_one(&fixture.repository.pool)
    .await
    .unwrap();
    // Isolate the production persisted-approval gate from the independent
    // qualification Unknown gate; an earlier universal denial is not coverage.
    let before = fixture
        .repository
        .current_approval_grant(&fixture.auth, &plan, deadline - Duration::microseconds(1))
        .await
        .unwrap()
        .unwrap();
    let at = fixture
        .repository
        .current_approval_grant(&fixture.auth, &plan, deadline)
        .await
        .unwrap();
    let after = fixture
        .repository
        .current_approval_grant(&fixture.auth, &plan, deadline + Duration::microseconds(1))
        .await
        .unwrap();
    assert_eq!(before, grant);
    assert!(at.is_none() && after.is_none());
    let stored = fixture
        .repository
        .latest_approval(&fixture.auth, &plan)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.expires_at, deadline);
    assert_eq!(stored.plan_hash, plan.plan_hash);
    assert_eq!(fixture.observer.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.observer.writes.load(Ordering::SeqCst), 0);
    fixture.close().await;
}
