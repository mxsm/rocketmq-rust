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

use chrono::{Duration, Utc};
use rocketmq_sre_contracts::{ClusterId, ExecutionAction, TenantId};
use rocketmq_sre_core::{DynamicSafetyFacts, EligibilityEngine};

use crate::PostgresRepository;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; real freeze state with explicitly Synthetic other policy facts"]
async fn postgres_d21_live_freeze_changes_the_intended_guard_without_an_execution_or_grant() {
    let database = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit disposable database");
    let repository = PostgresRepository::connect(&database, 3).await.unwrap();
    let tenant = TenantId::new();
    let cluster = ClusterId::new();
    sqlx::query("INSERT INTO clusters(id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,
        requested_access_profile,effective_access_profile,onboarding_state)
        VALUES($1,$2,$3,'test','task','unknown','SyntheticFixture','d21-freeze-control','read_only','read_only','pending')")
        .bind(cluster.as_uuid()).bind(tenant.to_string()).bind(format!("d21-freeze-{cluster}"))
        .execute(&repository.pool).await.unwrap();
    let now = Utc::now();
    let action = ExecutionAction::ObservabilityLoggerLevelTtl;
    let state = repository
        .autonomy_freeze_state(tenant, cluster, action, "1.0.0", now)
        .await
        .unwrap();
    assert!(!state.1);
    // These are deterministic policy controls, not observed physical sources or
    // normal CP admission. Changing only persisted freeze must flip its guard.
    let control = DynamicSafetyFacts {
        authoritative_sources_available: true,
        error_budget_available: true,
        freeze_active: state.1,
        kill_switch_active: false,
        evidence_fresh: true,
        policy_definition_matches: true,
        lifecycle_revision_matches: true,
    };
    assert!(EligibilityEngine::evaluate_dynamic_safety(control).allowed);
    let freeze = repository
        .set_autonomy_freeze(
            tenant,
            Some(cluster),
            Some(action),
            Some("1.0.0"),
            true,
            "isolated deterministic freeze control",
            now,
            Some(now + Duration::minutes(5)),
            "d21-test-owner",
        )
        .await
        .unwrap();
    let active = repository
        .autonomy_freeze_state(tenant, cluster, action, "1.0.0", now)
        .await
        .unwrap();
    assert!(active.1 && active.0 == freeze.revision);
    let denied = EligibilityEngine::evaluate_dynamic_safety(DynamicSafetyFacts {
        freeze_active: active.1,
        ..control
    });
    assert!(!denied.allowed);
    assert_eq!(denied.reason_codes, ["freeze_active"]);
    let other_action = repository
        .autonomy_freeze_state(tenant, cluster, ExecutionAction::ProxyScaleOutOne, "1.0.0", now)
        .await
        .unwrap();
    assert!(
        !other_action.1,
        "an exact action freeze must not invent an unrelated target freeze"
    );
    let expired = repository
        .autonomy_freeze_state(tenant, cluster, action, "1.0.0", now + Duration::minutes(5))
        .await
        .unwrap();
    assert!(!expired.1);
    assert!(
        EligibilityEngine::evaluate_dynamic_safety(DynamicSafetyFacts {
            freeze_active: expired.1,
            ..control
        })
        .allowed
    );
    let executions: i64 = sqlx::query_scalar("SELECT count(*) FROM executions WHERE tenant_id=$1")
        .bind(tenant.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
    let effects: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_agent_effects WHERE tenant_id=$1")
        .bind(tenant.as_uuid())
        .fetch_one(&repository.pool)
        .await
        .unwrap();
    assert_eq!((executions, effects), (0, 0));
}
