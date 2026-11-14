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

use std::future::Future;
use std::task::Poll;

use rocketmq_runtime::RuntimeContext;
use rocketmq_sre_contracts::*;
use sqlx::{PgPool, postgres::PgPoolOptions};

use super::*;
use crate::api::capability_verification_test_state;
use crate::connector_channel::{ConnectorCommand, ConnectorPrincipal, PostgresConnectorChannelService, channel_schema};

struct Fixture {
    admin: PostgresRepository,
    repository: PostgresRepository,
    service: ForecastService,
    channel: PostgresConnectorChannelService,
    auth: AuthContext,
    principal: ConnectorPrincipal,
    session: ConnectorSessionId,
    schema: String,
}

impl Fixture {
    async fn new() -> Self {
        let url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("explicit disposable PostgreSQL");
        let admin = PostgresRepository::connect(&url, 2).await.unwrap();
        let schema = format!("forecast_lifecycle_{}", uuid::Uuid::new_v4().simple());
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin.pool)
            .await
            .unwrap();
        let pool = scoped_pool(&url, &schema).await;
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let repository = PostgresRepository::from_pool(pool);
        let tenant = TenantId::new();
        let cluster = ClusterId::new();
        sqlx::query("INSERT INTO clusters(id,tenant_id,external_cluster_key,environment,region,rocketmq_version,deployment_mode,owner_name,requested_access_profile,effective_access_profile,onboarding_state) VALUES($1,$2,$3,'test','local','test','test','forecast-lifecycle','read_only','read_only','ready_read_only')")
            .bind(cluster.as_uuid()).bind(tenant.to_string()).bind(format!("forecast-{cluster}"))
            .execute(&repository.pool).await.unwrap();
        let principal = ConnectorPrincipal {
            subject: "forecast-connector".to_owned(),
            issuer: "forecast-test".to_owned(),
        };
        sqlx::query("INSERT INTO connector_identities(id,cluster_id,subject,issuer) VALUES($1,$2,$3,$4)")
            .bind(uuid::Uuid::new_v4())
            .bind(cluster.as_uuid())
            .bind(&principal.subject)
            .bind(&principal.issuer)
            .execute(&repository.pool)
            .await
            .unwrap();
        let mut state = capability_verification_test_state(repository.clone()).unwrap();
        let config = Arc::make_mut(&mut state.forecast.config);
        config.targets.truncate(2);
        config.windows.truncate(1);
        config.max_evaluations_per_run = 2;
        let auth = AuthContext {
            tenant_id: tenant,
            subject: "forecast-test".to_owned(),
            clusters: BTreeSet::from([cluster]),
            roles: BTreeSet::from(["diagnose".to_owned()]),
        };
        let session = ConnectorSessionId::new();
        state
            .connector_channel
            .register(
                &principal,
                &ConnectorRegister {
                    schema: channel_schema(),
                    session_id: session,
                    tenant_id: tenant,
                    cluster_id: cluster,
                    subject: principal.subject.clone(),
                    observed_at: Utc::now(),
                    capability: ConnectorCapabilityState {
                        mutation_supported: false,
                        sources: vec![ConnectorSourceCapability {
                            source: "prometheus".to_owned(),
                            schema_major: 1,
                            status: ConnectorSourceStatus::Queryable,
                            max_rows: 8192,
                            max_bytes: 655360,
                            max_time_range_seconds: 31 * 86400,
                            last_success_at: None,
                            freshness_seconds: Some(0),
                        }],
                    },
                },
            )
            .await
            .unwrap();
        Self {
            admin,
            repository,
            service: state.forecast,
            channel: state.connector_channel,
            auth,
            principal,
            session,
            schema,
        }
    }

    async fn first_query(&self) -> ConnectorQueryEnvelope {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let command: Option<serde_json::Value> = sqlx::query_scalar("SELECT command_payload FROM connector_channel_commands WHERE session_id=$1 AND command_kind='query' ORDER BY sequence LIMIT 1")
                    .bind(self.session.as_uuid()).fetch_optional(&self.repository.pool).await.unwrap();
                if let Some(command) = command {
                    let ConnectorCommand::Query { envelope } = serde_json::from_value(command).unwrap() else { panic!("query row"); };
                    return envelope;
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("owned scheduler reached a durable query")
    }

    async fn respond(&self, query: ConnectorQueryEnvelope) {
        let kind = if query.query.resource.starts_with("trend/30d/") {
            "trend_30d"
        } else {
            "trend_7d"
        };
        let metric = query.query.resource.splitn(3, '/').nth(2).unwrap().to_owned();
        let snapshot = EvidenceSnapshot::capture(query.query, current_evidence_schema(), Utc::now(), EvidenceContent::Inline(serde_json::json!({ "schema_version": "rocketmq.prometheus-evidence.v1", "query_kind": kind, "metric": metric, "series": [] }))).unwrap();
        self.channel
            .submit_response(
                &self.principal,
                self.session,
                &ConnectorResponseEnvelope {
                    schema: channel_schema(),
                    session_id: self.session,
                    correlation_id: query.correlation_id,
                    sequence: query.sequence,
                    evidence: Some(snapshot),
                    error_code: None,
                    retryable: false,
                },
            )
            .await
            .unwrap();
    }

    async fn close(self) {
        self.repository.pool.close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {} CASCADE", self.schema)))
            .execute(&self.admin.pool)
            .await
            .unwrap();
    }
}

async fn scoped_pool(url: &str, schema: &str) -> PgPool {
    let schema = schema.to_owned();
    PgPoolOptions::new()
        .max_connections(6)
        .after_connect(move |connection, _| {
            let schema = schema.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path',$1,false),set_config('application_name',$1,false)")
                    .bind(schema)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .unwrap()
}

fn immediate_schedule() -> ScheduledTaskConfig {
    ScheduledTaskConfig::fixed_delay("forecast-lifecycle", Duration::from_secs(60))
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL; creates an isolated UUID schema"]
async fn postgres_forecast_shutdown_cancels_pending_read_without_fallback_or_next_tuple() {
    let fixture = Fixture::new().await;
    let config = &fixture.service.config;
    let now = Utc::now();
    let saved = EvidenceSnapshot::capture(EvidenceQuery { query_id: QueryId::new(), correlation_id: CorrelationId::new(), tenant_id: fixture.auth.tenant_id, cluster_id: *fixture.auth.clusters.first().unwrap(), source: "prometheus".to_owned(), resource: format!("trend/7d/{}", config.targets[0].metric), time_range: TimeRange::new(now - chrono::Duration::days(7), now).unwrap() }, current_evidence_schema(), now, EvidenceContent::Inline(serde_json::json!({"schema_version":"rocketmq.prometheus-evidence.v1","query_kind":"trend_7d","metric":config.targets[0].metric,"series":[]}))).unwrap();
    fixture
        .service
        .evidence
        .persist_cluster(&fixture.auth, saved)
        .await
        .unwrap();
    let runtime = RuntimeContext::from_current("forecast-pg-read-stop");
    let owner = runtime.service_context("forecast");
    fixture
        .service
        .schedule_configured(&owner, immediate_schedule())
        .unwrap();
    let query = fixture.first_query().await;
    let report = tokio::time::timeout(
        Duration::from_secs(4),
        owner.task_group().shutdown(Duration::from_secs(3)),
    )
    .await
    .unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
    let commands: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT command_payload FROM connector_channel_commands WHERE session_id=$1 ORDER BY sequence",
    )
    .bind(fixture.session.as_uuid())
    .fetch_all(&fixture.repository.pool)
    .await
    .unwrap();
    assert_eq!(commands.len(), 2);
    let cancel: ConnectorCommand = serde_json::from_value(commands[1].clone()).unwrap();
    assert!(matches!(cancel, ConnectorCommand::Cancel { .. }));
    assert_eq!(cancel.correlation_id(), query.correlation_id);
    // A late remote response remains durable channel input; it cannot revive the stopped worker.
    fixture.respond(query).await;
    let artifacts: (i64, i64) =
        sqlx::query_as("SELECT (SELECT COUNT(*) FROM capacity_forecasts),(SELECT COUNT(*) FROM evidence_snapshots)")
            .fetch_one(&fixture.repository.pool)
            .await
            .unwrap();
    assert_eq!(artifacts, (0, 1));
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires disposable Docker PostgreSQL; creates an isolated UUID schema"]
async fn postgres_forecast_shutdown_drains_a_started_write_without_admitting_the_next_artifact() {
    let mut fixture = Fixture::new().await;
    let mut config = (*fixture.service.config).clone();
    config.windows = vec![config.windows[0].clone()];
    config.windows[0].window = ForecastWindow::ThirtyDays;
    fixture.service.config = Arc::new(config);
    let mut barrier = fixture.repository.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE capacity_forecasts IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *barrier)
        .await
        .unwrap();
    let runtime = RuntimeContext::from_current("forecast-pg-write-drain");
    let owner = runtime.service_context("forecast");
    fixture
        .service
        .schedule_configured(&owner, immediate_schedule())
        .unwrap();
    fixture.respond(fixture.first_query().await).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%INSERT INTO capacity_forecasts%')").bind(&fixture.schema).fetch_one(&fixture.admin.pool).await.unwrap();
            if waiting { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("forecast reached the controlled persistence lock");
    owner.task_group().cancel();
    let shutdown = owner.task_group().shutdown(Duration::from_secs(3));
    tokio::pin!(shutdown);
    std::future::poll_fn(|context| {
        assert!(shutdown.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    barrier.rollback().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(4), shutdown).await.unwrap();
    assert!(report.is_healthy(), "{}", report.to_json());
    let artifacts: (i64,i64,i64,i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM capacity_forecasts),(SELECT COUNT(*) FROM anomaly_assessments),(SELECT COUNT(*) FROM change_points),(SELECT COUNT(*) FROM connector_channel_commands WHERE command_kind='query')").fetch_one(&fixture.repository.pool).await.unwrap();
    assert_eq!(artifacts, (1, 0, 0, 1));
    fixture.close().await;
}
