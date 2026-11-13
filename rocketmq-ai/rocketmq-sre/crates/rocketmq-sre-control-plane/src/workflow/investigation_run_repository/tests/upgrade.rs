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

use std::borrow::Cow;

use sqlx::postgres::PgPoolOptions;

use super::*;

#[tokio::test]
#[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to Docker PostgreSQL"]
async fn migration_from_0059_preserves_saved_parent_and_old_workflow_state() {
    let admin = repository().await;
    // This test owns its UUID-named schema; no shared fixture or schema is reset.
    let schema = format!("run_upgrade_{}", Uuid::new_v4().simple());
    // The identifier consists solely of a fixed prefix and a generated UUID.
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin.pool)
        .await
        .unwrap();
    let search = schema.clone();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |conn, _| {
            let search = search.clone();
            Box::pin(async move {
                sqlx::query("SELECT set_config('search_path',$1,false)")
                    .bind(&search)
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    let mut old = sqlx::migrate!("../../migrations");
    old.migrations = Cow::Owned(
        old.iter()
            .filter(|migration| migration.version <= 59)
            .cloned()
            .collect(),
    );
    old.run(&pool).await.unwrap();
    let repo = PostgresRepository::from_pool(pool.clone());
    let (auth, scope) = seed_parent(&repo).await;
    sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
    let row = sqlx::query("SELECT status,resource FROM investigations WHERE id=$1")
        .bind(scope.investigation_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("status"), "open");
    assert_eq!(row.get::<String, _>("resource"), "cluster/overview");
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM investigation_runs WHERE tenant_id=$1 AND investigation_id=$2")
            .bind(auth.tenant_id.as_uuid())
            .bind(scope.investigation_id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0);
    let applied: i64 = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(applied, 60);
    pool.close().await;
    // Only this test's UUID-named schema is removed.
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin.pool)
        .await
        .unwrap();
}
