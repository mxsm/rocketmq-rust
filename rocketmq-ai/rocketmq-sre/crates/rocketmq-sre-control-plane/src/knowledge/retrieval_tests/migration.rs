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

use uuid::Uuid;

use super::support::*;

#[tokio::test]
#[ignore = "requires explicit task-owned PostgreSQL"]
async fn postgres_forward_migration_preserves_old_ids_flags_history_and_references() {
    let fixture = Fixture::new().await;
    let mut transaction = fixture.repository.pool.begin().await.unwrap();
    // A separate transactional schema exercises the actual old and new SQL,
    // including populated references, without changing the shared test schema.
    let schema = format!("d22_migration_{}", Uuid::new_v4().simple());
    // The only dynamic identifier is this task-generated UUID plus a fixed
    // prefix. No request, environment or source content enters the SQL text.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE SCHEMA {schema}; SET LOCAL search_path={schema},public; CREATE TABLE clusters(id UUID PRIMARY KEY);"
    )))
    .execute(&mut *transaction)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!("../../../../../migrations/0006_knowledge.sql"))
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../../../../../migrations/0010_knowledge_validity.sql"))
        .execute(&mut *transaction)
        .await
        .unwrap();
    let first_cluster = Uuid::new_v4();
    let second_cluster = Uuid::new_v4();
    let tenant = Uuid::new_v4();
    let global = Uuid::new_v4();
    let scoped = Uuid::new_v4();
    let review = Uuid::new_v4();
    let chunk = Uuid::new_v4();
    let feedback = Uuid::new_v4();
    sqlx::query("INSERT INTO clusters VALUES($1),($2)")
        .bind(first_cluster)
        .bind(second_cluster)
        .execute(&mut *transaction)
        .await
        .unwrap();
    for (id, cluster, source) in [
        (global, None, "https://legacy.invalid/global"),
        (scoped, Some(first_cluster), "https://legacy.invalid/scoped"),
    ] {
        sqlx::query("INSERT INTO knowledge_items(id,tenant_id,cluster_id,title,component,rocketmq_version_range,source_uri,source_version,owner_name,
            review_status,review_due_at,sensitivity,content_hash,conflict,created_at,updated_at)
            VALUES($1,$2,$3,'Legacy','consumer','*',$4,'v1','owner','validated',NOW()+INTERVAL '1 day','internal',$5,TRUE,NOW(),NOW())")
            .bind(id).bind(tenant).bind(cluster).bind(source).bind(format!("sha256:{}","1".repeat(64))).execute(&mut *transaction).await.unwrap();
    }
    sqlx::query("INSERT INTO knowledge_review_tasks VALUES($1,$2,'opaque-actor: old task','completed',NOW(),NOW())")
        .bind(review)
        .bind(scoped)
        .execute(&mut *transaction)
        .await
        .unwrap();
    let chunk_hash = format!("sha256:{}", "2".repeat(64));
    sqlx::query("INSERT INTO knowledge_chunks(id,knowledge_item_id,ordinal,heading,content,content_hash) VALUES($1,$2,0,'Legacy heading','Legacy content',$3)")
        .bind(chunk).bind(scoped).bind(&chunk_hash).execute(&mut *transaction).await.unwrap();
    sqlx::query("INSERT INTO knowledge_feedback(id,knowledge_item_id,tenant_id,cluster_id,kind,comment,created_by_subject,created_at)
        VALUES($1,$2,$3,$4,'incorrect','Legacy counterexample','legacy-feedback-actor',NOW())")
        .bind(feedback).bind(scoped).bind(tenant).bind(first_cluster).execute(&mut *transaction).await.unwrap();
    sqlx::raw_sql("CREATE TABLE published_reference (knowledge_id UUID REFERENCES knowledge_items(id));")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("INSERT INTO published_reference VALUES($1)")
        .bind(scoped)
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../../../../../migrations/0065_reviewed_knowledge.sql"))
        .execute(&mut *transaction)
        .await
        .unwrap();
    let old_ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM knowledge_items ORDER BY id")
        .fetch_all(&mut *transaction)
        .await
        .unwrap();
    assert!(old_ids.contains(&global) && old_ids.contains(&scoped));
    let flagged: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM knowledge_items WHERE conflict")
        .fetch_one(&mut *transaction)
        .await
        .unwrap();
    assert_eq!(flagged, 2);
    let legacy: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT actor_subject,event_kind FROM knowledge_review_tasks WHERE id=$1")
            .bind(review)
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    assert_eq!(legacy, (None, None));
    let preserved_chunk: (Uuid, Uuid, String, String) =
        sqlx::query_as("SELECT id,knowledge_item_id,content,content_hash FROM knowledge_chunks WHERE id=$1")
            .bind(chunk)
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    assert_eq!(
        preserved_chunk,
        (chunk, scoped, "Legacy content".to_owned(), chunk_hash)
    );
    let preserved_feedback: (Uuid, Uuid, String) =
        sqlx::query_as("SELECT id,knowledge_item_id,comment FROM knowledge_feedback WHERE id=$1")
            .bind(feedback)
            .fetch_one(&mut *transaction)
            .await
            .unwrap();
    assert_eq!(
        preserved_feedback,
        (feedback, scoped, "Legacy counterexample".to_owned())
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>("SELECT knowledge_id FROM published_reference")
            .fetch_one(&mut *transaction)
            .await
            .unwrap(),
        scoped
    );
    // Same content in another actual scope is now legitimate; old ID stays put.
    sqlx::query("INSERT INTO knowledge_items SELECT $1,tenant_id,$2,title,component,rocketmq_version_range,source_uri,source_version,owner_name,
        review_status,review_due_at,sensitivity,content_hash,conflict,created_at,updated_at,valid_from,valid_until FROM knowledge_items WHERE id=$3")
        .bind(Uuid::new_v4()).bind(second_cluster).bind(scoped).execute(&mut *transaction).await.unwrap();
    for duplicate in [global, scoped] {
        sqlx::raw_sql("SAVEPOINT duplicate_check")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let error=sqlx::query("INSERT INTO knowledge_items SELECT $1,tenant_id,cluster_id,title,component,rocketmq_version_range,source_uri,source_version,owner_name,
            review_status,review_due_at,sensitivity,content_hash,conflict,created_at,updated_at,valid_from,valid_until FROM knowledge_items WHERE id=$2")
            .bind(Uuid::new_v4()).bind(duplicate).execute(&mut *transaction).await.unwrap_err();
        assert_eq!(
            error.as_database_error().and_then(|error| error.code()).as_deref(),
            Some("23505")
        );
        sqlx::raw_sql("ROLLBACK TO SAVEPOINT duplicate_check")
            .execute(&mut *transaction)
            .await
            .unwrap();
    }
    transaction.rollback().await.unwrap();
}
