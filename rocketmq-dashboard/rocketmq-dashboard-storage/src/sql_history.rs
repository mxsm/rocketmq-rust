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

use super::with_pool;
use super::*;

impl SqlStore {
    pub(crate) async fn acquire_history_lease(
        &self,
        environment_id: EnvironmentId,
        holder_id: String,
        ttl_ms: i64,
    ) -> Result<Option<HistoryLease>, StorageError> {
        let document = self
            .load_desktop()
            .await?
            .ok_or_else(|| StorageError::new(StorageErrorKind::Uninitialized))?
            .document;
        if !document.profiles.iter().any(|profile| profile.id == environment_id) {
            return Err(StorageError::new(StorageErrorKind::Validation));
        }
        with_pool!(self, pool, {
            let mut transaction = pool.begin_with(self.begin_statement()).await.map_err(query_error)?;
            let prefix = if self.engine == StorageEngine::MySql {
                "INSERT IGNORE"
            } else {
                "INSERT"
            };
            let mut insert = QueryBuilder::new(prefix);
            insert.push(" INTO native_dashboard_history_lease (environment_id, holder_id, fencing_token, expires_at_ms) VALUES (")
                .push_bind(environment_id.as_str()).push(", '', 0, 0)");
            if self.engine != StorageEngine::MySql {
                insert.push(" ON CONFLICT (environment_id) DO NOTHING");
            }
            insert.build().execute(&mut *transaction).await.map_err(query_error)?;
            let mut update = QueryBuilder::new("UPDATE native_dashboard_history_lease SET holder_id = ");
            update
                .push_bind(&holder_id)
                .push(", fencing_token = fencing_token + 1, expires_at_ms = ")
                .push(self.clock())
                .push(" + ")
                .push_bind(ttl_ms)
                .push(" WHERE environment_id = ")
                .push_bind(environment_id.as_str())
                .push(" AND expires_at_ms <= ")
                .push(self.clock())
                .push(" AND fencing_token < ")
                .push_bind(i64::MAX);
            let acquired = update
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(query_error)?
                .rows_affected()
                == 1;
            let lease = if acquired {
                let mut select = QueryBuilder::new(
                    "SELECT fencing_token, expires_at_ms FROM native_dashboard_history_lease WHERE environment_id = ",
                );
                select.push_bind(environment_id.as_str());
                let row = select.build().fetch_one(&mut *transaction).await.map_err(query_error)?;
                Some(HistoryLease {
                    environment_id,
                    holder_id,
                    fencing_token: row.try_get(0).map_err(query_error)?,
                    expires_at_epoch_ms: row.try_get(1).map_err(query_error)?,
                })
            } else {
                None
            };
            transaction.commit().await.map_err(query_error)?;
            Ok(lease)
        })
    }

    pub(crate) async fn renew_history_lease(
        &self,
        lease: HistoryLease,
        ttl_ms: i64,
    ) -> Result<Option<HistoryLease>, StorageError> {
        with_pool!(self, pool, {
            let mut transaction = pool.begin_with(self.begin_statement()).await.map_err(query_error)?;
            let mut update = QueryBuilder::new("UPDATE native_dashboard_history_lease SET expires_at_ms = ");
            update
                .push(self.clock())
                .push(" + ")
                .push_bind(ttl_ms)
                .push(" WHERE environment_id = ")
                .push_bind(lease.environment_id.as_str())
                .push(" AND holder_id = ")
                .push_bind(&lease.holder_id)
                .push(" AND fencing_token = ")
                .push_bind(lease.fencing_token)
                .push(" AND expires_at_ms > ")
                .push(self.clock());
            let renewed = update
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(query_error)?
                .rows_affected()
                == 1;
            let result = if renewed {
                let mut select = QueryBuilder::new(
                    "SELECT expires_at_ms FROM native_dashboard_history_lease WHERE environment_id = ",
                );
                select.push_bind(lease.environment_id.as_str());
                let row = select.build().fetch_one(&mut *transaction).await.map_err(query_error)?;
                Some(HistoryLease {
                    expires_at_epoch_ms: row.try_get(0).map_err(query_error)?,
                    ..lease
                })
            } else {
                None
            };
            transaction.commit().await.map_err(query_error)?;
            Ok(result)
        })
    }

    pub(crate) async fn release_history_lease(&self, lease: HistoryLease) -> Result<bool, StorageError> {
        with_pool!(self, pool, {
            let mut update = QueryBuilder::new(
                "UPDATE native_dashboard_history_lease SET expires_at_ms = 0 WHERE environment_id = ",
            );
            update
                .push_bind(lease.environment_id.as_str())
                .push(" AND holder_id = ")
                .push_bind(lease.holder_id)
                .push(" AND fencing_token = ")
                .push_bind(lease.fencing_token)
                .push(" AND expires_at_ms > ")
                .push(self.clock());
            Ok(update.build().execute(pool).await.map_err(query_error)?.rows_affected() == 1)
        })
    }

    pub(crate) async fn append_history(
        &self,
        lease: HistoryLease,
        sample: Vec<HistoryPoint>,
        retention: HistoryRetention,
    ) -> Result<HistoryAppendOutcome, StorageError> {
        with_pool!(self, pool, {
            let mut transaction = pool.begin_with(self.begin_statement()).await.map_err(query_error)?;
            // Lock the ownership row before the history row. All holders use the same lock order,
            // so a peer cannot replace the fence between validation and the retention commit.
            let mut select =
                QueryBuilder::new("SELECT fencing_token FROM native_dashboard_history_lease WHERE environment_id = ");
            select
                .push_bind(lease.environment_id.as_str())
                .push(" AND holder_id = ")
                .push_bind(&lease.holder_id)
                .push(" AND fencing_token = ")
                .push_bind(lease.fencing_token)
                .push(" AND expires_at_ms > ")
                .push(self.clock())
                .push(self.row_lock());
            if select
                .build()
                .fetch_optional(&mut *transaction)
                .await
                .map_err(query_error)?
                .is_none()
            {
                return Err(StorageError::new(StorageErrorKind::LeaseLost));
            }
            let mut select = QueryBuilder::new(
                "SELECT storage_revision, payload FROM native_dashboard_document WHERE document_name = 'history'",
            );
            select.push(self.row_lock());
            let row = select
                .build()
                .fetch_optional(&mut *transaction)
                .await
                .map_err(query_error)?
                .ok_or_else(|| StorageError::new(StorageErrorKind::Uninitialized))?;
            let revision = StorageRevision(row.try_get(0).map_err(query_error)?);
            let payload: String = row.try_get(1).map_err(query_error)?;
            let (outcome, replacement) = self
                .context
                .storage_io()
                .spawn_io("native-sql-history-retain", move || {
                    let mut history: HistoryDocument = decode(&payload)?;
                    history.validate()?;
                    let before = history.clone();
                    let outcome = history.append(
                        sample
                            .into_iter()
                            .map(|point| EnvironmentHistoryPoint {
                                environment_id: Some(lease.environment_id.clone()),
                                point,
                            })
                            .collect(),
                        retention,
                    );
                    let replacement = if history != before {
                        Some(encode(&history)?)
                    } else {
                        None
                    };
                    Ok::<_, StorageError>((outcome, replacement))
                })
                .await
                .map_err(|_| StorageError::new(StorageErrorKind::Runtime))??;
            if let Some(payload) = replacement {
                let mut update = QueryBuilder::new("UPDATE native_dashboard_document SET payload = ");
                update
                    .push_bind(payload)
                    .push(", storage_revision = ")
                    .push_bind(revision.next()?.0)
                    .push(", written_at_ms = ")
                    .push(self.clock())
                    .push(" WHERE document_name = 'history' AND storage_revision = ")
                    .push_bind(revision.0);
                if update
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .map_err(query_error)?
                    .rows_affected()
                    != 1
                {
                    return Err(StorageError::conflict());
                }
            }
            transaction.commit().await.map_err(query_error)?;
            Ok(outcome)
        })
    }
}
