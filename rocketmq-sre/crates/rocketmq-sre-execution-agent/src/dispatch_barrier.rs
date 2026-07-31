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

use rocketmq_sre_contracts::ClusterId;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::pool::PoolConnection;

use crate::ExecutionAgentError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BarrierMode {
    Shared,
    Exclusive,
}

/// PostgreSQL session-level shared/exclusive barrier across Agent replicas.
#[derive(Clone, Debug)]
pub struct DispatchBarrier {
    pool: PgPool,
}

impl DispatchBarrier {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Acquires the shared side held from final grant validation until the
    /// driver result is durably recorded.
    ///
    /// # Errors
    ///
    /// Fails closed when PostgreSQL is unavailable.
    pub async fn acquire_dispatch(&self, cluster_id: ClusterId) -> Result<DispatchBarrierGuard, ExecutionAgentError> {
        self.acquire(cluster_id, BarrierMode::Shared).await
    }

    /// Acquires the exclusive side used by `AdvanceFence`.
    ///
    /// # Errors
    ///
    /// Fails closed when PostgreSQL is unavailable.
    pub async fn acquire_fence(&self, cluster_id: ClusterId) -> Result<DispatchBarrierGuard, ExecutionAgentError> {
        self.acquire(cluster_id, BarrierMode::Exclusive).await
    }

    async fn acquire(
        &self,
        cluster_id: ClusterId,
        mode: BarrierMode,
    ) -> Result<DispatchBarrierGuard, ExecutionAgentError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| ExecutionAgentError::DispatchBarrierUnavailable)?;
        let statement = match mode {
            BarrierMode::Shared => "SELECT pg_advisory_lock_shared(hashtextextended($1, 731))",
            BarrierMode::Exclusive => "SELECT pg_advisory_lock(hashtextextended($1, 731))",
        };
        sqlx::query(statement)
            .bind(cluster_id.to_string())
            .execute(&mut *connection)
            .await
            .map_err(|_| ExecutionAgentError::DispatchBarrierUnavailable)?;
        Ok(DispatchBarrierGuard {
            connection: Some(connection),
            cluster_id,
            mode,
        })
    }
}

/// Held barrier connection. Dropping without `release` closes the connection
/// instead of returning a still-locked session to the pool.
pub struct DispatchBarrierGuard {
    connection: Option<PoolConnection<Postgres>>,
    cluster_id: ClusterId,
    mode: BarrierMode,
}

impl DispatchBarrierGuard {
    /// Explicitly unlocks and returns the session to the pool.
    ///
    /// # Errors
    ///
    /// Closes the session and returns a fail-closed barrier error when unlock
    /// cannot be confirmed.
    pub async fn release(mut self) -> Result<(), ExecutionAgentError> {
        let mut connection = self
            .connection
            .take()
            .ok_or(ExecutionAgentError::DispatchBarrierUnavailable)?;
        let statement = match self.mode {
            BarrierMode::Shared => "SELECT pg_advisory_unlock_shared(hashtextextended($1, 731))",
            BarrierMode::Exclusive => "SELECT pg_advisory_unlock(hashtextextended($1, 731))",
        };
        let unlocked: Result<bool, sqlx::Error> = sqlx::query_scalar(statement)
            .bind(self.cluster_id.to_string())
            .fetch_one(&mut *connection)
            .await;
        match unlocked {
            Ok(true) => Ok(()),
            Ok(false) | Err(_) => {
                connection.close_on_drop();
                Err(ExecutionAgentError::DispatchBarrierUnavailable)
            }
        }
    }
}

impl Drop for DispatchBarrierGuard {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}
