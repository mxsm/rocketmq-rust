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

use chrono::{DateTime, Utc};
use rocketmq_runtime::TaskGroup;

use super::*;

const OWNER_CANCEL_ENQUEUE_LIMIT: Duration = Duration::from_secs(1);

pub(super) struct QueryWaiter {
    pub(super) session_id: ConnectorSessionId,
    pub(super) sequence: u64,
    pub(super) correlation_id: CorrelationId,
    response_signal: watch::Receiver<u64>,
}

impl<S: ConnectorChannelStore> ConnectorChannelService<S> {
    pub(super) async fn begin_query_wait(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        query: EvidenceQuery,
        deadline: DateTime<Utc>,
    ) -> Result<QueryWaiter, ControlPlaneRequestFailure> {
        let response_signal = self.response_signal.subscribe();
        let command = self.enqueue_query(tenant_id, cluster_id, query, deadline).await?;
        let ConnectorCommand::Query { envelope } = command else {
            return Err(ControlPlaneRequestFailure::configuration(
                "connector query was persisted as an invalid command kind",
            ));
        };
        Ok(QueryWaiter {
            session_id: envelope.session_id,
            sequence: envelope.sequence,
            correlation_id: envelope.correlation_id,
            response_signal,
        })
    }

    /// This future performs reads and notification waits only. Its caller may
    /// drop it without interrupting query admission or Evidence persistence.
    pub(super) async fn wait_query_response(
        &self,
        pending: &mut QueryWaiter,
        deadline: DateTime<Utc>,
    ) -> Result<Option<ConnectorResponseEnvelope>, ControlPlaneRequestFailure> {
        loop {
            if let Some(response) = self.store.response(pending.session_id, pending.sequence).await? {
                return Ok(Some(response));
            }
            let Ok(remaining) = (deadline - Utc::now()).to_std() else {
                return Ok(None);
            };
            if remaining.is_zero()
                || tokio::time::timeout(remaining, pending.response_signal.changed())
                    .await
                    .is_err()
            {
                return Ok(None);
            }
        }
    }

    /// Waits within the caller's existing owner. `None` means local owner
    /// cancellation, not a measured zero-cost read or a remote cancellation ACK.
    /// Query admission drains its transaction; only the read-only response wait
    /// is dropped. The durable deadline remains effective if cancel cannot be
    /// enqueued within the bounded cleanup interval.
    pub(crate) async fn query_and_wait_owned(
        &self,
        tenant_id: TenantId,
        cluster_id: ClusterId,
        query: EvidenceQuery,
        deadline: DateTime<Utc>,
        owner: &TaskGroup,
    ) -> Result<Option<ConnectorResponseEnvelope>, ControlPlaneRequestFailure> {
        let cancellation = owner.cancellation_token();
        if cancellation.is_cancelled() {
            return Ok(None);
        }
        // Do not race the commit of a durable query against shutdown. If the
        // database itself stalls, the owner must honestly report that drain.
        let mut pending = self.begin_query_wait(tenant_id, cluster_id, query, deadline).await?;
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => None,
            response = self.wait_query_response(&mut pending, deadline) => Some(response?),
        };
        if let Some(Some(response)) = response {
            return Ok(Some(response));
        }
        // Cancellation is intent only. A timed-out cleanup may have committed;
        // it never changes the original query's deadline or fabricates a result.
        let _ = tokio::time::timeout(
            OWNER_CANCEL_ENQUEUE_LIMIT,
            self.enqueue_cancel(tenant_id, cluster_id, pending.correlation_id),
        )
        .await;
        Ok(response.map(|_| deadline_response(pending.session_id, pending.sequence, pending.correlation_id)))
    }
}
