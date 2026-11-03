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

//! One owned collector grant; attribution is captured before starting either RPC sampler.

use super::*;
use rocketmq_dashboard_storage::{HistoryLease, UnassignedHistoryLease};
use std::time::Duration;

#[derive(Clone)]
pub(crate) enum HistoryCollectionLease {
    Environment(HistoryLease),
    Unassigned(UnassignedHistoryLease),
}

impl HistoryCollectionLease {
    pub(crate) fn environment(&self) -> Option<&EnvironmentId> {
        match self {
            Self::Environment(lease) => Some(lease.environment_id()),
            Self::Unassigned(_) => None,
        }
    }
}

impl HistoryStore {
    pub(crate) async fn collection_lease(
        &self,
        environment: Option<EnvironmentId>,
        ttl: Duration,
    ) -> Result<Option<HistoryCollectionLease>, ConfigStoreFailure> {
        let store = self.owner.native_storage().await?;
        let mut current = self.collection.lock().await;
        if current
            .as_ref()
            .is_some_and(|lease| lease.environment() != environment.as_ref())
            && let Some(previous) = current.take()
        {
            match previous {
                HistoryCollectionLease::Environment(lease) => {
                    store.release_history_lease(lease).await?;
                }
                HistoryCollectionLease::Unassigned(lease) => {
                    store.release_unassigned_history_lease(lease).await?;
                }
            }
        }
        let renewed = match current.as_ref() {
            Some(HistoryCollectionLease::Environment(lease)) => store
                .renew_history_lease(lease.clone(), ttl)
                .await?
                .map(HistoryCollectionLease::Environment),
            Some(HistoryCollectionLease::Unassigned(lease)) => store
                .renew_unassigned_history_lease(lease.clone(), ttl)
                .await?
                .map(HistoryCollectionLease::Unassigned),
            None => None,
        };
        let grant = match renewed {
            Some(grant) => Some(grant),
            None => match environment {
                Some(environment) => store
                    .acquire_history_lease(environment, self.holder_id.clone(), ttl)
                    .await?
                    .map(HistoryCollectionLease::Environment),
                None => store
                    .acquire_unassigned_history_lease(self.holder_id.clone(), ttl)
                    .await?
                    .map(HistoryCollectionLease::Unassigned),
            },
        };
        *current = grant.clone();
        Ok(grant)
    }

    pub(crate) async fn release_collection(&self) -> Result<(), ConfigStoreFailure> {
        let lease = self.collection.lock().await.take();
        let Some(lease) = lease else {
            return Ok(());
        };
        let store = self.owner.native_storage().await?;
        match lease {
            HistoryCollectionLease::Environment(lease) => {
                store.release_history_lease(lease).await?;
            }
            HistoryCollectionLease::Unassigned(lease) => {
                store.release_unassigned_history_lease(lease).await?;
            }
        }
        Ok(())
    }
}
