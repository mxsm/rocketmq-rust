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
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::execution::supervise_session;
use super::MutationToolRequest;
use super::MutationToolResponse;
use super::MutationToolSessionFactory;
use super::IDEMPOTENCY_CAPACITY;
use super::IDEMPOTENCY_TTL;
use crate::error::ControlError;
use crate::model::ClusterName;
use crate::model::ControlOperation;
use crate::model::Principal;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct IdempotencyKey {
    pub(super) principal: String,
    pub(super) operation: ControlOperation,
    pub(super) cluster: ClusterName,
    pub(super) targets: Vec<String>,
    pub(super) request_key: String,
}

#[derive(Clone)]
pub(super) struct IdempotencyIdentity {
    pub(super) key: Option<IdempotencyKey>,
    pub(super) payload: String,
}

impl IdempotencyIdentity {
    pub(super) fn from_request(
        principal: &Principal,
        cluster: &ClusterName,
        request: &MutationToolRequest,
    ) -> Result<Self, ControlError> {
        let mut targets = request.target_names();
        targets.sort();
        let key = request.request_key().map(|request_key| IdempotencyKey {
            principal: principal.subject.clone(),
            operation: request.operation(),
            cluster: cluster.clone(),
            targets,
            request_key: request_key.to_owned(),
        });
        Ok(Self {
            key,
            payload: request.canonical_payload()?,
        })
    }
}

#[derive(Default)]
pub(super) struct IdempotencyState {
    pub(super) entries: BTreeMap<IdempotencyKey, IdempotencyEntry>,
    sequence: u64,
}

pub(super) enum IdempotencyEntry {
    InFlight {
        payload: String,
        followers: Vec<oneshot::Sender<Result<MutationToolResponse, ControlError>>>,
    },
    Completed {
        payload: String,
        result: Box<Result<MutationToolResponse, ControlError>>,
        expires_at: tokio::time::Instant,
        sequence: u64,
    },
}

pub(super) enum CacheAdmission {
    Leader,
    Follower(oneshot::Receiver<Result<MutationToolResponse, ControlError>>),
    Hit(Box<Result<MutationToolResponse, ControlError>>),
    Uncached,
}

#[derive(Debug)]
pub(super) enum AdmissionError {
    Collision,
    Capacity,
}

pub(super) async fn execute_admitted(
    cache: Arc<Mutex<IdempotencyState>>,
    identity: IdempotencyIdentity,
    admission: CacheAdmission,
    factory: Arc<dyn MutationToolSessionFactory>,
    cluster: ClusterName,
    request: MutationToolRequest,
    timeout: Duration,
    request_cancellation: CancellationToken,
    owner_cancellation: CancellationToken,
) -> Result<MutationToolResponse, ControlError> {
    let Some(key) = identity.key else {
        return supervise_session(
            factory,
            cluster,
            request,
            timeout,
            request_cancellation,
            owner_cancellation,
        )
        .await;
    };
    match admission {
        CacheAdmission::Follower(receiver) => {
            let deadline = tokio::time::Instant::now() + timeout;
            tokio::select! {
                biased;
                _ = request_cancellation.cancelled() => Err(ControlError::cancelled()),
                _ = owner_cancellation.cancelled() => Err(ControlError::cancelled()),
                _ = tokio::time::sleep_until(deadline) => Err(ControlError::timeout()),
                result = receiver => result.map_err(|_| ControlError::execution_failed())?,
            }
        }
        CacheAdmission::Hit(result) => *result,
        CacheAdmission::Uncached => {
            supervise_session(
                factory,
                cluster,
                request,
                timeout,
                request_cancellation,
                owner_cancellation,
            )
            .await
        }
        CacheAdmission::Leader => {
            let result = supervise_session(
                factory,
                cluster,
                request,
                timeout,
                request_cancellation,
                owner_cancellation,
            )
            .await;
            complete_cache(&cache, key, identity.payload, result.clone()).await;
            result
        }
    }
}

pub(super) async fn admit_cache(
    cache: &Mutex<IdempotencyState>,
    key: Option<&IdempotencyKey>,
    payload: &str,
) -> Result<CacheAdmission, AdmissionError> {
    let Some(key) = key else {
        return Ok(CacheAdmission::Uncached);
    };
    let mut state = cache.lock().await;
    let now = tokio::time::Instant::now();
    state
        .entries
        .retain(|_, entry| !matches!(entry, IdempotencyEntry::Completed { expires_at, .. } if *expires_at <= now));
    if let Some(entry) = state.entries.get_mut(key) {
        return match entry {
            IdempotencyEntry::InFlight {
                payload: recorded,
                followers,
            } if recorded == payload => {
                let (sender, receiver) = oneshot::channel();
                followers.push(sender);
                Ok(CacheAdmission::Follower(receiver))
            }
            IdempotencyEntry::Completed {
                payload: recorded,
                result,
                ..
            } if recorded == payload => Ok(CacheAdmission::Hit(result.clone())),
            _ => Err(AdmissionError::Collision),
        };
    }
    while state.entries.len() >= IDEMPOTENCY_CAPACITY {
        let oldest = state
            .entries
            .iter()
            .filter_map(|(key, entry)| match entry {
                IdempotencyEntry::Completed { sequence, .. } => Some((key.clone(), *sequence)),
                IdempotencyEntry::InFlight { .. } => None,
            })
            .min_by_key(|(_, sequence)| *sequence)
            .map(|(key, _)| key);
        let Some(oldest) = oldest else {
            return Err(AdmissionError::Capacity);
        };
        state.entries.remove(&oldest);
    }
    state.entries.insert(
        key.clone(),
        IdempotencyEntry::InFlight {
            payload: payload.to_owned(),
            followers: Vec::new(),
        },
    );
    Ok(CacheAdmission::Leader)
}

pub(super) async fn abort_cache_reservation(
    cache: &Mutex<IdempotencyState>,
    identity: &IdempotencyIdentity,
    error: ControlError,
) {
    let Some(key) = &identity.key else {
        return;
    };
    let mut state = cache.lock().await;
    let followers = match state.entries.remove(key) {
        Some(IdempotencyEntry::InFlight { followers, .. }) => followers,
        Some(entry) => {
            state.entries.insert(key.clone(), entry);
            Vec::new()
        }
        None => Vec::new(),
    };
    drop(state);
    for follower in followers {
        let _ = follower.send(Err(error.clone()));
    }
}

pub(super) async fn complete_cache(
    cache: &Mutex<IdempotencyState>,
    key: IdempotencyKey,
    payload: String,
    result: Result<MutationToolResponse, ControlError>,
) {
    let mut state = cache.lock().await;
    let followers = match state.entries.remove(&key) {
        Some(IdempotencyEntry::InFlight { followers, .. }) => followers,
        _ => Vec::new(),
    };
    state.sequence = state.sequence.saturating_add(1);
    let sequence = state.sequence;
    state.entries.insert(
        key,
        IdempotencyEntry::Completed {
            payload,
            result: Box::new(result.clone()),
            expires_at: tokio::time::Instant::now() + IDEMPOTENCY_TTL,
            sequence,
        },
    );
    drop(state);
    for follower in followers {
        let _ = follower.send(result.clone());
    }
}
