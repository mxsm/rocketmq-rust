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

//! History compatibility, retention, and fenced collection ownership.

use crate::{StorageError, StorageErrorKind};
use rocketmq_dashboard_common::{
    retain_history_points_bounded, EnvironmentHistoryPoint, EnvironmentId, HistoryMetricKind, HistoryPoint,
    HistoryRetention,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io, path::PathBuf};

pub const HISTORY_SCHEMA_VERSION: u32 = 2;
const MAX_SERIES_IDENTITY_BYTES: usize = 1_024;

/// Whether at least one submitted observation remains after retention.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryAppendOutcome {
    Retained,
    NotRetained,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryWriteOutcome {
    Unchanged,
    Committed,
}

/// A retained observation and a successful physical write are separate facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryAppendReport {
    pub outcome: HistoryAppendOutcome,
    pub write: HistoryWriteOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryReadOrigin {
    Missing,
    Existing,
    Migrated,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistoryReadReport {
    pub document: HistoryDocument,
    pub origin: HistoryReadOrigin,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HistoryDocument {
    pub schema_version: u32,
    pub points: Vec<EnvironmentHistoryPoint>,
    pub legacy_notes: Vec<LegacyHistoryRecord>,
}

impl Default for HistoryDocument {
    fn default() -> Self {
        Self {
            schema_version: HISTORY_SCHEMA_VERSION,
            points: Vec::new(),
            legacy_notes: Vec::new(),
        }
    }
}

impl HistoryDocument {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.schema_version != HISTORY_SCHEMA_VERSION {
            return Err(StorageError::unsupported_schema(
                self.schema_version,
                HISTORY_SCHEMA_VERSION,
            ));
        }
        if self.points.iter().any(|observation| !valid_point(observation)) {
            return Err(StorageError::new(StorageErrorKind::Validation));
        }
        Ok(())
    }

    pub(crate) fn append(
        &mut self,
        sample: Vec<EnvironmentHistoryPoint>,
        retention: HistoryRetention,
    ) -> HistoryAppendOutcome {
        let sample: Vec<_> = sample.into_iter().filter(valid_point).collect();
        if sample.is_empty() {
            return HistoryAppendOutcome::NotRetained;
        }
        let retention = HistoryRetention {
            max_points_per_series: retention.max_points_per_series.min(crate::HISTORY_MAX_POINTS_CAP),
            max_series: retention.max_series.min(crate::HISTORY_MAX_SERIES_CAP),
            max_total_points: retention.max_total_points.min(crate::HISTORY_MAX_TOTAL_POINTS_CAP),
        };
        let submitted = sample
            .iter()
            .map(observation_identity)
            .collect::<std::collections::BTreeSet<_>>();
        self.points.extend(sample);
        self.points = retain_observations(std::mem::take(&mut self.points), retention);
        if self
            .points
            .iter()
            .any(|retained| submitted.contains(&observation_identity(retained)))
        {
            HistoryAppendOutcome::Retained
        } else {
            HistoryAppendOutcome::NotRetained
        }
    }
}

// Values and provider revisions distinguish a newly submitted observation from an older retained one.
fn observation_identity(
    observation: &EnvironmentHistoryPoint,
) -> (Option<EnvironmentId>, HistoryMetricKind, String, u64, u64, u64) {
    (
        observation.environment_id.clone(),
        observation.point.metric,
        observation.point.series_identity.clone(),
        observation.point.timestamp_epoch_ms,
        observation.point.source_revision,
        observation.point.value.to_bits(),
    )
}

fn valid_point(observation: &EnvironmentHistoryPoint) -> bool {
    observation.point.value.is_finite()
        && !observation.point.series_identity.trim().is_empty()
        && observation.point.series_identity.len() <= MAX_SERIES_IDENTITY_BYTES
}

/// A fenced collection grant. A stale holder cannot write after another holder takes over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryLease {
    pub(crate) environment_id: EnvironmentId,
    pub(crate) holder_id: String,
    pub(crate) fencing_token: i64,
    pub(crate) expires_at_epoch_ms: i64,
}

/// A fenced grant for legacy observations whose source supplied no environment identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnassignedHistoryLease(pub(crate) CollectionLease);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CollectionLease {
    pub(crate) environment_id: Option<EnvironmentId>,
    pub(crate) holder_id: String,
    pub(crate) fencing_token: i64,
    pub(crate) expires_at_epoch_ms: i64,
}

impl CollectionLease {
    #[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
    pub(crate) fn key(&self) -> &str {
        self.environment_id.as_ref().map_or("", EnvironmentId::as_str)
    }

    pub(crate) fn matches(&self, lease: &Self, now: i64) -> bool {
        self.environment_id == lease.environment_id
            && self.holder_id == lease.holder_id
            && self.fencing_token == lease.fencing_token
            && self.expires_at_epoch_ms > now
    }
}

impl HistoryLease {
    pub(crate) fn from_grant(environment_id: EnvironmentId, grant: CollectionLease) -> Self {
        Self {
            environment_id,
            holder_id: grant.holder_id,
            fencing_token: grant.fencing_token,
            expires_at_epoch_ms: grant.expires_at_epoch_ms,
        }
    }

    pub(crate) fn grant(&self) -> CollectionLease {
        CollectionLease {
            environment_id: Some(self.environment_id.clone()),
            holder_id: self.holder_id.clone(),
            fencing_token: self.fencing_token,
            expires_at_epoch_ms: self.expires_at_epoch_ms,
        }
    }
    pub fn environment_id(&self) -> &EnvironmentId {
        &self.environment_id
    }

    pub fn holder_id(&self) -> &str {
        &self.holder_id
    }

    pub fn fencing_token(&self) -> i64 {
        self.fencing_token
    }

    pub fn expires_at_epoch_ms(&self) -> i64 {
        self.expires_at_epoch_ms
    }
}

pub(crate) fn retain_observations(
    points: Vec<EnvironmentHistoryPoint>,
    retention: HistoryRetention,
) -> Vec<EnvironmentHistoryPoint> {
    let mut environments: BTreeMap<Option<EnvironmentId>, Vec<HistoryPoint>> = BTreeMap::new();
    for observation in points {
        environments
            .entry(observation.environment_id)
            .or_default()
            .push(observation.point);
    }
    let mut retained = Vec::new();
    for (environment_id, points) in environments {
        for point in retain_history_points_bounded(points, retention) {
            retained.push(EnvironmentHistoryPoint {
                environment_id: environment_id.clone(),
                point,
            });
        }
    }
    let mut series = BTreeMap::new();
    for observation in &retained {
        series
            .entry((
                observation.environment_id.clone(),
                observation.point.metric,
                observation.point.series_identity.clone(),
            ))
            .and_modify(|newest: &mut u64| *newest = (*newest).max(observation.point.timestamp_epoch_ms))
            .or_insert(observation.point.timestamp_epoch_ms);
    }
    let mut ages = series
        .into_iter()
        .map(|(key, newest)| (newest, key))
        .collect::<Vec<_>>();
    ages.sort();
    let evicted = ages
        .into_iter()
        .rev()
        .skip(retention.max_series)
        .map(|(_, key)| key)
        .collect::<std::collections::BTreeSet<_>>();
    retained.retain(|observation| {
        !evicted.contains(&(
            observation.environment_id.clone(),
            observation.point.metric,
            observation.point.series_identity.clone(),
        ))
    });
    retained.sort_by(|left, right| {
        left.point
            .timestamp_epoch_ms
            .cmp(&right.point.timestamp_epoch_ms)
            .then(left.environment_id.cmp(&right.environment_id))
            .then(left.point.series_identity.cmp(&right.point.series_identity))
    });
    let evict = retained.len().saturating_sub(retention.max_total_points);
    retained.drain(..evict);
    retained.sort_by(|left, right| {
        left.environment_id
            .cmp(&right.environment_id)
            .then(left.point.metric.cmp(&right.point.metric))
            .then(left.point.series_identity.cmp(&right.point.series_identity))
            .then(left.point.timestamp_epoch_ms.cmp(&right.point.timestamp_epoch_ms))
    });
    retained
}

pub(crate) fn load_document(path: &PathBuf) -> Result<(HistoryDocument, bool), StorageError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((HistoryDocument::default(), false)),
        Err(source) => return Err(StorageError::io("read history", path.clone(), source)),
    };
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|source| StorageError::invalid_document(path.clone(), source))?;
    if value.is_array() {
        let legacy_notes =
            serde_json::from_value(value).map_err(|source| StorageError::invalid_document(path.clone(), source))?;
        return Ok((
            HistoryDocument {
                legacy_notes,
                ..HistoryDocument::default()
            },
            true,
        ));
    }
    let version = value
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    match version {
        1 => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields, rename_all = "camelCase")]
            struct LegacyMetricDocument {
                schema_version: u32,
                points: Vec<HistoryPoint>,
            }
            let legacy: LegacyMetricDocument =
                serde_json::from_value(value).map_err(|source| StorageError::invalid_document(path.clone(), source))?;
            let _ = legacy.schema_version;
            Ok((
                HistoryDocument {
                    points: legacy
                        .points
                        .into_iter()
                        .map(|point| EnvironmentHistoryPoint {
                            environment_id: None,
                            point,
                        })
                        .collect(),
                    ..HistoryDocument::default()
                },
                true,
            ))
        }
        2 => serde_json::from_value(value)
            .map(|document| (document, false))
            .map_err(|source| StorageError::invalid_document(path.clone(), source)),
        version => Err(StorageError::unsupported_schema(
            u32::try_from(version).unwrap_or(u32::MAX),
            HISTORY_SCHEMA_VERSION,
        )),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LegacyHistoryRecord {
    pub id: String,
    pub note: String,
}
