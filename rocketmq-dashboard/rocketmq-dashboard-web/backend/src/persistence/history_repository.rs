// Copyright 2023 The RocketMQ Rust Authors
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
use crate::model::EnvironmentId;
use crate::model::MetricDimension;
use crate::model::MetricSample;
use crate::persistence::DashboardPersistence;
use crate::persistence::TimeRange;
use crate::persistence::backend::PersistenceBackend;
use crate::persistence::error::PersistenceError;
use crate::persistence::lease_repository::HistoryLease;
use chrono::TimeZone;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

pub const MAX_HISTORY_PAGE_SIZE: u32 = 5_000;
pub const MAX_HISTORY_APPEND_BATCH: usize = 500;

/// Returns whether a value is a non-negative UTC epoch millisecond accepted
/// by every history backend, including the File backend's calendar layout.
pub(crate) fn is_valid_history_timestamp_ms(value: i64) -> bool {
    value >= 0 && Utc.timestamp_millis_opt(value).single().is_some()
}

/// A bounded keyset query over one environment and one metric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryQuery {
    pub environment_id: EnvironmentId,
    pub metric: String,
    pub range: TimeRange,
    /// An empty list selects only samples without dimensions. This keeps the
    /// current dashboard cards exact while still allowing a caller to use a
    /// metric with several dimension sets through an explicit repository API.
    pub dimensions: Vec<MetricDimension>,
    pub limit: u32,
    pub cursor: Option<String>,
}

impl HistoryQuery {
    pub fn validate_and_normalize(&mut self) -> Result<(), PersistenceError> {
        if self.environment_id.0.is_empty() || self.environment_id.0.len() > 36 {
            return Err(PersistenceError::InvalidConfig(
                "history query environment is invalid".to_string(),
            ));
        }
        if self.metric.is_empty() || self.metric.len() > MetricSample::MAX_METRIC_LENGTH {
            return Err(PersistenceError::InvalidConfig(
                "history query metric is invalid".to_string(),
            ));
        }
        if !self.range.is_valid()
            || !is_valid_history_timestamp_ms(self.range.start_ms)
            || !is_valid_history_timestamp_ms(self.range.end_ms)
        {
            return Err(PersistenceError::InvalidConfig(
                "history query range is invalid".to_string(),
            ));
        }
        if self.limit == 0 || self.limit > MAX_HISTORY_PAGE_SIZE {
            return Err(PersistenceError::InvalidConfig(
                "history query limit is invalid".to_string(),
            ));
        }
        normalize_dimensions(&mut self.dimensions).map_err(PersistenceError::InvalidConfig)?;
        let dimensions_json = self.dimensions_json()?;
        if dimensions_json.len() > MetricSample::MAX_DIMENSIONS_JSON_LENGTH {
            return Err(PersistenceError::InvalidConfig(
                "history query dimensions are too large".to_string(),
            ));
        }
        if let Some(cursor) = &self.cursor {
            let decoded = HistoryCursor::decode(cursor)?;
            if decoded.environment_id != self.environment_id.0
                || decoded.metric != self.metric
                || decoded.start_ms != self.range.start_ms
                || decoded.end_ms != self.range.end_ms
                || decoded.dimensions_json != dimensions_json
                || decoded.last_bucket_ms < self.range.start_ms
                || decoded.last_bucket_ms > self.range.end_ms
                || decoded.last_dimensions_json != dimensions_json
            {
                return Err(PersistenceError::InvalidConfig(
                    "history cursor does not match the query filters".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn dimensions_json(&self) -> Result<String, PersistenceError> {
        serde_json::to_string(&self.dimensions).map_err(PersistenceError::Serialization)
    }

    pub fn cursor_key(&self) -> Result<Option<(i64, String)>, PersistenceError> {
        self.cursor
            .as_deref()
            .map(HistoryCursor::decode)
            .transpose()
            .map(|cursor| cursor.map(|cursor| (cursor.last_bucket_ms, cursor.last_dimensions_json)))
    }
}

/// One stable keyset page from history storage.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryPage {
    pub samples: Vec<MetricSample>,
    pub next_cursor: Option<String>,
}

/// The bounded result of one retention pass. Callers repeat while `has_more`
/// is true; this is intentionally not an unbounded deletion operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryRetentionResult {
    pub deleted: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct HistoryCursor {
    environment_id: String,
    metric: String,
    start_ms: i64,
    end_ms: i64,
    dimensions_json: String,
    last_bucket_ms: i64,
    last_dimensions_json: String,
}

impl HistoryCursor {
    fn encode(self) -> Result<String, PersistenceError> {
        let json = serde_json::to_vec(&self).map_err(PersistenceError::Serialization)?;
        Ok(json.iter().map(|byte| format!("{byte:02x}")).collect())
    }

    fn decode(value: &str) -> Result<Self, PersistenceError> {
        if !value.len().is_multiple_of(2) || value.len() > 8_192 {
            return Err(PersistenceError::InvalidConfig("history cursor is invalid".to_string()));
        }
        let bytes = value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = hex_nibble(pair[0])?;
                let low = hex_nibble(pair[1])?;
                Ok((high << 4) | low)
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;
        serde_json::from_slice(&bytes)
            .map_err(|_| PersistenceError::InvalidConfig("history cursor is invalid".to_string()))
    }
}

fn hex_nibble(value: u8) -> Result<u8, PersistenceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(PersistenceError::InvalidConfig("history cursor is invalid".to_string())),
    }
}

pub(crate) fn normalize_dimensions(dimensions: &mut [MetricDimension]) -> Result<(), String> {
    if dimensions.len() > MetricSample::MAX_DIMENSIONS {
        return Err("history query has too many dimensions".to_string());
    }
    dimensions.sort_by(|left, right| left.key.cmp(&right.key));
    for dimension in dimensions.iter() {
        if dimension.key.is_empty()
            || dimension.key.len() > MetricSample::MAX_DIMENSION_KEY_LENGTH
            || dimension.value.len() > MetricSample::MAX_DIMENSION_VALUE_LENGTH
        {
            return Err("history query dimension is invalid".to_string());
        }
    }
    if dimensions.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err("history query dimensions contain a duplicate key".to_string());
    }
    Ok(())
}

pub(crate) fn normalize_samples(mut samples: Vec<MetricSample>) -> Result<Vec<MetricSample>, PersistenceError> {
    let mut unique = BTreeMap::<(String, String, i64, String), MetricSample>::new();
    for mut sample in samples.drain(..) {
        sample.normalize().map_err(PersistenceError::InvalidConfig)?;
        // JSON does not preserve a useful distinction between positive and
        // negative zero for dashboard gauges; store the canonical value.
        if sample.value == 0.0 {
            sample.value = 0.0;
        }
        let dimensions_json = sample.dimensions_json().map_err(PersistenceError::InvalidConfig)?;
        let key = (
            sample.environment_id.0.clone(),
            sample.metric.clone(),
            sample.bucket_ms,
            dimensions_json,
        );
        if let Some(existing) = unique.get(&key) {
            if existing.value.to_bits() != sample.value.to_bits() {
                return Err(PersistenceError::Conflict);
            }
            continue;
        }
        unique.insert(key, sample);
    }
    Ok(unique.into_values().collect())
}

pub(crate) fn page_samples(
    query: &HistoryQuery,
    mut samples: Vec<MetricSample>,
) -> Result<HistoryPage, PersistenceError> {
    samples.sort_by(|left, right| {
        left.bucket_ms
            .cmp(&right.bucket_ms)
            .then_with(|| left.dimensions.cmp(&right.dimensions))
    });
    let dimensions_json = query.dimensions_json()?;
    let cursor_key = query.cursor_key()?;
    let mut matching = samples
        .into_iter()
        .filter(|sample| {
            sample.environment_id == query.environment_id
                && sample.metric == query.metric
                && sample.bucket_ms >= query.range.start_ms
                && sample.bucket_ms <= query.range.end_ms
                && sample.dimensions_json().ok().as_deref() == Some(dimensions_json.as_str())
        })
        .filter(|sample| {
            let Some((bucket_ms, dimensions)) = &cursor_key else {
                return true;
            };
            sample
                .dimensions_json()
                .map(|sample_dimensions| (sample.bucket_ms, sample_dimensions) > (*bucket_ms, dimensions.clone()))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    let has_more = matching.len() > query.limit as usize;
    matching.truncate(query.limit as usize);
    let next_cursor = if has_more {
        let last = matching.last().ok_or(PersistenceError::CorruptedData)?;
        HistoryCursor {
            environment_id: query.environment_id.0.clone(),
            metric: query.metric.clone(),
            start_ms: query.range.start_ms,
            end_ms: query.range.end_ms,
            dimensions_json,
            last_bucket_ms: last.bucket_ms,
            last_dimensions_json: last.dimensions_json().map_err(PersistenceError::InvalidConfig)?,
        }
        .encode()
        .map(Some)?
    } else {
        None
    };
    Ok(HistoryPage {
        samples: matching,
        next_cursor,
    })
}

impl DashboardPersistence {
    pub async fn append_history(
        &self,
        samples: Vec<MetricSample>,
        lease: Option<&HistoryLease>,
    ) -> Result<(), PersistenceError> {
        let samples = normalize_samples(samples)?;
        if samples.is_empty() {
            return Ok(());
        }
        if samples.len() > MAX_HISTORY_APPEND_BATCH {
            return Err(PersistenceError::InvalidConfig(
                "history append batch exceeds the bounded storage limit".to_string(),
            ));
        }
        let environment_id = &samples[0].environment_id;
        if samples.iter().any(|sample| sample.environment_id != *environment_id) {
            return Err(PersistenceError::InvalidConfig(
                "history append batch must contain exactly one environment".to_string(),
            ));
        }
        if self.history_uses_sql_lease() && lease.map(HistoryLease::environment_id) != Some(environment_id) {
            return Err(PersistenceError::Conflict);
        }
        match &self.backend {
            PersistenceBackend::File(store) => store.append_history(samples).await,
            PersistenceBackend::Sql(store) => store.append_history(samples, lease).await,
        }
    }

    pub async fn query_history(&self, mut query: HistoryQuery) -> Result<HistoryPage, PersistenceError> {
        query.validate_and_normalize()?;
        match &self.backend {
            PersistenceBackend::File(store) => page_samples(&query, store.read_history(&query).await?),
            PersistenceBackend::Sql(store) => store.query_history(query).await,
        }
    }

    pub async fn delete_history_before(
        &self,
        environment_id: &EnvironmentId,
        cutoff_ms: i64,
        batch_size: u32,
        lease: Option<&HistoryLease>,
    ) -> Result<HistoryRetentionResult, PersistenceError> {
        if !is_valid_history_timestamp_ms(cutoff_ms) || batch_size == 0 || batch_size > MAX_HISTORY_PAGE_SIZE {
            return Err(PersistenceError::InvalidConfig(
                "history retention request is invalid".to_string(),
            ));
        }
        match &self.backend {
            PersistenceBackend::File(store) => store.delete_history_before(environment_id, cutoff_ms, batch_size).await,
            PersistenceBackend::Sql(store) => {
                if lease.map(HistoryLease::environment_id) != Some(environment_id) {
                    return Err(PersistenceError::Conflict);
                }
                store
                    .delete_history_before(environment_id, cutoff_ms, batch_size, lease)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HistoryCursor;
    use super::HistoryQuery;
    use super::is_valid_history_timestamp_ms;
    use super::normalize_samples;
    use super::page_samples;
    use crate::model::EnvironmentId;
    use crate::model::MetricDimension;
    use crate::model::MetricSample;
    use crate::persistence::TimeRange;
    use crate::persistence::error::PersistenceError;

    fn sample(bucket_ms: i64, value: f64) -> MetricSample {
        MetricSample {
            environment_id: EnvironmentId("environment-000000000000000000000000".to_string()),
            metric: "broker-count".to_string(),
            bucket_ms,
            dimensions: Vec::new(),
            value,
        }
    }

    fn query(cursor: Option<String>) -> HistoryQuery {
        HistoryQuery {
            environment_id: EnvironmentId("environment-000000000000000000000000".to_string()),
            metric: "broker-count".to_string(),
            range: TimeRange {
                start_ms: 0,
                end_ms: 10,
            },
            dimensions: Vec::new(),
            limit: 2,
            cursor,
        }
    }

    #[test]
    fn conflicting_batch_is_rejected_before_any_backend_write() {
        assert!(matches!(
            normalize_samples(vec![sample(1, 1.0), sample(1, 2.0)]),
            Err(PersistenceError::Conflict)
        ));
    }

    #[test]
    fn cursor_is_bound_to_the_full_filter_and_never_repeats_a_key() {
        let mut first_query = query(None);
        first_query.validate_and_normalize().expect("query");
        let samples = vec![sample(1, 1.0), sample(2, 2.0), sample(3, 3.0)];
        let first = page_samples(&first_query, samples.clone()).expect("first page");
        assert_eq!(first.samples.len(), 2);
        let second_cursor = first.next_cursor.clone().expect("next cursor");
        let mut second_query = query(Some(second_cursor));
        second_query.validate_and_normalize().expect("second query");
        let second = page_samples(&second_query, samples).expect("second page");
        assert_eq!(second.samples.len(), 1);
        assert_eq!(second.samples[0].bucket_ms, 3);

        let mut mismatched = query(first.next_cursor);
        mismatched.dimensions = vec![MetricDimension {
            key: "topic".to_string(),
            value: "orders".to_string(),
        }];
        assert!(mismatched.validate_and_normalize().is_err());
    }

    #[test]
    fn cursor_rejects_out_of_range_positions_and_tampered_dimensions() {
        let invalid_position = HistoryCursor {
            environment_id: "environment-000000000000000000000000".to_string(),
            metric: "broker-count".to_string(),
            start_ms: 0,
            end_ms: 10,
            dimensions_json: "[]".to_string(),
            last_bucket_ms: 11,
            last_dimensions_json: "[]".to_string(),
        }
        .encode()
        .expect("cursor encoding");
        assert!(query(Some(invalid_position)).validate_and_normalize().is_err());

        let tampered_dimensions = HistoryCursor {
            environment_id: "environment-000000000000000000000000".to_string(),
            metric: "broker-count".to_string(),
            start_ms: 0,
            end_ms: 10,
            dimensions_json: "[]".to_string(),
            last_bucket_ms: 5,
            last_dimensions_json: "[{\"key\":\"topic\",\"value\":\"orders\"}]".to_string(),
        }
        .encode()
        .expect("cursor encoding");
        assert!(query(Some(tampered_dimensions)).validate_and_normalize().is_err());
    }

    #[test]
    fn utc_history_timestamp_boundaries_reject_i64_max() {
        assert!(is_valid_history_timestamp_ms(0));
        assert!(!is_valid_history_timestamp_ms(i64::MAX));
        let mut query = query(None);
        query.range.end_ms = i64::MAX;
        assert!(query.validate_and_normalize().is_err());
    }
}
