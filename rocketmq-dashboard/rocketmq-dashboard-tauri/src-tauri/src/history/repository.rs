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

use crate::error::{DashboardError, DashboardResult};
use rusqlite::{Connection, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum HistoryMetric {
    BrokerCount,
    TopicCount,
    TopicTotalMessages,
}
impl HistoryMetric {
    fn as_str(self) -> &'static str {
        match self {
            Self::BrokerCount => "broker-count",
            Self::TopicCount => "topic-count",
            Self::TopicTotalMessages => "topic-total-messages",
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Sample {
    pub(crate) metric: HistoryMetric,
    pub(crate) dimension: String,
    pub(crate) timestamp_ms: i64,
    pub(crate) value: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HistoryQuery {
    pub(crate) begin_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) topic_name: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) before_ms: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HistoryPage {
    pub(crate) samples: Vec<Sample>,
    pub(crate) next_before_ms: Option<i64>,
}

pub(super) fn insert(
    connection: &mut Connection,
    environment: &str,
    revision: i64,
    samples: &[Sample],
) -> DashboardResult<bool> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current: i64 = transaction.query_row("SELECT revision FROM connection_metadata WHERE id=1", [], |row| {
        row.get(0)
    })?;
    if current != revision {
        return Ok(false);
    }
    for sample in samples {
        if !sample.value.is_finite() || sample.value < 0.0 || sample.timestamp_ms < 0 {
            return Err(DashboardError::Validation("Invalid history sample.".into()));
        }
        transaction.execute("INSERT INTO history_samples(environment_id,metric,dimension,timestamp_ms,value) VALUES (?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING",
            params![environment, sample.metric.as_str(), sample.dimension, sample.timestamp_ms, sample.value])?;
    }
    transaction.commit()?;
    Ok(true)
}

pub(super) fn query(
    connection: &Connection,
    environment: &str,
    metric: HistoryMetric,
    request: HistoryQuery,
) -> DashboardResult<HistoryPage> {
    let limit = request.limit.unwrap_or(1000);
    if request.begin_ms < 0
        || request.end_ms <= request.begin_ms
        || request.end_ms - request.begin_ms > 366 * 86_400_000
        || !(1..=2000).contains(&limit)
    {
        return Err(DashboardError::Validation(
            "Select a valid UTC range of at most 366 days and a limit from 1 to 2000.".into(),
        ));
    }
    let dimension = request.topic_name.unwrap_or_default();
    let mut statement = connection.prepare("SELECT timestamp_ms,value FROM history_samples WHERE environment_id=?1 AND metric=?2 AND dimension=?3 AND timestamp_ms>=?4 AND timestamp_ms<?5 AND timestamp_ms<?6 ORDER BY timestamp_ms DESC LIMIT ?7")?;
    let samples = statement
        .query_map(
            params![
                environment,
                metric.as_str(),
                dimension,
                request.begin_ms,
                request.end_ms,
                request.before_ms.unwrap_or(request.end_ms),
                limit + 1
            ],
            |row| {
                Ok(Sample {
                    metric,
                    dimension: dimension.clone(),
                    timestamp_ms: row.get(0)?,
                    value: row.get(1)?,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let mut page = HistoryPage {
        samples,
        next_before_ms: None,
    };
    if page.samples.len() > limit {
        page.samples.truncate(limit);
        page.next_before_ms = page.samples.last().map(|sample| sample.timestamp_ms);
    }
    Ok(page)
}

pub(super) fn cleanup(connection: &Connection, cutoff: i64) -> DashboardResult<usize> {
    Ok(connection.execute("DELETE FROM history_samples WHERE rowid IN (SELECT rowid FROM history_samples WHERE timestamp_ms < ?1 ORDER BY timestamp_ms LIMIT 1000)", [cutoff])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn history_is_persistent_scoped_unique_paginated_and_retained() {
        let path = std::env::temp_dir().join(format!("tauri-history-{}.db", uuid::Uuid::new_v4()));
        let mut connection = Connection::open(&path).unwrap();
        crate::persistence::schema::initialize(&mut connection).unwrap();
        let samples = [100, 200, 300].map(|timestamp_ms| Sample {
            metric: HistoryMetric::TopicTotalMessages,
            dimension: "orders".into(),
            timestamp_ms,
            value: 7.0,
        });
        assert!(insert(&mut connection, "env-a", 0, &samples).unwrap());
        assert!(insert(&mut connection, "env-a", 0, &samples).unwrap());
        assert!(insert(&mut connection, "env-b", 0, &samples).unwrap());
        connection
            .execute("UPDATE connection_metadata SET revision=1", [])
            .unwrap();
        assert!(!insert(&mut connection, "stale-env", 0, &samples).unwrap());
        drop(connection);
        let connection = Connection::open(&path).unwrap();
        let request = |before_ms| HistoryQuery {
            begin_ms: 0,
            end_ms: 400,
            topic_name: Some("orders".into()),
            limit: Some(2),
            before_ms,
        };
        let first = query(&connection, "env-a", HistoryMetric::TopicTotalMessages, request(None)).unwrap();
        assert_eq!(
            first
                .samples
                .iter()
                .map(|sample| sample.timestamp_ms)
                .collect::<Vec<_>>(),
            [300, 200]
        );
        assert_eq!(first.next_before_ms, Some(200));
        assert_eq!(
            query(
                &connection,
                "env-a",
                HistoryMetric::TopicTotalMessages,
                request(first.next_before_ms)
            )
            .unwrap()
            .samples
            .len(),
            1
        );
        assert!(
            query(
                &connection,
                "stale-env",
                HistoryMetric::TopicTotalMessages,
                request(None)
            )
            .unwrap()
            .samples
            .is_empty()
        );
        assert_eq!(cleanup(&connection, 200).unwrap(), 2);
        assert_eq!(
            query(&connection, "env-a", HistoryMetric::TopicTotalMessages, request(None))
                .unwrap()
                .samples
                .len(),
            2
        );
        assert!(
            query(&connection, "env-a", HistoryMetric::BrokerCount, request(None))
                .unwrap()
                .samples
                .is_empty()
        );
        drop(connection);
        std::fs::remove_file(path).unwrap();
    }
}
