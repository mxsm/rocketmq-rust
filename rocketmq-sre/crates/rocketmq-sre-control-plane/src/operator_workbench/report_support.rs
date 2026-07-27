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
use rocketmq_sre_contracts::IncidentId;
use rocketmq_sre_contracts::OperationsFinding;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::ControlPlaneError;
use crate::auth::AuthContext;

pub(super) const MAX_SECTION_ITEMS: usize = 64;
pub(super) const FETCH_LIMIT: i64 = (MAX_SECTION_ITEMS + 1) as i64;

pub(super) struct ReportSection {
    pub(super) items: Vec<OperationsFinding>,
    pub(super) truncated: bool,
}

pub(super) fn scoped_clusters(
    auth: &AuthContext,
    requested: Option<ClusterId>,
) -> Result<Vec<Uuid>, ControlPlaneError> {
    if let Some(cluster_id) = requested {
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "requested cluster is outside the authenticated scope",
            ));
        }
        return Ok(vec![cluster_id.as_uuid()]);
    }
    Ok(auth.clusters.iter().map(|cluster_id| cluster_id.as_uuid()).collect())
}

pub(super) fn bounded_rows<F>(
    rows: Vec<PgRow>,
    _section: &'static str,
    mut map: F,
) -> Result<ReportSection, ControlPlaneError>
where
    F: FnMut(&PgRow) -> Result<OperationsFinding, ControlPlaneError>,
{
    let truncated = rows.len() > MAX_SECTION_ITEMS;
    let items = rows
        .iter()
        .take(MAX_SECTION_ITEMS)
        .map(&mut map)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ReportSection { items, truncated })
}

pub(super) fn normalized_owner(owner: String) -> String {
    let trimmed = owner.trim();
    if trimmed.is_empty() {
        "unassigned".to_owned()
    } else {
        trimmed.chars().take(256).collect()
    }
}

pub(super) fn incident_link(incident_id: IncidentId) -> String {
    format!("/incidents/{incident_id}")
}

pub(super) fn display_optional_number(value: Option<f64>) -> String {
    value.map_or_else(|| "unknown".to_owned(), |number| format!("{number:.3}"))
}

pub(super) fn mean_error(findings: &[OperationsFinding]) -> Option<f64> {
    let values = findings
        .iter()
        .filter_map(|finding| finding.detail.rsplit_once("mae_sample="))
        .filter_map(|(_, value)| value.parse::<f64>().ok())
        .collect::<Vec<_>>();
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}
