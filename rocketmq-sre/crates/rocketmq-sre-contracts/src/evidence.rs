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

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use sha2::Digest;
use sha2::Sha256;

use crate::ClusterId;
use crate::ContractError;
use crate::CorrelationId;
use crate::EvidenceId;
use crate::QueryId;
use crate::SchemaVersion;
use crate::TenantId;

/// Availability of a required signal or evidence source.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageStatus {
    Available,
    Partial,
    Missing,
    NotProductionVerified,
}

/// Data handling classification applied before evidence leaves its source.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sensitivity {
    Public,
    Internal,
    Confidential,
    Restricted,
}

/// Inclusive time range requested from an evidence source.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl TimeRange {
    /// Creates and validates an inclusive time range.
    ///
    /// # Errors
    ///
    /// Returns [`ContractError::InvalidTimeRange`] when `start` is later than
    /// `end`.
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Self, ContractError> {
        if start > end {
            return Err(ContractError::InvalidTimeRange);
        }
        Ok(Self { start, end })
    }
}

/// Canonical read-only request for evidence.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct EvidenceQuery {
    pub query_id: QueryId,
    pub correlation_id: CorrelationId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub source: String,
    pub resource: String,
    pub time_range: TimeRange,
}

/// Content stored outside the inline evidence envelope.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct EvidenceReference {
    pub uri: String,
    pub digest: String,
    pub media_type: String,
    pub size_bytes: u64,
}

/// Bounded evidence payload or a content-addressed reference.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(tag = "storage", content = "value", rename_all = "snake_case")]
pub enum EvidenceContent {
    Inline(Value),
    Reference(EvidenceReference),
}

/// Immutable evidence captured from a queryable source.
#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct EvidenceSnapshot {
    pub schema: SchemaVersion,
    pub evidence_id: EvidenceId,
    pub query_id: QueryId,
    pub correlation_id: CorrelationId,
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub source: String,
    pub resource: String,
    pub time_range: TimeRange,
    pub observed_at: DateTime<Utc>,
    pub freshness_seconds: u64,
    pub partial: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub sensitivity: Sensitivity,
    pub coverage: CoverageStatus,
    pub content: EvidenceContent,
    pub content_hash: String,
}

#[derive(Serialize)]
struct EvidenceHashMaterial<'a> {
    schema: &'a SchemaVersion,
    source: &'a str,
    cluster_id: ClusterId,
    resource: &'a str,
    time_range: &'a TimeRange,
    content: &'a EvidenceContent,
}

impl EvidenceSnapshot {
    /// Captures a complete snapshot and seals its content hash.
    ///
    /// The hash deliberately excludes identifiers, tenant, collection time,
    /// freshness, coverage, warnings, partial state, sensitivity, and the hash
    /// itself. Those fields may change as evidence moves through the pipeline
    /// without changing the identity of the source content.
    ///
    /// # Errors
    ///
    /// Returns [`ContractError::InvalidDescriptor`] if the RFC 8785
    /// canonicalization step fails.
    pub fn capture(
        query: EvidenceQuery,
        schema: SchemaVersion,
        observed_at: DateTime<Utc>,
        content: EvidenceContent,
    ) -> Result<Self, ContractError> {
        let mut snapshot = Self {
            schema,
            evidence_id: EvidenceId::new(),
            query_id: query.query_id,
            correlation_id: query.correlation_id,
            tenant_id: query.tenant_id,
            cluster_id: query.cluster_id,
            source: query.source,
            resource: query.resource,
            time_range: query.time_range,
            observed_at,
            freshness_seconds: 0,
            partial: false,
            warnings: Vec::new(),
            sensitivity: Sensitivity::Internal,
            coverage: CoverageStatus::Available,
            content,
            content_hash: String::new(),
        };
        snapshot.content_hash = snapshot.compute_content_hash()?;
        Ok(snapshot)
    }

    /// Computes `sha256:<lowercase-hex>` over RFC 8785 canonical JSON.
    ///
    /// # Errors
    ///
    /// Returns [`ContractError::InvalidDescriptor`] when the selected content
    /// cannot be represented by RFC 8785, for example a non-finite number.
    pub fn compute_content_hash(&self) -> Result<String, ContractError> {
        let material = EvidenceHashMaterial {
            schema: &self.schema,
            source: &self.source,
            cluster_id: self.cluster_id,
            resource: &self.resource,
            time_range: &self.time_range,
            content: &self.content,
        };
        let canonical = serde_jcs::to_vec(&material).map_err(|error| ContractError::InvalidDescriptor {
            reason: format!("evidence cannot be canonicalized: {error}"),
        })?;
        let digest = Sha256::digest(canonical);
        Ok(format!("sha256:{digest:x}"))
    }

    /// Verifies the serialized snapshot against its canonical content hash.
    ///
    /// # Errors
    ///
    /// Returns [`ContractError::InvalidContentHash`] on a missing or mismatched
    /// hash, or the canonicalization error from [`Self::compute_content_hash`].
    pub fn verify_content_hash(&self) -> Result<(), ContractError> {
        if self.content_hash.is_empty() || self.content_hash != self.compute_content_hash()? {
            return Err(ContractError::InvalidContentHash);
        }
        Ok(())
    }
}

/// Whether a piece of evidence supports or contradicts a hypothesis.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceRelation {
    Supports,
    Contradicts,
}

/// Evidence linked to a hypothesis with an explicit rationale.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticEvidence {
    pub evidence_id: EvidenceId,
    pub relation: EvidenceRelation,
    pub rationale: String,
    pub confidence_percent: u8,
}

/// Evaluation state for a diagnostic hypothesis.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HypothesisStatus {
    Proposed,
    Supported,
    Rejected,
    Inconclusive,
}

/// Explainable diagnostic hypothesis with supporting and counter-evidence.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Hypothesis {
    pub id: String,
    pub statement: String,
    pub status: HypothesisStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<DiagnosticEvidence>,
    /// Evidence that must still be collected before this hypothesis can be
    /// promoted from an inconclusive state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_evidence: Vec<String>,
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;
    use crate::current_evidence_schema;

    fn snapshot() -> EvidenceSnapshot {
        let start = Utc
            .with_ymd_and_hms(2026, 7, 26, 1, 0, 0)
            .single()
            .expect("timestamp should be valid");
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: TenantId::new(),
            cluster_id: ClusterId::new(),
            source: "rocketmq-mcp".to_owned(),
            resource: "consumer-lag/group-a".to_owned(),
            time_range: TimeRange::new(start, start).expect("time range should be valid"),
        };
        EvidenceSnapshot::capture(
            query,
            current_evidence_schema(),
            start,
            EvidenceContent::Inline(json!({"lag": 42, "group": "group-a"})),
        )
        .expect("snapshot should canonicalize")
    }

    #[test]
    fn hash_is_stable_across_metadata_changes_and_json_key_order() {
        let original = snapshot();
        let mut changed = original.clone();
        changed.evidence_id = EvidenceId::new();
        changed.query_id = QueryId::new();
        changed.correlation_id = CorrelationId::new();
        changed.tenant_id = TenantId::new();
        changed.freshness_seconds = 30;
        changed.partial = true;
        changed.warnings.push("bounded".to_owned());
        changed.content = EvidenceContent::Inline(json!({"group": "group-a", "lag": 42}));

        assert_eq!(
            changed.compute_content_hash().expect("hash should compute"),
            original.content_hash
        );
    }

    #[test]
    fn hash_changes_when_source_content_changes() {
        let original = snapshot();
        let mut changed = original.clone();
        changed.content = EvidenceContent::Inline(json!({"lag": 43, "group": "group-a"}));

        assert_ne!(
            changed.compute_content_hash().expect("hash should compute"),
            original.content_hash
        );
        assert_eq!(changed.verify_content_hash(), Err(ContractError::InvalidContentHash));
    }
}
