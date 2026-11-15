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

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;

use crate::ActionReadinessReason;
use crate::CoverageStatus;
use crate::EvidenceSnapshot;

/// Checks recorded source age plus exact storage elapsed time without renewing proof.
///
/// The caller supplies the applicable limit, including any stricter Agent limit.
/// Equality at the evidence deadline is valid. This check grants no authority.
///
/// # Errors
///
/// Returns a closed reason for incomplete, invalid, future, overflowing or stale
/// evidence. Content integrity is checked independently of proof-origin authenticity.
pub fn action_evidence_valid_until(
    snapshot: &EvidenceSnapshot,
    as_of: DateTime<Utc>,
    maximum_age_seconds: u64,
) -> Result<DateTime<Utc>, ActionReadinessReason> {
    use ActionReadinessReason as Reason;

    if snapshot.partial || snapshot.coverage != CoverageStatus::Available {
        return Err(Reason::EvidenceMissing);
    }
    snapshot.verify_content_hash().map_err(|_| Reason::EvidenceInvalid)?;
    if snapshot.observed_at > as_of {
        return Err(Reason::EvidenceInvalid);
    }
    let elapsed = as_of
        .signed_duration_since(snapshot.observed_at)
        .num_nanoseconds()
        .and_then(|value| u128::try_from(value).ok())
        .ok_or(Reason::EvidenceInvalid)?;
    let source = u128::from(snapshot.freshness_seconds)
        .checked_mul(1_000_000_000)
        .ok_or(Reason::EvidenceInvalid)?;
    let allowed = u128::from(maximum_age_seconds)
        .checked_mul(1_000_000_000)
        .ok_or(Reason::EvidenceInvalid)?;
    if source.checked_add(elapsed).ok_or(Reason::EvidenceInvalid)? > allowed {
        return Err(Reason::EvidenceStale);
    }
    let remaining = maximum_age_seconds
        .checked_sub(snapshot.freshness_seconds)
        .and_then(|value| i64::try_from(value).ok())
        .and_then(TimeDelta::try_seconds)
        .ok_or(Reason::EvidenceInvalid)?;
    snapshot
        .observed_at
        .checked_add_signed(remaining)
        .ok_or(Reason::EvidenceInvalid)
}

#[cfg(test)]
mod tests {
    use crate::CorrelationId;
    use crate::EvidenceContent;
    use crate::EvidenceQuery;
    use crate::QueryId;
    use crate::TimeRange;
    use crate::current_evidence_schema;

    use super::*;

    fn evidence(age: u64) -> EvidenceSnapshot {
        let at: DateTime<Utc> = "2026-11-14T12:00:00.123456789Z".parse().unwrap();
        let mut value = EvidenceSnapshot::capture(
            EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: crate::TenantId::new(),
                cluster_id: crate::ClusterId::new(),
                source: "diagnostic-fixture".to_owned(),
                resource: "broker/example".to_owned(),
                time_range: TimeRange::new(at, at).unwrap(),
            },
            current_evidence_schema(),
            at,
            EvidenceContent::Inline(serde_json::json!({"fixture": true})),
        )
        .unwrap();
        value.coverage = CoverageStatus::Available;
        value.freshness_seconds = age;
        value
    }

    #[test]
    fn combined_age_accepts_the_boundary_and_rejects_one_more_nanosecond() {
        let value = evidence(899);
        let deadline = value.observed_at + TimeDelta::seconds(1);
        assert_eq!(action_evidence_valid_until(&value, deadline, 900), Ok(deadline));
        assert_eq!(
            action_evidence_valid_until(&value, deadline + TimeDelta::nanoseconds(1), 900),
            Err(ActionReadinessReason::EvidenceStale)
        );
        assert_eq!(
            action_evidence_valid_until(&value, value.observed_at + TimeDelta::seconds(2), 900),
            Err(ActionReadinessReason::EvidenceStale)
        );
        let agent = evidence(119);
        assert!(action_evidence_valid_until(&agent, agent.observed_at + TimeDelta::seconds(1), 120).is_ok());
        assert!(action_evidence_valid_until(&agent, agent.observed_at + TimeDelta::seconds(2), 120).is_err());
    }

    #[test]
    fn invalid_future_overflow_and_incomplete_metadata_fail_closed() {
        let mut value = evidence(0);
        assert_eq!(
            action_evidence_valid_until(&value, value.observed_at - TimeDelta::nanoseconds(1), 900),
            Err(ActionReadinessReason::EvidenceInvalid)
        );
        assert_eq!(
            action_evidence_valid_until(&value, value.observed_at, u64::MAX),
            Err(ActionReadinessReason::EvidenceInvalid)
        );
        value.partial = true;
        assert_eq!(
            action_evidence_valid_until(&value, value.observed_at, 900),
            Err(ActionReadinessReason::EvidenceMissing)
        );
        value.partial = false;
        value.content_hash.clear();
        assert_eq!(
            action_evidence_valid_until(&value, value.observed_at, 900),
            Err(ActionReadinessReason::EvidenceInvalid)
        );
    }
}
