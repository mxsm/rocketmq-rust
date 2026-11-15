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

use super::{NAMESRV_ROUTE_SCHEMA, normalized};
use rocketmq_sre_contracts::{
    ContractJsonValue as Value, EvidenceContent, EvidenceExposure, EvidenceSnapshot, SreContractError, SreTimestamp,
    TimeRange, canonical_sha256,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamesrvComparisonState {
    Equal,
    Difference,
    Transient,
    Incomparable,
}
impl NamesrvComparisonState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "compared_equal",
            Self::Difference => "stable_observed_difference",
            Self::Transient => "transient_change_observed",
            Self::Incomparable => "incomparable",
        }
    }
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::Equal => "current_routes_compared_equal",
            Self::Difference => "current_route_difference_observed",
            Self::Transient => "current_route_change_observed",
            Self::Incomparable => "current_routes_incomparable",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamesrvObservationIssue {
    Schema,
    Scope,
    Window,
    Future,
    Stale,
    Domain,
}
impl NamesrvObservationIssue {
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::Schema => "invalid_namesrv_observation_contract",
            Self::Scope => "namesrv_observation_scope_mismatch",
            Self::Window => "namesrv_observation_time_invalid",
            Self::Future => "namesrv_observation_in_future",
            Self::Stale => "route_evidence_stale",
            Self::Domain => "fingerprint_domain_unavailable",
        }
    }
}
use NamesrvObservationIssue as Issue;

#[derive(Clone, Debug, PartialEq)]
pub struct NamesrvRouteObservation {
    pub state: NamesrvComparisonState,
    pub compared_dimensions: Vec<&'static str>,
    pub differing_dimensions: Vec<&'static str>,
    pub unknown_dimensions: Vec<&'static str>,
    pub timing_eligible: bool,
    pub change_observed: bool,
    pub acquisition_span_millis: u64,
    pub source_age_seconds: u64,
    semantic: Value,
}

fn json_object<const N: usize>(fields: [(&str, Value); N]) -> Value {
    Value::Object(fields.into_iter().map(|(key, value)| (key.to_owned(), value)).collect())
}
impl NamesrvRouteObservation {
    pub fn comparison_json(&self) -> Value {
        let reason = if self.timing_eligible {
            self.state.reason_code()
        } else {
            "acquisition_span_exceeded"
        };
        json_object([
            ("state", self.state.as_str().into()),
            ("reason_code", reason.into()),
            (
                "compared_dimensions",
                Value::Array(
                    self.compared_dimensions
                        .iter()
                        .map(|value| Value::from(*value))
                        .collect(),
                ),
            ),
            (
                "differing_dimensions",
                Value::Array(
                    self.differing_dimensions
                        .iter()
                        .map(|value| Value::from(*value))
                        .collect(),
                ),
            ),
            (
                "unknown_dimensions",
                Value::Array(
                    self.unknown_dimensions
                        .iter()
                        .map(|value| Value::from(*value))
                        .collect(),
                ),
            ),
            ("timing_eligible", self.timing_eligible.into()),
            ("change_observed", self.change_observed.into()),
            ("acquisition_span_millis", self.acquisition_span_millis.into()),
        ])
    }
}

fn instant(value: &Value) -> Result<SreTimestamp, Issue> {
    value.as_str().and_then(|value| value.parse().ok()).ok_or(Issue::Window)
}
fn interval(start: &Value, end: &Value) -> Result<TimeRange, Issue> {
    TimeRange::new(instant(start)?, instant(end)?).map_err(|_| Issue::Window)
}
fn nanos(value: SreTimestamp) -> i128 {
    value.timestamp() as i128 * 1_000_000_000 + value.timestamp_subsec_nanos() as i128
}
fn equivalent(inner: SreTimestamp, outer: SreTimestamp) -> bool {
    inner == outer
        || (outer.timestamp_subsec_nanos().is_multiple_of(1000) && inner.timestamp_micros() == outer.timestamp_micros())
}
fn contains(outer: &TimeRange, inner: &TimeRange) -> bool {
    inner.start >= outer.start && inner.end <= outer.end
}
fn flags(value: &Value) -> Result<[Option<bool>; 3], Issue> {
    let mut values = [None; 3];
    let map = value.as_object().filter(|map| map.len() == 3).ok_or(Issue::Schema)?;
    for (index, key) in ["cluster_test", "order_message_enable", "support_acting_master"]
        .iter()
        .enumerate()
    {
        let value = map.get(*key).ok_or(Issue::Schema)?;
        if !value.is_null() {
            values[index] = Some(value.as_bool().ok_or(Issue::Schema)?);
        }
    }
    Ok(values)
}
fn read_span(read: &Value, round: &TimeRange) -> Result<Option<TimeRange>, Issue> {
    if read["started_at"].is_null() && read["finished_at"].is_null() {
        return Ok(None);
    }
    let span = interval(&read["started_at"], &read["finished_at"])?;
    if !contains(round, &span) {
        return Err(Issue::Window);
    }
    Ok(Some(span))
}

fn basic(snapshot: &EvidenceSnapshot) -> Result<&Value, Issue> {
    snapshot.verify_content_hash().map_err(|_| Issue::Schema)?;
    let EvidenceContent::Inline(content) = &snapshot.content else {
        return Err(Issue::Schema);
    };
    let topic = snapshot
        .resource
        .strip_prefix("namesrv-route/")
        .filter(|topic| !topic.is_empty() && !topic.contains('/'))
        .ok_or(Issue::Scope)?;
    if snapshot.source != "admin-query"
        || snapshot.exposure != EvidenceExposure::AdminRpc
        || content["schema_version"].as_str() != Some(NAMESRV_ROUTE_SCHEMA)
        || content["profile"].as_str() != Some("nameserver_route_comparison")
        || content["tenant_id"].as_str() != Some(snapshot.tenant_id.to_string().as_str())
        || content["cluster_id"].as_str() != Some(snapshot.cluster_id.to_string().as_str())
        || content["topic"].as_str() != Some(topic)
    {
        return Err(Issue::Scope);
    }
    let requested = interval(
        &content["requested_window"]["start"],
        &content["requested_window"]["end"],
    )?;
    if !equivalent(requested.start, snapshot.time_range.start) || !equivalent(requested.end, snapshot.time_range.end) {
        return Err(Issue::Window);
    }
    if content["configuration"]["fingerprint_algorithm"].as_str() != Some("hmac-sha256-v1")
        || !normalized::digest(&content["configuration"]["fingerprint_domain"])
    {
        return Err(Issue::Domain);
    }
    Ok(content)
}

/// Immutable metadata only. Business age/view/route sufficiency is deliberately
/// not checked; callers authorize the original reference and retention first.
pub fn namesrv_route_domain(snapshot: &EvidenceSnapshot) -> Result<String, Issue> {
    basic(snapshot)?["configuration"]["fingerprint_domain"]
        .as_str()
        .map(str::to_owned)
        .ok_or(Issue::Domain)
}

/// Recomputes point comparison from full validated safe facts, ignoring supplied
/// comparison/complete flags. No clock/network or hidden query is involved.
pub fn compare_namesrv_route_content(content: &Value) -> Result<NamesrvRouteObservation, Issue> {
    if content["schema_version"].as_str() != Some(NAMESRV_ROUTE_SCHEMA)
        || content["profile"].as_str() != Some("nameserver_route_comparison")
        || content["temporal_semantics"].as_str() != Some("current_context_only")
        || content["historical_coverage"].as_str() != Some("unknown")
    {
        return Err(Issue::Schema);
    }
    let collection = interval(&content["collection_started_at"], &content["collected_at"])?;
    let requested = interval(
        &content["requested_window"]["start"],
        &content["requested_window"]["end"],
    )?;
    if requested.end > collection.start {
        return Err(Issue::Window);
    }
    let config = &content["configuration"];
    if config["independence_source"].as_str() != Some("operator_declared_direct_peers")
        || config["fingerprint_algorithm"].as_str() != Some("hmac-sha256-v1")
        || !normalized::digest(&config["fingerprint_domain"])
    {
        return Err(Issue::Domain);
    }
    let expected_view = flags(&config["expected_view"])?;
    if expected_view[0] != Some(false) || expected_view.iter().any(Option::is_none) {
        return Err(Issue::Schema);
    }
    let physical = config["physical_cluster"]
        .as_str()
        .filter(|text| !text.is_empty() && text.len() <= 255)
        .ok_or(Issue::Scope)?;
    let topic = content["topic"]
        .as_str()
        .filter(|text| !text.is_empty() && text.len() <= 255)
        .ok_or(Issue::Scope)?;
    let peers = config["peers"]
        .as_array()
        .filter(|peers| (2..=4).contains(&peers.len()))
        .ok_or(Issue::Schema)?;
    let mut expected = BTreeMap::new();
    let mut endpoints = BTreeSet::new();
    let mut declarations = BTreeSet::new();
    for peer in peers {
        if !normalized::opaque(&peer["peer_id"])
            || !normalized::opaque(&peer["instance_reference"])
            || !normalized::digest(&peer["peer_endpoint_id"])
        {
            return Err(Issue::Schema);
        }
        let id = peer["peer_id"].as_str().ok_or(Issue::Schema)?;
        let endpoint = peer["peer_endpoint_id"].as_str().ok_or(Issue::Schema)?;
        if expected.insert(id, endpoint).is_some()
            || !endpoints.insert(endpoint)
            || !declarations.insert(peer["instance_reference"].as_str())
        {
            return Err(Issue::Schema);
        }
    }
    let rounds = content["rounds"]
        .as_array()
        .filter(|rounds| rounds.len() == 2)
        .ok_or(Issue::Schema)?;
    let mut seen_rounds = BTreeSet::new();
    let mut by_peer = BTreeMap::<&str, Vec<(&Value, Option<&Value>, [Option<bool>; 3])>>::new();
    let mut semantic_rounds = BTreeMap::new();
    let mut started_config = 0u64;
    let mut started_route = 0u64;
    let mut received = 0u64;
    let mut unknown_bytes = 0u64;
    let mut earliest = collection.end;
    let mut timing_eligible = true;
    let mut complete = true;
    let mut unknown_dimensions = BTreeSet::new();
    let mut previous_end = None;
    for (round_index, round) in rounds.iter().enumerate() {
        let number = round["round"]
            .as_u64()
            .filter(|n| *n == 1 || *n == 2)
            .ok_or(Issue::Schema)?;
        if !seen_rounds.insert(number) || number != round_index as u64 + 1 {
            return Err(Issue::Schema);
        }
        let span = interval(&round["started_at"], &round["finished_at"])?;
        if !contains(&collection, &span) || previous_end.is_some_and(|end| span.start < end) {
            return Err(Issue::Window);
        }
        previous_end = Some(span.end);
        timing_eligible &= nanos(span.end) - nanos(span.start) <= 2_000_000_000;
        let rows = round["peers"]
            .as_array()
            .filter(|rows| rows.len() == peers.len())
            .ok_or(Issue::Schema)?;
        let mut seen = BTreeSet::new();
        let mut semantic = BTreeMap::new();
        let mut last_config = None;
        let mut first_route = None;
        for row in rows {
            let id = row["peer_id"].as_str().ok_or(Issue::Schema)?;
            let endpoint = *expected.get(id).ok_or(Issue::Scope)?;
            if row["peer_endpoint_id"].as_str() != Some(endpoint) || !seen.insert(id) {
                return Err(Issue::Scope);
            }
            let view = &row["configuration"];
            let route = &row["route"];
            let observed_view = flags(&view["flags"])?;
            let view_status = view["status"].as_str().ok_or(Issue::Schema)?;
            if !["available", "unavailable", "invalid", "view_mismatch"].contains(&view_status) {
                return Err(Issue::Schema);
            }
            let config_span = read_span(view, &span)?;
            let route_span = read_span(route, &span)?;
            if let Some(config) = &config_span {
                last_config = Some(last_config.map_or(config.end, |end: SreTimestamp| end.max(config.end)));
            }
            if let Some(route) = &route_span {
                first_route = Some(first_route.map_or(route.start, |start: SreTimestamp| start.min(route.start)));
            }
            for (index, (read, read_span)) in [(view, &config_span), (route, &route_span)].into_iter().enumerate() {
                if let Some(read_span) = read_span {
                    earliest = earliest.min(read_span.start);
                    if index == 0 {
                        started_config += 1;
                    } else {
                        started_route += 1;
                    }
                    match read["received_body_bytes"].as_u64() {
                        Some(bytes) => received = received.checked_add(bytes).ok_or(Issue::Schema)?,
                        None if read["received_body_bytes"].is_null() => unknown_bytes += 1,
                        None => return Err(Issue::Schema),
                    }
                } else if !read["received_body_bytes"].is_null() {
                    return Err(Issue::Schema);
                }
            }
            let eligible = view_status == "available" && observed_view == expected_view && config_span.is_some();
            if view_status == "available" && !eligible {
                return Err(Issue::Schema);
            }
            let status = route["status"].as_str().ok_or(Issue::Schema)?;
            if ![
                "found",
                "topic_absent",
                "not_attempted",
                "unavailable",
                "permission_denied",
                "unsupported",
                "invalid",
                "too_large",
            ]
            .contains(&status)
            {
                return Err(Issue::Schema);
            }
            if !eligible && status != "not_attempted" {
                return Err(Issue::Schema);
            }
            if (status == "not_attempted") != route_span.is_none() {
                return Err(Issue::Schema);
            }
            let normalized = if status == "found" {
                normalized::validate(&route["normalized"], physical, topic)?;
                unknown_dimensions.extend(normalized::unknowns(&route["normalized"]));
                Some(&route["normalized"])
            } else {
                if !route["normalized"].is_null() {
                    return Err(Issue::Schema);
                }
                None
            };
            complete &= eligible && ["found", "topic_absent"].contains(&status);
            by_peer
                .entry(endpoint)
                .or_default()
                .push((route, normalized, observed_view));
            semantic.insert(
                endpoint,
                json_object([
                    ("flags", view["flags"].clone()),
                    (
                        "configuration_status",
                        Value::from(if view_status == "view_mismatch" {
                            "available"
                        } else {
                            view_status
                        }),
                    ),
                    ("route_status", status.into()),
                    ("route", route["normalized"].clone()),
                ]),
            );
        }
        if last_config
            .zip(first_route)
            .is_some_and(|(config, route)| config > route)
        {
            return Err(Issue::Window);
        }
        semantic_rounds.insert(
            number,
            Value::Object(
                semantic
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value))
                    .collect(),
            ),
        );
    }
    timing_eligible &= nanos(collection.end) - nanos(earliest) <= 5_000_000_000;
    let age = nanos(collection.end)
        .checked_sub(nanos(earliest))
        .filter(|age| *age >= 0)
        .ok_or(Issue::Window)?;
    let source_age = u64::try_from((age + 999_999_999) / 1_000_000_000).map_err(|_| Issue::Window)?;
    if content["source_age_seconds"].as_u64() != Some(source_age) {
        return Err(Issue::Window);
    }
    let statistics = &content["read_statistics"];
    if statistics["planned_config_rpcs"].as_u64() != Some((peers.len() * 2) as u64)
        || statistics["planned_route_rpcs"].as_u64() != Some((peers.len() * 2) as u64)
        || statistics["started_config_rpcs"].as_u64() != Some(started_config)
        || statistics["started_route_rpcs"].as_u64() != Some(started_route)
        || statistics["known_received_body_bytes"].as_u64() != Some(received)
        || statistics["unknown_response_byte_count"].as_u64() != Some(unknown_bytes)
        || statistics["accepted_body_bytes"]
            .as_u64()
            .is_none_or(|bytes| bytes > received)
        || !(statistics["row_work"].is_null() || statistics["row_work"].as_u64().is_some())
    {
        return Err(Issue::Schema);
    }
    complete &= statistics["bounded"].as_bool() == Some(false) && unknown_dimensions.is_empty();
    let mut changed = false;
    let mut differences = BTreeSet::new();
    let mut baseline_status: Option<&Value> = None;
    let mut baseline_route: Option<&Value> = None;
    let mut compared = BTreeSet::new();
    for observations in by_peer.values() {
        if observations.len() != 2 {
            return Err(Issue::Schema);
        }
        let (first, first_route, first_view) = observations[0];
        let (second, second_route, second_view) = observations[1];
        let comparable = |status: &Value| matches!(status["status"].as_str(), Some("found" | "topic_absent"));
        changed |= first_view.iter().all(Option::is_some)
            && second_view.iter().all(Option::is_some)
            && first_view != second_view;
        if comparable(first) && comparable(second) {
            changed |= first["status"] != second["status"]
                || first_route
                    .zip(second_route)
                    .is_some_and(|(left, right)| !normalized::differences(left, right).is_empty());
        }
        if comparable(first) {
            if let Some(baseline_status) = baseline_status {
                compared.insert("topic_presence");
                if first["status"] != *baseline_status {
                    differences.insert("topic_presence");
                }
            } else {
                baseline_status = Some(&first["status"]);
            }
            if let Some(right) = first_route {
                if let Some(left) = baseline_route {
                    compared.extend(
                        normalized::DIMENSIONS
                            .iter()
                            .copied()
                            .filter(|dimension| !unknown_dimensions.contains(dimension)),
                    );
                    differences.extend(normalized::differences(left, right));
                } else {
                    baseline_route = Some(right);
                }
            }
        }
    }
    let state = if !timing_eligible {
        NamesrvComparisonState::Incomparable
    } else if changed {
        NamesrvComparisonState::Transient
    } else if !complete {
        NamesrvComparisonState::Incomparable
    } else if differences.is_empty() {
        NamesrvComparisonState::Equal
    } else {
        NamesrvComparisonState::Difference
    };
    Ok(NamesrvRouteObservation {
        state,
        compared_dimensions: compared.into_iter().collect(),
        differing_dimensions: differences.into_iter().collect(),
        unknown_dimensions: unknown_dimensions.into_iter().collect(),
        timing_eligible,
        change_observed: changed,
        acquisition_span_millis: u64::try_from((age + 999_999) / 1_000_000).map_err(|_| Issue::Window)?,
        source_age_seconds: source_age,
        semantic: Value::Array(semantic_rounds.into_values().collect()),
    })
}

/// Version-owned freshness and scope validation for current-context admission.
pub fn assess_namesrv_route_observation(
    snapshot: &EvidenceSnapshot,
    as_of_unix_ms: i64,
) -> Result<NamesrvRouteObservation, Issue> {
    let content = basic(snapshot)?;
    let collection = interval(&content["collection_started_at"], &content["collected_at"])?;
    if !equivalent(collection.end, snapshot.observed_at) {
        return Err(Issue::Window);
    }
    let now_ns = (as_of_unix_ms as i128)
        .checked_add(1)
        .and_then(|value| value.checked_mul(1_000_000))
        .and_then(|value| value.checked_sub(1))
        .ok_or(Issue::Future)?;
    let elapsed = now_ns
        .checked_sub(nanos(collection.end))
        .filter(|elapsed| *elapsed >= 0)
        .ok_or(Issue::Future)?;
    let observation = compare_namesrv_route_content(content)?;
    if content["comparison"] != observation.comparison_json() {
        return Err(Issue::Schema);
    }
    if snapshot.freshness_seconds != observation.source_age_seconds {
        return Err(Issue::Window);
    }
    let total = (snapshot.freshness_seconds as i128)
        .checked_mul(1_000_000_000)
        .and_then(|age| age.checked_add(elapsed))
        .ok_or(Issue::Stale)?;
    if total > 300_000_000_000 {
        return Err(Issue::Stale);
    }
    Ok(observation)
}

pub(super) fn semantic_fingerprint(snapshot: &EvidenceSnapshot) -> Result<Option<String>, SreContractError> {
    let Ok(content) = basic(snapshot) else {
        return Ok(None);
    };
    let Ok(observation) = compare_namesrv_route_content(content) else {
        return Ok(None);
    };
    canonical_sha256(&(&snapshot.source, &snapshot.resource, &observation.semantic)).map(Some)
}
