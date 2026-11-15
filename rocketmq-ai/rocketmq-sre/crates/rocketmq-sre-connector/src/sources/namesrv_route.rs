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

//! One fixed two-round direct-peer read. Private response bodies never escape.

use chrono::Utc;
use rocketmq_admin_core::core::nameserver_route::{
    NameserverObservationBounds, NameserverObservationReply, NameserverObservationRequest, NameserverObservationStatus,
    NameserverRouteView,
};
use rocketmq_admin_core::read_client_adapter::ReadAdminSession;
use rocketmq_sre_contracts::{CoverageStatus, EvidenceExposure};
use rocketmq_sre_core::diagnostics::packs::{
    NAMESRV_MISSING_FACTS, NAMESRV_ROUTE_SCHEMA, compare_namesrv_route_content,
};
use serde_json::{Value, json};

use super::common::{SourceOutput, bounded_future};
use crate::config::{NamesrvRouteConfig, SourceLimits};
use crate::read_gateway::ReadContext;
use crate::{ConnectorError, ConnectorFailure};

#[cfg(test)]
mod tests;

pub(super) trait NamesrvPeerReader {
    async fn observe(
        &mut self,
        request: &NameserverObservationRequest,
    ) -> Result<NameserverObservationReply, ConnectorError>;
}
impl NamesrvPeerReader for ReadAdminSession {
    async fn observe(
        &mut self,
        request: &NameserverObservationRequest,
    ) -> Result<NameserverObservationReply, ConnectorError> {
        self.observe_nameserver(request)
            .await
            .map_err(|_| ConnectorError::source("configured NameServer observation failed"))
    }
}

struct Budget {
    max_bytes: usize,
    max_rows: usize,
    accepted: usize,
    rows: usize,
    row_work: u64,
    rows_known: bool,
    known_received: u64,
    unknown_bytes: u64,
    started_config: u64,
    started_route: u64,
    bounded: bool,
}
impl Budget {
    fn new(limits: &SourceLimits) -> Self {
        Self {
            max_bytes: limits.max_bytes,
            max_rows: limits.max_rows,
            accepted: 0,
            rows: 0,
            row_work: 0,
            rows_known: true,
            known_received: 0,
            unknown_bytes: 0,
            started_config: 0,
            started_route: 0,
            bounded: false,
        }
    }
    fn remaining(&self) -> Option<(usize, usize)> {
        if self.bounded {
            return None;
        }
        self.max_bytes
            .checked_sub(self.accepted)
            .zip(self.max_rows.checked_sub(self.rows))
            .filter(|(bytes, rows)| *bytes > 0 && *rows > 0)
    }
    fn accept(&mut self, reply: &NameserverObservationReply) -> Result<(), ConnectorError> {
        if let Some(bytes) = reply.received_body_bytes {
            self.known_received = self
                .known_received
                .checked_add(bytes as u64)
                .ok_or_else(contract_error)?;
            if bytes > self.max_bytes.saturating_sub(self.accepted) {
                self.bounded = true;
            } else {
                self.accepted += bytes;
            }
        } else {
            self.unknown_bytes += 1;
        }
        match reply.rows {
            Some(rows) => {
                self.row_work = self.row_work.checked_add(rows as u64).ok_or_else(contract_error)?;
                if rows <= self.max_rows.saturating_sub(self.rows) {
                    self.rows += rows;
                } else {
                    self.bounded = true;
                }
            }
            None => {
                self.rows_known = false;
                // A malformed received body has no established work bound. A
                // failed transport has no accepted body and may leave peers readable.
                if reply.received_body_bytes.is_some_and(|bytes| bytes > 0) {
                    self.bounded = true;
                }
            }
        }
        self.bounded |= matches!(
            reply.status,
            NameserverObservationStatus::TooLarge | NameserverObservationStatus::RowLimit
        );
        Ok(())
    }
}

fn flags(view: Option<NameserverRouteView>) -> Value {
    match view {
        Some(view) => {
            json!({"cluster_test":view.cluster_test,"order_message_enable":view.order_message_enable,"support_acting_master":view.support_acting_master})
        }
        None => json!({"cluster_test":null,"order_message_enable":null,"support_acting_master":null}),
    }
}
fn no_config() -> Value {
    json!({"status":"unavailable","reason_code":"source_response_bounded","flags":flags(None),"started_at":null,"finished_at":null,"received_body_bytes":null})
}
fn no_route(reason: &str) -> Value {
    json!({"status":"not_attempted","reason_code":reason,"started_at":null,"finished_at":null,"received_body_bytes":null,"normalized":null})
}
fn contract_error() -> ConnectorError {
    ConnectorError::new(
        ConnectorFailure::InvalidEvidenceQuery,
        false,
        "invalid bounded NameServer observation contract",
    )
}

async fn read<R: NamesrvPeerReader>(
    reader: &mut R,
    request: &NameserverObservationRequest,
    context: &ReadContext<'_>,
) -> Result<NameserverObservationReply, ConnectorError> {
    match bounded_future(context.deadline, context.cancel, Box::pin(reader.observe(request))).await {
        Ok(reply) => Ok(reply),
        Err(error) if error.failure() == ConnectorFailure::SourceUnavailable => Ok(NameserverObservationReply {
            status: NameserverObservationStatus::Unavailable,
            received_body_bytes: None,
            rows: None,
            view: None,
            route: None,
        }),
        Err(error) => Err(error),
    }
}
fn bounds(
    budget: &Budget,
    context: &ReadContext<'_>,
    rpc_timeout: std::time::Duration,
) -> Result<Option<NameserverObservationBounds>, ConnectorError> {
    if context.cancel.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorFailure::QueryCancelled,
            false,
            "NameServer observation cancelled",
        ));
    }
    let millis = context.deadline.signed_duration_since(Utc::now()).num_milliseconds();
    if millis <= 0 {
        return Err(ConnectorError::new(
            ConnectorFailure::DeadlineExceeded,
            true,
            "NameServer observation deadline elapsed",
        ));
    }
    Ok(budget
        .remaining()
        .map(|(max_body_bytes, max_rows)| NameserverObservationBounds {
            timeout_millis: (millis as u64).min(rpc_timeout.as_millis() as u64).max(1),
            max_body_bytes,
            max_rows,
        }))
}

pub(super) async fn collect<R: NamesrvPeerReader>(
    reader: &mut R,
    context: &ReadContext<'_>,
    topic: &str,
    config: &NamesrvRouteConfig,
    limits: &SourceLimits,
    rpc_timeout: std::time::Duration,
) -> Result<SourceOutput, ConnectorError> {
    if config.cluster_id != context.cluster_id {
        return Err(ConnectorError::new(
            ConnectorFailure::ClusterNotAllowed,
            false,
            "NameServer observation configured cluster scope mismatch",
        ));
    }
    let started = Utc::now();
    let mut budget = Budget::new(limits);
    let mut rounds = Vec::with_capacity(2);
    let configured_peers=config.peers.iter().map(|peer|Ok(json!({"peer_id":peer.peer_id,"instance_reference":peer.instance_reference,
        "peer_endpoint_id":config.key.identify(context.tenant_id,context.cluster_id,"nameserver-target",&peer.endpoint)?}))).collect::<Result<Vec<_>,ConnectorError>>()?;
    for number in 1..=2 {
        let round_started = Utc::now();
        let mut peers = Vec::with_capacity(config.peers.len());
        // Configuration first for every peer. An unsafe/missing view never
        // starts the route RPC, including clusterTest's external fallback path.
        for (peer, safe_peer) in config.peers.iter().zip(&configured_peers) {
            let mut config_read = no_config();
            if let Some(bounds) = bounds(&budget, context, rpc_timeout)? {
                let request = NameserverObservationRequest::configuration(&peer.endpoint, bounds)
                    .map_err(|_| contract_error())?;
                let at = Utc::now();
                budget.started_config += 1;
                let reply = read(reader, &request, context).await?;
                let finished = Utc::now();
                budget.accept(&reply)?;
                let eligible = reply.status == NameserverObservationStatus::Available
                    && reply.view == Some(config.expected_view)
                    && !budget.bounded;
                let (status, reason) = if eligible {
                    ("available", "view_configuration_observed")
                } else if reply.view.is_some() {
                    ("view_mismatch", "view_configuration_mismatch")
                } else if matches!(
                    reply.status,
                    NameserverObservationStatus::Invalid | NameserverObservationStatus::ScopeMismatch
                ) {
                    ("invalid", "view_configuration_invalid")
                } else {
                    ("unavailable", "view_configuration_unavailable")
                };
                config_read = json!({"status":status,"reason_code":reason,"flags":flags(reply.view),"started_at":at,"finished_at":finished,"received_body_bytes":reply.received_body_bytes});
            }
            peers.push(json!({"peer_id":safe_peer["peer_id"],"peer_endpoint_id":safe_peer["peer_endpoint_id"],"configuration":config_read,"route":no_route("route_not_attempted_view_ineligible")}));
        }
        for (peer, row) in config.peers.iter().zip(&mut peers) {
            if row["configuration"]["status"] != "available" {
                continue;
            }
            let Some(bounds) = bounds(&budget, context, rpc_timeout)? else {
                row["route"] = no_route("source_response_bounded");
                continue;
            };
            let request = NameserverObservationRequest::route(&peer.endpoint, &config.physical_cluster, topic, bounds)
                .map_err(|_| contract_error())?;
            let at = Utc::now();
            budget.started_route += 1;
            let reply = read(reader, &request, context).await?;
            let finished = Utc::now();
            budget.accept(&reply)?;
            let mut mapping_error = None;
            let normalized = reply.route.map(|route| {
                route.into_pseudonymized(|endpoint| {
                    match config
                        .key
                        .identify(context.tenant_id, context.cluster_id, "route-endpoint", endpoint)
                    {
                        Ok(value) => value,
                        Err(error) => {
                            mapping_error = Some(error);
                            String::new()
                        }
                    }
                })
            });
            if let Some(error) = mapping_error {
                return Err(error);
            }
            let status = match reply.status {
                NameserverObservationStatus::Available if normalized.is_some() => "found",
                NameserverObservationStatus::Available
                | NameserverObservationStatus::Invalid
                | NameserverObservationStatus::ScopeMismatch => "invalid",
                NameserverObservationStatus::TopicAbsent => "topic_absent",
                NameserverObservationStatus::PermissionDenied => "permission_denied",
                NameserverObservationStatus::Unsupported => "unsupported",
                NameserverObservationStatus::Unavailable => "unavailable",
                NameserverObservationStatus::TooLarge | NameserverObservationStatus::RowLimit => "too_large",
            };
            row["route"] = json!({"status":status,"reason_code":reply.status.reason_code(),"started_at":at,"finished_at":finished,"received_body_bytes":reply.received_body_bytes,"normalized":normalized});
        }
        rounds.push(json!({"round":number,"started_at":round_started,"finished_at":Utc::now(),"peers":peers}));
    }
    let collected = Utc::now();
    let nanos = collected
        .signed_duration_since(started)
        .num_nanoseconds()
        .ok_or_else(contract_error)?;
    let source_age = u64::try_from(nanos)
        .map_err(|_| contract_error())?
        .div_ceil(1_000_000_000);
    let mut content = json!({"schema_version":NAMESRV_ROUTE_SCHEMA,"profile":"nameserver_route_comparison",
        "tenant_id":context.tenant_id,"cluster_id":context.cluster_id,"topic":topic,
        "requested_window":{"start":context.time_range_start,"end":context.time_range_end},"collection_started_at":started,"collected_at":collected,
        "source_age_seconds":source_age,"temporal_semantics":"current_context_only","historical_coverage":"unknown",
        "configuration":{"physical_cluster":config.physical_cluster,"revision":config.revision,"deployment_reference":config.deployment_reference,
            "independence_source":"operator_declared_direct_peers","expected_view":config.expected_view,"fingerprint_algorithm":"hmac-sha256-v1",
            "fingerprint_domain":config.key.identify(context.tenant_id,context.cluster_id,"fingerprint-domain","")?,"peers":configured_peers},
        "rounds":rounds,"read_statistics":{"planned_config_rpcs":2*config.peers.len(),"planned_route_rpcs":2*config.peers.len(),
            "started_config_rpcs":budget.started_config,"started_route_rpcs":budget.started_route,"known_received_body_bytes":budget.known_received,
            "unknown_response_byte_count":budget.unknown_bytes,"accepted_body_bytes":budget.accepted,"row_work":budget.rows_known.then_some(budget.row_work),"bounded":budget.bounded},
        "missing_facts":NAMESRV_MISSING_FACTS});
    // Age is measured from the earliest actual RPC, not preparation time.
    let earliest = content["rounds"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|round| round["peers"].as_array().into_iter().flatten())
        .flat_map(|peer| [&peer["configuration"], &peer["route"]])
        .filter_map(|read| {
            read["started_at"]
                .as_str()
                .and_then(|value| value.parse::<chrono::DateTime<Utc>>().ok())
        })
        .min()
        .unwrap_or(collected);
    let source_age = u64::try_from(
        collected
            .signed_duration_since(earliest)
            .num_nanoseconds()
            .ok_or_else(contract_error)?,
    )
    .map_err(|_| contract_error())?
    .div_ceil(1_000_000_000);
    content["source_age_seconds"] = json!(source_age);
    let observation = compare_namesrv_route_content(&content).map_err(|_| contract_error())?;
    content["comparison"] = observation.comparison_json();
    if serde_json::to_vec(&content).map_err(|_| contract_error())?.len()
        > limits
            .max_bytes
            .min(rocketmq_sre_contracts::CONNECTOR_RESPONSE_MAX_BYTES)
    {
        return Err(ConnectorError::new(
            ConnectorFailure::OutputTooLarge,
            false,
            "safe NameServer observation exceeds output bound",
        ));
    }
    let mut output = SourceOutput::available(content, collected).with_exposure(EvidenceExposure::AdminRpc);
    output.freshness_seconds = source_age;
    output.partial = true;
    output.coverage = CoverageStatus::Partial;
    output.warnings = vec!["namesrv_current_context_only".to_owned()];
    Ok(output)
}

/// Measure the real wire DTO with a maximum-width sequence before accepting a
/// snapshot. UUID wire widths and schema match the normal channel; no payload
/// is trimmed and no remote write is attempted by this check.
pub(super) fn bounded_capture(
    snapshot: rocketmq_sre_contracts::EvidenceSnapshot,
) -> Result<rocketmq_sre_contracts::EvidenceSnapshot, ConnectorError> {
    let envelope = rocketmq_sre_contracts::ConnectorResponseEnvelope {
        schema: rocketmq_sre_contracts::SchemaVersion::new("rocketmq-sre.connector-channel", 1, 0),
        session_id: rocketmq_sre_contracts::ConnectorSessionId::from_uuid(snapshot.evidence_id.as_uuid()),
        correlation_id: snapshot.correlation_id,
        sequence: u64::MAX,
        evidence: Some(snapshot),
        error_code: None,
        retryable: false,
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|_| contract_error())?;
    if bytes.len() > rocketmq_sre_contracts::CONNECTOR_RESPONSE_MAX_BYTES {
        return Err(ConnectorError::new(
            ConnectorFailure::OutputTooLarge,
            false,
            "NameServer response exceeds the complete channel envelope bound",
        ));
    }
    envelope.evidence.ok_or_else(contract_error)
}
