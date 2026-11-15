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

//! Full, presence-aware private route observations, separate from legacy DTOs.

mod decode;
mod normalize;
#[cfg(test)]
mod tests;

use crate::core::{AdminError, AdminResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use decode::decode_nameserver_view;
#[cfg(feature = "read-client-adapter")]
pub(crate) use decode::{route_row_work, view_row_work};
pub use normalize::decode_nameserver_route;

/// Only these source flags are retained from the full private configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NameserverRouteView {
    pub cluster_test: bool,
    pub order_message_enable: bool,
    pub support_acting_master: bool,
}

/// Address-free outcome; errors never contain the original wire/config body.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NameserverObservationStatus {
    Available,
    TopicAbsent,
    PermissionDenied,
    Unsupported,
    Unavailable,
    Invalid,
    TooLarge,
    ScopeMismatch,
    RowLimit,
}

impl NameserverObservationStatus {
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::Available => "route_observed",
            Self::TopicAbsent => "topic_absent",
            Self::PermissionDenied => "route_permission_denied",
            Self::Unsupported => "route_source_unsupported",
            Self::Unavailable => "route_source_unavailable",
            Self::Invalid => "route_schema_invalid",
            Self::TooLarge => "route_response_too_large",
            Self::ScopeMismatch => "route_scope_mismatch",
            Self::RowLimit => "source_row_budget_exceeded",
        }
    }
}

/// Limits apply to this one child read; collectors pass their remaining total.
#[derive(Clone, Copy, Debug)]
pub struct NameserverObservationBounds {
    pub timeout_millis: u64,
    pub max_body_bytes: usize,
    pub max_rows: usize,
}

impl NameserverObservationBounds {
    pub fn validate(self) -> AdminResult<()> {
        if self.timeout_millis == 0
            || self.timeout_millis > 3_600_000
            || self.max_body_bytes == 0
            || self.max_body_bytes > 16 * 1024 * 1024
            || self.max_rows == 0
            || self.max_rows > 100_000
        {
            return Err(AdminError::invalid_argument(
                "observation_bounds",
                "invalid observation bounds",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct NameserverObservationRequest {
    endpoint: String,
    pub(crate) topic: Option<String>,
    pub(crate) cluster: Option<String>,
    pub bounds: NameserverObservationBounds,
}

impl NameserverObservationRequest {
    pub fn configuration(endpoint: &str, bounds: NameserverObservationBounds) -> AdminResult<Self> {
        bounds.validate()?;
        let endpoint = super::broker::canonical_remoting_endpoint(endpoint)
            .ok_or_else(|| AdminError::invalid_argument("nameserver", "invalid configured target"))?;
        Ok(Self {
            endpoint,
            topic: None,
            cluster: None,
            bounds,
        })
    }

    pub fn route(endpoint: &str, cluster: &str, topic: &str, bounds: NameserverObservationBounds) -> AdminResult<Self> {
        let mut request = Self::configuration(endpoint, bounds)?;
        for value in [cluster, topic] {
            if value.is_empty() || value.len() > 255 || value.chars().any(char::is_control) {
                return Err(AdminError::invalid_argument("route_scope", "invalid exact route scope"));
            }
        }
        request.topic = Some(topic.to_owned());
        request.cluster = Some(cluster.to_owned());
        Ok(request)
    }

    /// Canonical private target supplied by the trusted adapter. Do not include
    /// this value in user-facing evidence, logging or diagnostics.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl std::fmt::Debug for NameserverObservationRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NameserverObservationRequest")
            .field("target", &"[CONFIGURED]")
            .field("bounds", &self.bounds)
            .finish_non_exhaustive()
    }
}

/// Private normalized route: safe JSON requires an explicit keyed endpoint map.
/// There is intentionally no Serialize implementation or body-revealing Debug.
pub struct NameserverRouteObservation {
    pub(super) normalized: Value,
    pub rows: usize,
}

impl std::fmt::Debug for NameserverRouteObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NameserverRouteObservation")
            .field("rows", &self.rows)
            .finish_non_exhaustive()
    }
}

impl NameserverRouteObservation {
    /// Maps all private endpoints with one caller-owned stable scope domain.
    /// The caller must not substitute unkeyed hashes or per-peer/capture salts.
    pub fn into_pseudonymized(mut self, mut endpoint_id: impl FnMut(&str) -> String) -> Value {
        if let Some(brokers) = self.normalized["brokers"].as_object_mut() {
            for broker in brokers.values_mut() {
                if let Some(addresses) = broker["addresses"].as_object_mut() {
                    for address in addresses.values_mut() {
                        if let Some(value) = address.as_str() {
                            *address = Value::String(endpoint_id(value));
                        }
                    }
                }
            }
        }
        if let Some(table) = self.normalized["filter_servers"].as_object_mut() {
            let private = std::mem::take(table);
            for (broker, filters) in private {
                let mut mapped = filters
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(&mut endpoint_id)
                    .collect::<Vec<_>>();
                mapped.sort();
                table.insert(endpoint_id(&broker), serde_json::json!(mapped));
            }
        }
        self.normalized
    }
}

/// One actual child response. Failed transport byte counts remain unknown.
#[derive(Debug)]
pub struct NameserverObservationReply {
    pub status: NameserverObservationStatus,
    pub received_body_bytes: Option<usize>,
    pub rows: Option<usize>,
    pub view: Option<NameserverRouteView>,
    pub route: Option<NameserverRouteObservation>,
}
