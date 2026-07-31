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

//! Bounded, privacy-preserving telemetry for the AI SRE control plane.
//!
//! This module deliberately accepts enums instead of arbitrary label strings.
//! That keeps every metric surface finite and prevents tenant, cluster,
//! incident, prompt, evidence, tool arguments, or credentials from becoming
//! metric labels. High-cardinality operation identity is carried only by the
//! trace correlation identifier.

#[cfg(test)]
mod asset_tests;
mod correlation;
mod health;
mod metrics;
mod spans;

pub use correlation::CORRELATION_ID_HEADER;
pub use correlation::CorrelationContext;
pub use health::ConnectorHealthSample;
pub use health::DatabaseHealthSample;
pub use health::DependencyStatus;
pub use health::HealthAggregator;
pub use health::HealthReasonCode;
pub use health::ProviderHealthSample;
pub use health::SreHealthViewV1;
pub use metrics::DiagnosticPackLabel;
pub use metrics::EvidenceSourceLabel;
pub use metrics::IncidentOutcome;
pub use metrics::ModelPurposeLabel;
pub use metrics::ModelTokenDirection;
pub use metrics::ProviderFamilyLabel;
pub use metrics::ResultClass;
pub use metrics::SreMetricSink;
pub use metrics::SreMetrics;
pub use metrics::ToolClassLabel;
pub use spans::SPAN_DIAGNOSTIC_EVALUATE;
pub use spans::SPAN_EVIDENCE_COLLECT;
pub use spans::SPAN_INCIDENT_RUN;
pub use spans::SPAN_MODEL_INVOKE;
pub use spans::SreObservability;
