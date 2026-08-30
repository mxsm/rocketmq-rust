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

use super::capacity::CapacityEvaluation;
use super::capacity::evaluate_capacity;
use super::trend::ObservedPoint;
use super::trend::TrendPolicy;

/// Evaluates disk usage against a configured used-capacity threshold.
///
/// # Errors
///
/// Returns a stable reason when the threshold or trend policy is invalid.
pub fn evaluate_disk_runway(
    used_capacity: &[ObservedPoint],
    policy: TrendPolicy,
    used_threshold: f64,
    now_seconds: i64,
) -> Result<CapacityEvaluation, &'static str> {
    if !used_threshold.is_finite() || used_threshold <= 0.0 {
        return Err("invalid_disk_threshold");
    }
    evaluate_capacity(used_capacity, policy, used_threshold, now_seconds)
}
