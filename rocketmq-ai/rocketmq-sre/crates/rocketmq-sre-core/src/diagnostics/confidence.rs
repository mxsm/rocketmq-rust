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

use super::ConfidenceBand;
use super::ConfidenceScore;

/// Integer-only inputs to deterministic confidence calculation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConfidenceInputs {
    pub required_total: u16,
    pub required_available: u16,
    pub optional_total: u16,
    pub optional_available: u16,
    pub supporting_signals: u16,
    pub counter_signals: u16,
    pub partial_evidence: u16,
    pub missing_required: u16,
    pub unsupported_required: u16,
}

/// Calculates reproducible confidence without model-provided scores.
///
/// Required coverage contributes most of the score. Missing required evidence
/// caps confidence at 49, while local-only or otherwise unsupported required
/// evidence caps it at 24. Partial snapshots and counter-signals reduce the
/// score. The integer formula is stable across platforms.
#[must_use]
pub fn calculate_confidence(inputs: ConfidenceInputs) -> ConfidenceScore {
    let required_points = ratio_points(inputs.required_available, inputs.required_total, 40);
    let optional_points = ratio_points(inputs.optional_available, inputs.optional_total, 10);
    let support_points = inputs.supporting_signals.min(3) * 8;
    let counter_penalty = inputs.counter_signals.min(3) * 10;
    let partial_penalty = inputs.partial_evidence.min(3) * 5;

    let positive = 30_u16
        .saturating_add(required_points)
        .saturating_add(optional_points)
        .saturating_add(support_points);
    let penalty = counter_penalty.saturating_add(partial_penalty);
    let mut percent = positive.saturating_sub(penalty).min(99) as u8;

    if inputs.missing_required > 0 {
        percent = percent.min(49);
    }
    if inputs.unsupported_required > 0 {
        percent = percent.min(24);
    }

    let band = match percent {
        75..=u8::MAX => ConfidenceBand::High,
        50..=74 => ConfidenceBand::Medium,
        _ => ConfidenceBand::Low,
    };
    let explanation = format!(
        "required={}/{}, optional={}/{}, support={}, counter={}, partial={}, missing_required={}, \
         unsupported_required={}",
        inputs.required_available,
        inputs.required_total,
        inputs.optional_available,
        inputs.optional_total,
        inputs.supporting_signals,
        inputs.counter_signals,
        inputs.partial_evidence,
        inputs.missing_required,
        inputs.unsupported_required,
    );

    ConfidenceScore {
        percent,
        band,
        explanation,
    }
}

fn ratio_points(available: u16, total: u16, maximum: u16) -> u16 {
    if total == 0 {
        return 0;
    }
    available.min(total).saturating_mul(maximum) / total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_inputs_produce_identical_confidence() {
        let inputs = ConfidenceInputs {
            required_total: 2,
            required_available: 2,
            optional_total: 2,
            optional_available: 1,
            supporting_signals: 2,
            counter_signals: 1,
            partial_evidence: 0,
            missing_required: 0,
            unsupported_required: 0,
        };

        assert_eq!(calculate_confidence(inputs), calculate_confidence(inputs));
    }

    #[test]
    fn missing_and_unsupported_required_evidence_are_never_high_confidence() {
        let optimistic = ConfidenceInputs {
            required_total: 2,
            required_available: 2,
            optional_total: 4,
            optional_available: 4,
            supporting_signals: 10,
            ..ConfidenceInputs::default()
        };

        let missing = calculate_confidence(ConfidenceInputs {
            missing_required: 1,
            ..optimistic
        });
        let unsupported = calculate_confidence(ConfidenceInputs {
            unsupported_required: 1,
            ..optimistic
        });

        assert_eq!(missing.percent, 49);
        assert_eq!(missing.band, ConfidenceBand::Low);
        assert_eq!(unsupported.percent, 24);
        assert_eq!(unsupported.band, ConfidenceBand::Low);
    }
}
