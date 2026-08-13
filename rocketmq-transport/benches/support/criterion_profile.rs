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

use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::BenchmarkGroup;

const WARMUP_SECONDS: &str = "ROCKETMQ_REMOTING_COMMAND_BASELINE_WARMUP_SECONDS";
const MEASUREMENT_SECONDS: &str = "ROCKETMQ_REMOTING_COMMAND_BASELINE_MEASUREMENT_SECONDS";
const SAMPLE_SIZE: &str = "ROCKETMQ_REMOTING_COMMAND_BASELINE_SAMPLE_SIZE";

#[derive(Clone, Copy)]
struct CriterionProfile {
    warmup_seconds: u64,
    measurement_seconds: u64,
    sample_size: usize,
}

impl CriterionProfile {
    const QUICK: Self = Self {
        warmup_seconds: 1,
        measurement_seconds: 2,
        sample_size: 10,
    };

    fn from_environment() -> Option<Self> {
        let values = [
            std::env::var(WARMUP_SECONDS).ok(),
            std::env::var(MEASUREMENT_SECONDS).ok(),
            std::env::var(SAMPLE_SIZE).ok(),
        ];
        if values.iter().all(Option::is_none) {
            return None;
        }

        let parse = |name: &str, value: &Option<String>| {
            value
                .as_deref()
                .unwrap_or_else(|| panic!("{name} must be set with the other formal benchmark settings"))
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
        };
        let profile = Self {
            warmup_seconds: parse(WARMUP_SECONDS, &values[0]),
            measurement_seconds: parse(MEASUREMENT_SECONDS, &values[1]),
            sample_size: parse(SAMPLE_SIZE, &values[2])
                .try_into()
                .expect("formal benchmark sample size must fit usize"),
        };
        assert!(profile.warmup_seconds > 0, "{WARMUP_SECONDS} must be positive");
        assert!(
            profile.measurement_seconds > 0,
            "{MEASUREMENT_SECONDS} must be positive"
        );
        assert!(profile.sample_size >= 10, "{SAMPLE_SIZE} must be at least 10");
        Some(profile)
    }
}

pub(crate) fn apply_remoting_command_baseline_profile(group: &mut BenchmarkGroup<'_, WallTime>) {
    let profile = CriterionProfile::from_environment().unwrap_or(CriterionProfile::QUICK);
    group.sample_size(profile.sample_size);
    group.warm_up_time(Duration::from_secs(profile.warmup_seconds));
    group.measurement_time(Duration::from_secs(profile.measurement_seconds));
}
