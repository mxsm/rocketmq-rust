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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

const SAMPLE_SCALE: u64 = 1_000_000;

#[derive(Debug)]
pub struct SamplingGate {
    sampled_per_million: u64,
    accumulator: AtomicU64,
}

impl SamplingGate {
    pub fn new(sample_ratio: f64) -> Self {
        Self {
            sampled_per_million: sampled_per_million(sample_ratio),
            accumulator: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn should_sample(&self) -> bool {
        match self.sampled_per_million {
            0 => false,
            SAMPLE_SCALE => true,
            sampled_per_million => {
                let previous = self.accumulator.fetch_add(sampled_per_million, Ordering::Relaxed);
                previous % SAMPLE_SCALE + sampled_per_million >= SAMPLE_SCALE
            }
        }
    }

    pub fn sampled_per_million(&self) -> u64 {
        self.sampled_per_million
    }
}

impl Default for SamplingGate {
    fn default() -> Self {
        Self::new(1.0)
    }
}

fn sampled_per_million(sample_ratio: f64) -> u64 {
    if sample_ratio.is_nan() || sample_ratio <= 0.0 {
        return 0;
    }
    if sample_ratio >= 1.0 {
        return SAMPLE_SCALE;
    }
    (sample_ratio * SAMPLE_SCALE as f64).ceil() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_sample_ratio_always_samples() {
        let gate = SamplingGate::new(1.0);

        assert_eq!(gate.sampled_per_million(), SAMPLE_SCALE);
        assert!((0..8).all(|_| gate.should_sample()));
    }

    #[test]
    fn non_positive_or_invalid_ratio_never_samples() {
        for ratio in [0.0, -1.0, f64::NAN, f64::NEG_INFINITY] {
            let gate = SamplingGate::new(ratio);

            assert_eq!(gate.sampled_per_million(), 0);
            assert!((0..8).all(|_| !gate.should_sample()));
        }
    }

    #[test]
    fn fractional_ratio_is_evenly_spread() {
        let one_quarter = SamplingGate::new(0.25);
        let three_quarters = SamplingGate::new(0.75);

        assert_eq!((0..8).filter(|_| one_quarter.should_sample()).count(), 2);
        assert_eq!((0..8).filter(|_| three_quarters.should_sample()).count(), 6);
    }
}
