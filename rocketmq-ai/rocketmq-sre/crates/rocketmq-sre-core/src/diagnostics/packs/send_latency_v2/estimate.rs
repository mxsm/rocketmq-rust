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

use std::collections::BTreeSet;

#[cfg(test)]
mod tests;

/// The locked native OTel explicit grid, in recorded milliseconds; `None` is +Inf.
pub const LATENCY_GRID: [Option<f64>; 16] = [
    Some(0.0),
    Some(5.0),
    Some(10.0),
    Some(25.0),
    Some(50.0),
    Some(75.0),
    Some(100.0),
    Some(250.0),
    Some(500.0),
    Some(750.0),
    Some(1000.0),
    Some(2500.0),
    Some(5000.0),
    Some(7500.0),
    Some(10000.0),
    None,
];
const MAX_EXACT_COUNT: f64 = 9_007_199_254_740_991.0;
const MAX_HISTOGRAMS: usize = 64;
const MAX_RAW_POINTS: usize = 10_000;

/// One actual stored scrape, not a query-range evaluation point.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LatencyCounterSample {
    pub at_millis: i64,
    pub count: f64,
}

/// Original counter samples paired with the server's per-series increase.
#[derive(Clone, Debug, PartialEq)]
pub struct LatencyBucket {
    pub upper_bound_millis: Option<f64>,
    pub samples: Vec<LatencyCounterSample>,
    pub increase: f64,
}

/// One logical node's original histogram. Scrape duplicates must not be merged.
#[derive(Clone, Debug, PartialEq)]
pub struct LatencyHistogram {
    pub node_id: String,
    pub buckets: Vec<LatencyBucket>,
}

/// Distinguishes an interpolated finite bucket from an unresolved +Inf tail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyResolution {
    FiniteBucket,
    TailLimited,
}

/// A classic P99 estimate and its bucket resolution, not an exact path latency.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LatencyQuantile {
    pub estimate_millis: f64,
    pub lower_bound_millis: f64,
    pub upper_bound_millis: Option<f64>,
    pub resolution: LatencyResolution,
}

/// Eligibility for this estimator's observed population only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyWindowState {
    Available,
    NoRecordedTraffic,
    InsufficientPopulation,
    TailLimited,
}

/// Bounded facts derived from actual samples and server-side per-series increases.
#[derive(Clone, Debug, PartialEq)]
pub struct LatencyWindowAssessment {
    pub state: LatencyWindowState,
    pub quantile: Option<LatencyQuantile>,
    pub cumulative_increases: Vec<f64>,
    pub observed_increments: f64,
    pub estimated_increments: f64,
    pub observed_nodes: u64,
    pub scrape_samples: u64,
    pub observed_resets: u64,
    pub data_first_millis: i64,
    pub data_last_millis: i64,
    pub oldest_last_millis: i64,
}

/// Stable failure classes; none implies zero traffic or healthy latency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyWindowIssue {
    MissingHistogram,
    UnsupportedWindow,
    AmbiguousPopulation,
    UnrecognizedGrid,
    InvalidDistribution,
    InvalidSampleTime,
    InsufficientSamples,
    IncompleteSpan,
    ScrapeGap,
    BoundExceeded,
}

impl LatencyWindowIssue {
    /// Closed source reason suitable for an authorized diagnostic view.
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::MissingHistogram => "missing_histogram",
            Self::UnsupportedWindow => "unsupported_window_resolution",
            Self::AmbiguousPopulation => "ambiguous_metric_population",
            Self::UnrecognizedGrid => "unrecognized_bucket_grid",
            Self::InvalidDistribution => "invalid_histogram_distribution",
            Self::InvalidSampleTime => "invalid_histogram_sample_time",
            Self::InsufficientSamples => "insufficient_scrape_samples",
            Self::IncompleteSpan => "incomplete_window_span",
            Self::ScrapeGap => "scrape_gap_exceeded",
            Self::BoundExceeded => "source_response_bounded",
        }
    }
}

/// Validates original histograms before summing their per-series increases.
///
/// The source window is left-open/right-closed on the millisecond grid. The
/// 80% span, 60-second gap and 100-increment floor are engineering eligibility
/// policy, not Prometheus requirements or statistical confidence guarantees.
///
/// # Errors
/// Rejects incomplete grids, ambiguous populations, invalid counts/timestamps,
/// insufficient scrape coverage, and bounded arithmetic or sample limits.
pub fn assess_latency_histograms(
    start_millis: i64,
    end_millis: i64,
    histograms: &[LatencyHistogram],
) -> Result<LatencyWindowAssessment, LatencyWindowIssue> {
    let width = end_millis
        .checked_sub(start_millis)
        .filter(|width| *width > 0)
        .ok_or(LatencyWindowIssue::UnsupportedWindow)?;
    if histograms.is_empty() {
        return Err(LatencyWindowIssue::MissingHistogram);
    }
    if histograms.len() > MAX_HISTOGRAMS {
        return Err(LatencyWindowIssue::BoundExceeded);
    }
    let mut result = LatencyWindowAssessment {
        state: LatencyWindowState::Available,
        quantile: None,
        cumulative_increases: vec![0.0; LATENCY_GRID.len()],
        observed_increments: 0.0,
        estimated_increments: 0.0,
        observed_nodes: histograms.len() as u64,
        scrape_samples: 0,
        observed_resets: 0,
        data_first_millis: i64::MAX,
        data_last_millis: i64::MIN,
        oldest_last_millis: i64::MAX,
    };
    let mut nodes = BTreeSet::new();
    let mut points = 0_usize;
    for histogram in histograms {
        if histogram.node_id.is_empty()
            || histogram.node_id.len() > 255
            || histogram.node_id == "other"
            || histogram.node_id.chars().any(char::is_control)
            || !nodes.insert(&histogram.node_id)
        {
            return Err(LatencyWindowIssue::AmbiguousPopulation);
        }
        if histogram.buckets.len() != LATENCY_GRID.len() {
            return Err(LatencyWindowIssue::UnrecognizedGrid);
        }
        let reference = &histogram.buckets[0].samples;
        validate_times(start_millis, end_millis, width, reference)?;
        let first = reference[0].at_millis;
        let last = reference[reference.len() - 1].at_millis;
        result.data_first_millis = result.data_first_millis.min(first);
        result.data_last_millis = result.data_last_millis.max(last);
        result.oldest_last_millis = result.oldest_last_millis.min(last);
        result.scrape_samples = result
            .scrape_samples
            .checked_add(reference.len() as u64)
            .ok_or(LatencyWindowIssue::BoundExceeded)?;
        let mut resets = BTreeSet::new();
        let mut previous_increase = 0.0;
        for (index, bucket) in histogram.buckets.iter().enumerate() {
            if bucket.upper_bound_millis != LATENCY_GRID[index] {
                return Err(LatencyWindowIssue::UnrecognizedGrid);
            }
            points = points
                .checked_add(bucket.samples.len())
                .filter(|points| *points <= MAX_RAW_POINTS)
                .ok_or(LatencyWindowIssue::BoundExceeded)?;
            if bucket.samples.len() != reference.len()
                || !finite_count(bucket.increase)
                || bucket.increase < previous_increase
            {
                return Err(LatencyWindowIssue::InvalidDistribution);
            }
            previous_increase = bucket.increase;
            let mut delta = 0.0;
            for (sample_index, sample) in bucket.samples.iter().enumerate() {
                if sample.at_millis != reference[sample_index].at_millis
                    || !finite_count(sample.count)
                    || sample.count.fract() != 0.0
                {
                    return Err(LatencyWindowIssue::InvalidDistribution);
                }
                if index > 0 && sample.count < histogram.buckets[index - 1].samples[sample_index].count {
                    return Err(LatencyWindowIssue::InvalidDistribution);
                }
                if sample_index > 0 {
                    let previous = bucket.samples[sample_index - 1].count;
                    let increment = if sample.count < previous {
                        resets.insert(sample_index);
                        sample.count
                    } else {
                        sample.count - previous
                    };
                    delta = add_count(delta, increment)?;
                }
            }
            if (delta == 0.0) != (bucket.increase == 0.0) || bucket.increase + (delta * 1e-12) < delta {
                return Err(LatencyWindowIssue::InvalidDistribution);
            }
            result.cumulative_increases[index] = add_count(result.cumulative_increases[index], bucket.increase)?;
            if index == LATENCY_GRID.len() - 1 {
                result.observed_increments = add_count(result.observed_increments, delta)?;
            }
        }
        result.observed_resets = result
            .observed_resets
            .checked_add(resets.len() as u64)
            .ok_or(LatencyWindowIssue::BoundExceeded)?;
    }
    result.estimated_increments = result.cumulative_increases[LATENCY_GRID.len() - 1];
    if result.estimated_increments == 0.0 {
        result.state = LatencyWindowState::NoRecordedTraffic;
        return Ok(result);
    }
    let quantile = classic_p99(&result.cumulative_increases)?;
    result.state = if quantile.resolution == LatencyResolution::TailLimited {
        LatencyWindowState::TailLimited
    } else if result.observed_increments < 100.0 {
        LatencyWindowState::InsufficientPopulation
    } else {
        LatencyWindowState::Available
    };
    result.quantile = Some(quantile);
    Ok(result)
}

fn validate_times(
    start: i64,
    end: i64,
    width: i64,
    samples: &[LatencyCounterSample],
) -> Result<(), LatencyWindowIssue> {
    if samples.len() < 2 {
        return Err(LatencyWindowIssue::InsufficientSamples);
    }
    if samples.len() > MAX_RAW_POINTS {
        return Err(LatencyWindowIssue::BoundExceeded);
    }
    if samples
        .iter()
        .any(|sample| sample.at_millis <= start || sample.at_millis > end)
        || samples.windows(2).any(|pair| pair[1].at_millis <= pair[0].at_millis)
    {
        return Err(LatencyWindowIssue::InvalidSampleTime);
    }
    let first = samples[0].at_millis;
    let last = samples[samples.len() - 1].at_millis;
    if i128::from(last - first) * 5 < i128::from(width) * 4 {
        return Err(LatencyWindowIssue::IncompleteSpan);
    }
    if first - start > 60_000
        || end - last > 60_000
        || samples
            .windows(2)
            .any(|pair| pair[1].at_millis - pair[0].at_millis > 60_000)
    {
        return Err(LatencyWindowIssue::ScrapeGap);
    }
    Ok(())
}

fn finite_count(value: f64) -> bool {
    value.is_finite() && (0.0..=MAX_EXACT_COUNT).contains(&value)
}
fn add_count(left: f64, right: f64) -> Result<f64, LatencyWindowIssue> {
    let sum = left + right;
    if finite_count(sum) {
        Ok(sum)
    } else {
        Err(LatencyWindowIssue::InvalidDistribution)
    }
}

fn classic_p99(counts: &[f64]) -> Result<LatencyQuantile, LatencyWindowIssue> {
    let total = counts[LATENCY_GRID.len() - 1];
    let rank = 0.99 * total;
    let index = counts
        .iter()
        .position(|count| *count >= rank)
        .ok_or(LatencyWindowIssue::InvalidDistribution)?;
    if index == LATENCY_GRID.len() - 1 {
        return Ok(LatencyQuantile {
            estimate_millis: 10000.0,
            lower_bound_millis: 10000.0,
            upper_bound_millis: None,
            resolution: LatencyResolution::TailLimited,
        });
    }
    let upper = LATENCY_GRID[index].ok_or(LatencyWindowIssue::InvalidDistribution)?;
    let lower = if index == 0 {
        0.0
    } else {
        LATENCY_GRID[index - 1].ok_or(LatencyWindowIssue::InvalidDistribution)?
    };
    let previous = if index == 0 { 0.0 } else { counts[index - 1] };
    let bucket_count = counts[index] - previous;
    if bucket_count <= 0.0 {
        return Err(LatencyWindowIssue::InvalidDistribution);
    }
    let estimate = lower + (upper - lower) * ((rank - previous) / bucket_count);
    if !estimate.is_finite() || estimate < lower || estimate > upper {
        return Err(LatencyWindowIssue::InvalidDistribution);
    }
    Ok(LatencyQuantile {
        estimate_millis: estimate,
        lower_bound_millis: lower,
        upper_bound_millis: Some(upper),
        resolution: LatencyResolution::FiniteBucket,
    })
}
