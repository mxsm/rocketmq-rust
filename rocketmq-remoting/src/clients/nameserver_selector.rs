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

//! Smart NameServer selection based on latency and health metrics.
//!
//! This module provides intelligent nameserver selection to replace simple round-robin,
//! improving performance by routing to the fastest and healthiest nameservers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use parking_lot::RwLock;

/// Tracks latency metrics for nameservers to enable smart selection.
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │         LatencyTracker                  │
/// ├─────────────────────────────────────────┤
/// │  addr -> LatencyMetrics                 │
/// │  ├─ recent_latencies: VecDeque<u64>    │
/// │  ├─ p50: Duration                       │
/// │  ├─ p99: Duration                       │
/// │  └─ error_count: u32                    │
/// └─────────────────────────────────────────┘
/// ```
///
/// # Performance
///
/// - **Lock**: RwLock for read-heavy workload (selection > recording)
/// - **Window**: Rolling window of last 100 samples per nameserver
/// - **Update**: O(1) amortized (ring buffer)
/// - **Select**: O(N) where N = nameserver count (typically <10)
#[derive(Clone)]
pub struct LatencyTracker {
    metrics: Arc<RwLock<HashMap<CheetahString, LatencyMetrics>>>,
}

/// Per-nameserver latency and health metrics.
#[derive(Debug, Clone)]
struct LatencyMetrics {
    /// Rolling window of recent request latencies (microseconds)
    recent_latencies: Vec<u64>,

    /// Maximum samples to keep (older samples dropped)
    max_samples: usize,

    /// P50 latency (median)
    p50: Duration,

    /// P99 latency (99th percentile)
    p99: Duration,

    /// Consecutive error count (resets on success)
    consecutive_errors: u32,

    /// Total request count
    total_requests: u64,

    /// Last update timestamp
    last_update: Instant,
}

impl LatencyMetrics {
    fn new(max_samples: usize) -> Self {
        Self {
            recent_latencies: Vec::with_capacity(max_samples),
            max_samples,
            p50: Duration::from_millis(0),
            p99: Duration::from_millis(0),
            consecutive_errors: 0,
            total_requests: 0,
            last_update: Instant::now(),
        }
    }

    /// Records a successful request latency.
    fn record_success(&mut self, latency: Duration) {
        // Add new sample
        self.recent_latencies.push(latency.as_micros() as u64);

        // Keep window size bounded
        if self.recent_latencies.len() > self.max_samples {
            self.recent_latencies.remove(0);
        }

        // Recalculate percentiles
        self.update_percentiles();

        // Reset error counter on success
        self.consecutive_errors = 0;
        self.total_requests += 1;
        self.last_update = Instant::now();
    }

    /// Records a failed request.
    fn record_error(&mut self) {
        self.consecutive_errors += 1;
        self.total_requests += 1;
        self.last_update = Instant::now();
    }

    /// Updates P50 and P99 based on current samples.
    fn update_percentiles(&mut self) {
        if self.recent_latencies.is_empty() {
            return;
        }

        // Sort for percentile calculation
        let mut sorted = self.recent_latencies.clone();
        sorted.sort_unstable();

        let len = sorted.len();

        // P50 (median)
        let p50_idx = len / 2;
        self.p50 = Duration::from_micros(sorted[p50_idx]);

        // P99
        let p99_idx = (len as f64 * 0.99).ceil() as usize - 1;
        self.p99 = Duration::from_micros(sorted[p99_idx.min(len - 1)]);
    }

    /// Calculates a score for nameserver selection (lower is better).
    ///
    /// # Scoring Formula
    ///
    /// ```text
    /// score = p99_latency_ms + (consecutive_errors * 100)
    /// ```
    ///
    /// This heavily penalizes failing nameservers while preferring low-latency ones.
    fn score(&self) -> u64 {
        let latency_penalty = self.p99.as_millis() as u64;
        let error_penalty = self.consecutive_errors as u64 * 100;
        latency_penalty + error_penalty
    }

    /// Checks if this nameserver is considered healthy.
    fn is_healthy(&self) -> bool {
        // Consider unhealthy if >3 consecutive errors
        self.consecutive_errors < 3
    }
}

impl LatencyTracker {
    /// Creates a new latency tracker.
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Records a successful request to a nameserver.
    ///
    /// # Arguments
    ///
    /// * `addr` - Nameserver address
    /// * `latency` - Request latency
    pub fn record_success(&self, addr: &CheetahString, latency: Duration) {
        let mut metrics = self.metrics.write();
        metrics
            .entry(addr.clone())
            .or_insert_with(|| LatencyMetrics::new(100))
            .record_success(latency);
    }

    /// Records a failed request to a nameserver.
    ///
    /// # Arguments
    ///
    /// * `addr` - Nameserver address
    pub fn record_error(&self, addr: &CheetahString) {
        let mut metrics = self.metrics.write();
        metrics
            .entry(addr.clone())
            .or_insert_with(|| LatencyMetrics::new(100))
            .record_error();
    }

    /// Selects the best nameserver from available candidates.
    ///
    /// # Selection Algorithm
    ///
    /// 1. Filter out unhealthy nameservers (>3 consecutive errors)
    /// 2. Sort by score (P99 latency + error penalty)
    /// 3. Return lowest-score (best) nameserver
    /// 4. Fall back to first candidate if no metrics available
    ///
    /// # Arguments
    ///
    /// * `candidates` - Available nameserver addresses
    ///
    /// # Returns
    ///
    /// Best nameserver address, or `None` if no healthy candidates
    pub fn select_best<'a>(&self, candidates: &'a [CheetahString]) -> Option<&'a CheetahString> {
        if candidates.is_empty() {
            return None;
        }

        let metrics = self.metrics.read();

        // Find best candidate with metrics
        let mut best: Option<(&CheetahString, u64)> = None;

        for addr in candidates {
            if let Some(m) = metrics.get(addr) {
                if !m.is_healthy() {
                    continue; // Skip unhealthy nameservers
                }

                let score = m.score();
                match best {
                    None => best = Some((addr, score)),
                    Some((_, best_score)) if score < best_score => {
                        best = Some((addr, score));
                    }
                    _ => {}
                }
            }
        }

        // Return best with metrics, or first candidate as fallback
        best.map(|(addr, _)| addr).or_else(|| candidates.first())
    }

    /// Gets P99 latency for a nameserver.
    ///
    /// # Returns
    ///
    /// P99 latency, or `None` if no metrics available
    pub fn get_p99(&self, addr: &CheetahString) -> Option<Duration> {
        let metrics = self.metrics.read();
        metrics.get(addr).map(|m| m.p99)
    }

    /// Gets consecutive error count for a nameserver.
    pub fn get_error_count(&self, addr: &CheetahString) -> u32 {
        let metrics = self.metrics.read();
        metrics.get(addr).map(|m| m.consecutive_errors).unwrap_or(0)
    }

    /// Checks if a nameserver is considered healthy.
    pub fn is_healthy(&self, addr: &CheetahString) -> bool {
        let metrics = self.metrics.read();
        metrics.get(addr).map(|m| m.is_healthy()).unwrap_or(true) // Unknown = healthy
    }

    /// Clears metrics for a nameserver (e.g., when removed from config).
    pub fn clear(&self, addr: &CheetahString) {
        let mut metrics = self.metrics.write();
        metrics.remove(addr);
    }

    /// Gets a snapshot of all metrics for debugging/monitoring.
    pub fn snapshot(&self) -> HashMap<CheetahString, (Duration, Duration, u32)> {
        let metrics = self.metrics.read();
        metrics
            .iter()
            .map(|(addr, m)| (addr.clone(), (m.p50, m.p99, m.consecutive_errors)))
            .collect()
    }
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_tracker_basic() {
        let tracker = LatencyTracker::new();
        let addr = CheetahString::from_static_str("127.0.0.1:9876");

        // Record some latencies
        tracker.record_success(&addr, Duration::from_millis(10));
        tracker.record_success(&addr, Duration::from_millis(15));
        tracker.record_success(&addr, Duration::from_millis(20));

        // Should have metrics now
        assert!(tracker.get_p99(&addr).is_some());
        assert!(tracker.is_healthy(&addr));
    }

    #[test]
    fn test_latency_tracker_error_handling() {
        let tracker = LatencyTracker::new();
        let addr = CheetahString::from_static_str("127.0.0.1:9876");

        // Record multiple errors
        tracker.record_error(&addr);
        tracker.record_error(&addr);
        assert!(tracker.is_healthy(&addr)); // Still healthy (<3 errors)

        tracker.record_error(&addr);
        assert!(!tracker.is_healthy(&addr)); // Now unhealthy (>=3 errors)

        // Success resets error counter
        tracker.record_success(&addr, Duration::from_millis(10));
        assert!(tracker.is_healthy(&addr));
    }

    #[test]
    fn test_nameserver_selection() {
        let tracker = LatencyTracker::new();

        let addr1 = CheetahString::from_static_str("127.0.0.1:9876");
        let addr2 = CheetahString::from_static_str("127.0.0.1:9877");
        let addr3 = CheetahString::from_static_str("127.0.0.1:9878");

        // addr1: Low latency, healthy
        tracker.record_success(&addr1, Duration::from_millis(5));
        tracker.record_success(&addr1, Duration::from_millis(6));

        // addr2: High latency, healthy
        tracker.record_success(&addr2, Duration::from_millis(50));
        tracker.record_success(&addr2, Duration::from_millis(60));

        // addr3: Low latency, but has errors
        tracker.record_success(&addr3, Duration::from_millis(10));
        tracker.record_error(&addr3);
        tracker.record_error(&addr3);
        tracker.record_error(&addr3); // Now unhealthy

        let candidates = vec![addr1.clone(), addr2.clone(), addr3.clone()];
        let best = tracker.select_best(&candidates);

        // Should select addr1 (lowest latency + healthy)
        assert_eq!(best, Some(&addr1));
    }
}
