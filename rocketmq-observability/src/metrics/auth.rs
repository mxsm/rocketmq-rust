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
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct AuthMetrics {
    inner: Arc<AuthMetricsInner>,
}

#[derive(Default)]
struct AuthMetricsInner {
    acl_reload_attempts: AtomicU64,
    acl_reload_successes: AtomicU64,
    acl_reload_failures: AtomicU64,
    acl_reload_skipped: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_invalidations: AtomicU64,
    signature_successes: AtomicU64,
    signature_failures: AtomicU64,
    whitelist_hits: AtomicU64,
    whitelist_misses: AtomicU64,
    authentication_successes: AtomicU64,
    authentication_failures: AtomicU64,
    authorization_successes: AtomicU64,
    authorization_failures: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuthMetricsSnapshot {
    pub acl_reload_attempts: u64,
    pub acl_reload_successes: u64,
    pub acl_reload_failures: u64,
    pub acl_reload_skipped: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_invalidations: u64,
    pub signature_successes: u64,
    pub signature_failures: u64,
    pub whitelist_hits: u64,
    pub whitelist_misses: u64,
    pub authentication_successes: u64,
    pub authentication_failures: u64,
    pub authorization_successes: u64,
    pub authorization_failures: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthMetricSample {
    pub name: &'static str,
    pub value: u64,
}

impl AuthMetricsSnapshot {
    pub const SAMPLE_COUNT: usize = 15;

    pub fn samples(&self) -> [AuthMetricSample; Self::SAMPLE_COUNT] {
        [
            AuthMetricSample {
                name: "acl_reload_attempts",
                value: self.acl_reload_attempts,
            },
            AuthMetricSample {
                name: "acl_reload_successes",
                value: self.acl_reload_successes,
            },
            AuthMetricSample {
                name: "acl_reload_failures",
                value: self.acl_reload_failures,
            },
            AuthMetricSample {
                name: "acl_reload_skipped",
                value: self.acl_reload_skipped,
            },
            AuthMetricSample {
                name: "cache_hits",
                value: self.cache_hits,
            },
            AuthMetricSample {
                name: "cache_misses",
                value: self.cache_misses,
            },
            AuthMetricSample {
                name: "cache_invalidations",
                value: self.cache_invalidations,
            },
            AuthMetricSample {
                name: "signature_successes",
                value: self.signature_successes,
            },
            AuthMetricSample {
                name: "signature_failures",
                value: self.signature_failures,
            },
            AuthMetricSample {
                name: "whitelist_hits",
                value: self.whitelist_hits,
            },
            AuthMetricSample {
                name: "whitelist_misses",
                value: self.whitelist_misses,
            },
            AuthMetricSample {
                name: "authentication_successes",
                value: self.authentication_successes,
            },
            AuthMetricSample {
                name: "authentication_failures",
                value: self.authentication_failures,
            },
            AuthMetricSample {
                name: "authorization_successes",
                value: self.authorization_successes,
            },
            AuthMetricSample {
                name: "authorization_failures",
                value: self.authorization_failures,
            },
        ]
    }
}

impl AuthMetrics {
    pub fn snapshot(&self) -> AuthMetricsSnapshot {
        AuthMetricsSnapshot {
            acl_reload_attempts: self.inner.acl_reload_attempts.load(Ordering::Relaxed),
            acl_reload_successes: self.inner.acl_reload_successes.load(Ordering::Relaxed),
            acl_reload_failures: self.inner.acl_reload_failures.load(Ordering::Relaxed),
            acl_reload_skipped: self.inner.acl_reload_skipped.load(Ordering::Relaxed),
            cache_hits: self.inner.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.inner.cache_misses.load(Ordering::Relaxed),
            cache_invalidations: self.inner.cache_invalidations.load(Ordering::Relaxed),
            signature_successes: self.inner.signature_successes.load(Ordering::Relaxed),
            signature_failures: self.inner.signature_failures.load(Ordering::Relaxed),
            whitelist_hits: self.inner.whitelist_hits.load(Ordering::Relaxed),
            whitelist_misses: self.inner.whitelist_misses.load(Ordering::Relaxed),
            authentication_successes: self.inner.authentication_successes.load(Ordering::Relaxed),
            authentication_failures: self.inner.authentication_failures.load(Ordering::Relaxed),
            authorization_successes: self.inner.authorization_successes.load(Ordering::Relaxed),
            authorization_failures: self.inner.authorization_failures.load(Ordering::Relaxed),
        }
    }

    #[inline]
    pub fn record_acl_reload_attempt(&self) {
        self.inner.acl_reload_attempts.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_acl_reload_success(&self) {
        self.inner.acl_reload_successes.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_acl_reload_failure(&self) {
        self.inner.acl_reload_failures.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_acl_reload_skipped(&self) {
        self.inner.acl_reload_skipped.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_cache_hit(&self) {
        self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_cache_miss(&self) {
        self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_cache_invalidation(&self) {
        self.inner.cache_invalidations.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_signature_verification(&self, success: bool) {
        if success {
            self.inner.signature_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.signature_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_whitelist_check(&self, matched: bool) {
        if matched {
            self.inner.whitelist_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.whitelist_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_authentication_result(&self, success: bool) {
        if success {
            self.inner.authentication_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.authentication_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_authorization_result(&self, success: bool) {
        if success {
            self.inner.authorization_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.authorization_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AuthMetrics;
    use super::AuthMetricsSnapshot;

    #[test]
    fn auth_metrics_snapshot_tracks_all_counter_groups() {
        let metrics = AuthMetrics::default();

        metrics.record_acl_reload_attempt();
        metrics.record_acl_reload_success();
        metrics.record_acl_reload_failure();
        metrics.record_acl_reload_skipped();
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        metrics.record_cache_invalidation();
        metrics.record_signature_verification(true);
        metrics.record_signature_verification(false);
        metrics.record_whitelist_check(true);
        metrics.record_whitelist_check(false);
        metrics.record_authentication_result(true);
        metrics.record_authentication_result(false);
        metrics.record_authorization_result(true);
        metrics.record_authorization_result(false);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.acl_reload_attempts, 1);
        assert_eq!(snapshot.acl_reload_successes, 1);
        assert_eq!(snapshot.acl_reload_failures, 1);
        assert_eq!(snapshot.acl_reload_skipped, 1);
        assert_eq!(snapshot.cache_hits, 1);
        assert_eq!(snapshot.cache_misses, 1);
        assert_eq!(snapshot.cache_invalidations, 1);
        assert_eq!(snapshot.signature_successes, 1);
        assert_eq!(snapshot.signature_failures, 1);
        assert_eq!(snapshot.whitelist_hits, 1);
        assert_eq!(snapshot.whitelist_misses, 1);
        assert_eq!(snapshot.authentication_successes, 1);
        assert_eq!(snapshot.authentication_failures, 1);
        assert_eq!(snapshot.authorization_successes, 1);
        assert_eq!(snapshot.authorization_failures, 1);
    }

    #[test]
    fn auth_metrics_snapshot_exports_stable_samples() {
        let snapshot = AuthMetricsSnapshot {
            acl_reload_attempts: 1,
            authorization_failures: 15,
            ..AuthMetricsSnapshot::default()
        };

        let samples = snapshot.samples();

        assert_eq!(samples.len(), AuthMetricsSnapshot::SAMPLE_COUNT);
        assert_eq!(samples[0].name, "acl_reload_attempts");
        assert_eq!(samples[0].value, 1);
        assert_eq!(samples[14].name, "authorization_failures");
        assert_eq!(samples[14].value, 15);
    }
}
