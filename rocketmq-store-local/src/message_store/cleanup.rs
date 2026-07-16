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

use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

use crate::config::backend::LocalCleanupConfig;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DiskCleanDecision {
    pub should_delete: bool,
    pub clean_immediately: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskUsageState {
    Warning,
    Forcible,
    Reclaim,
    Healthy,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CleanupPolicy {
    config: LocalCleanupConfig,
}

impl CleanupPolicy {
    pub const fn new(config: LocalCleanupConfig) -> Self {
        Self { config }
    }

    pub const fn disk_warning_ratio(self) -> f64 {
        self.config.disk_warning_ratio
    }

    pub const fn disk_clean_forcibly_ratio(self) -> f64 {
        self.config.disk_clean_forcibly_ratio
    }

    pub const fn disk_max_used_ratio(self) -> f64 {
        self.config.disk_max_used_ratio
    }

    pub const fn clean_file_forcibly_enabled(self) -> bool {
        self.config.clean_file_forcibly_enabled
    }

    pub fn classify(self, ratio: f64) -> DiskUsageState {
        if ratio > self.disk_warning_ratio() {
            DiskUsageState::Warning
        } else if ratio > self.disk_clean_forcibly_ratio() {
            DiskUsageState::Forcible
        } else if ratio < 0.0 || ratio > self.disk_max_used_ratio() {
            DiskUsageState::Reclaim
        } else {
            DiskUsageState::Healthy
        }
    }

    pub fn decide(self, physic_ratio: f64, logic_ratio: f64) -> DiskCleanDecision {
        let physic = self.classify(physic_ratio);
        let logic = self.classify(logic_ratio);
        if matches!(physic, DiskUsageState::Warning | DiskUsageState::Forcible)
            || matches!(logic, DiskUsageState::Warning | DiskUsageState::Forcible)
        {
            return DiskCleanDecision {
                should_delete: true,
                clean_immediately: true,
            };
        }

        if physic == DiskUsageState::Reclaim || logic == DiskUsageState::Reclaim {
            return DiskCleanDecision {
                should_delete: true,
                clean_immediately: false,
            };
        }

        DiskCleanDecision::default()
    }
}

pub struct ManualDeleteTracker {
    requests: AtomicI32,
    retry_budget: i32,
}

impl ManualDeleteTracker {
    pub const fn new(retry_budget: i32) -> Self {
        Self {
            requests: AtomicI32::new(0),
            retry_budget,
        }
    }

    pub fn request(&self) {
        self.requests.store(self.retry_budget, Ordering::SeqCst);
    }

    pub fn consume(&self) -> bool {
        self.requests
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |requests| {
                (requests > 0).then_some(requests - 1)
            })
            .is_ok()
    }

    pub fn remaining(&self) -> i32 {
        self.requests.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> CleanupPolicy {
        CleanupPolicy::new(LocalCleanupConfig {
            disk_warning_ratio: 0.9,
            disk_clean_forcibly_ratio: 0.85,
            disk_max_used_ratio: 0.75,
            clean_file_forcibly_enabled: true,
        })
    }

    #[test]
    fn cleanup_decision_preserves_java_threshold_ordering() {
        assert_eq!(policy().classify(0.91), DiskUsageState::Warning);
        assert_eq!(policy().classify(0.86), DiskUsageState::Forcible);
        assert_eq!(policy().classify(0.8), DiskUsageState::Reclaim);
        assert_eq!(policy().classify(0.5), DiskUsageState::Healthy);
        assert_eq!(
            policy().decide(0.91, 0.1),
            DiskCleanDecision {
                should_delete: true,
                clean_immediately: true,
            }
        );
        assert_eq!(
            policy().decide(0.8, 0.1),
            DiskCleanDecision {
                should_delete: true,
                clean_immediately: false,
            }
        );
        assert_eq!(policy().decide(0.5, 0.5), DiskCleanDecision::default());
    }

    #[test]
    fn manual_delete_tracker_consumes_a_bounded_retry_budget() {
        let tracker = ManualDeleteTracker::new(2);
        tracker.request();
        assert_eq!(tracker.remaining(), 2);
        assert!(tracker.consume());
        assert!(tracker.consume());
        assert!(!tracker.consume());
    }
}
