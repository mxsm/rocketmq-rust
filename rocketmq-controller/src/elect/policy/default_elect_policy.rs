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

//! Default election policy implementation for RocketMQ controller.
//!
//! This module implements the default master election strategy, which attempts to:
//! 1. Prioritize replicas in sync state set
//! 2. Validate broker liveness
//! 3. Prefer the old master if still valid
//! 4. Honor preferred broker if specified
//! 5. Select the best candidate based on (epoch, offset, priority)

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use crate::elect::elect_policy::ElectPolicy;
use crate::heartbeat::broker_live_info::BrokerLiveInfo;
use crate::helper::broker_live_info_getter::BrokerLiveInfoGetter;
use crate::helper::broker_valid_predicate::BrokerValidPredicate;

/// Default implementation of the election policy.
///
/// This policy performs a two-stage election:
/// 1. First attempts to elect from sync state brokers (fully synchronized replicas)
/// 2. Falls back to all replica brokers if no suitable sync state broker is found
///
/// The selection process:
/// - Filters brokers by liveness using `BrokerValidPredicate`
/// - Prefers the old master if it's still valid
/// - Respects preferred broker ID if provided
/// - Sorts remaining candidates by (epoch DESC, offset DESC, priority ASC)
/// - Returns the best candidate or falls back to any alive broker
pub struct DefaultElectPolicy {
    /// Predicate to check if a broker is valid/alive
    valid_predicate: Option<Arc<dyn BrokerValidPredicate>>,

    /// Getter to retrieve live broker information
    broker_live_info_getter: Option<Arc<dyn BrokerLiveInfoGetter>>,
}

impl DefaultElectPolicy {
    /// Creates a new `DefaultElectPolicy` with provided dependencies.
    ///
    /// # Arguments
    /// * `valid_predicate` - Optional predicate to validate broker liveness
    /// * `broker_live_info_getter` - Optional getter to retrieve broker live information
    ///
    /// # Returns
    /// A new instance of `DefaultElectPolicy`
    pub fn new(
        valid_predicate: Option<Arc<dyn BrokerValidPredicate>>,
        broker_live_info_getter: Option<Arc<dyn BrokerLiveInfoGetter>>,
    ) -> Self {
        Self {
            valid_predicate,
            broker_live_info_getter,
        }
    }

    /// Creates a default instance with no predicate or info getter.
    pub fn default_instance() -> Self {
        Self {
            valid_predicate: None,
            broker_live_info_getter: None,
        }
    }

    /// Sets the broker valid predicate.
    pub fn set_valid_predicate(&mut self, predicate: Arc<dyn BrokerValidPredicate>) {
        self.valid_predicate = Some(predicate);
    }

    /// Sets the broker live info getter.
    pub fn set_broker_live_info_getter(&mut self, getter: Arc<dyn BrokerLiveInfoGetter>) {
        self.broker_live_info_getter = Some(getter);
    }

    /// Gets a reference to the broker live info getter.
    pub fn broker_live_info_getter(&self) -> Option<&Arc<dyn BrokerLiveInfoGetter>> {
        self.broker_live_info_getter.as_ref()
    }

    /// Attempts to elect a master from the given set of brokers.
    ///
    /// # Arguments
    /// * `cluster_name` - The cluster name
    /// * `broker_name` - The broker group name
    /// * `brokers` - Set of candidate broker IDs
    /// * `old_master` - Previous master broker ID
    /// * `prefer_broker_id` - Preferred broker ID to be elected
    ///
    /// # Returns
    /// The elected broker ID, or None if no valid candidate exists
    fn try_elect(
        &self,
        cluster_name: &str,
        broker_name: &str,
        brokers: &HashSet<i64>,
        old_master: Option<i64>,
        prefer_broker_id: Option<i64>,
    ) -> Option<i64> {
        // Filter brokers by validity predicate
        let mut valid_brokers = brokers.clone();
        if let Some(ref predicate) = self.valid_predicate {
            valid_brokers.retain(|&broker_id| predicate.check(cluster_name, broker_name, Some(broker_id)));
        }

        if valid_brokers.is_empty() {
            return None;
        }

        // If old master is still valid and (no preference OR preference equals old master)
        if let Some(old_master_id) = old_master {
            if valid_brokers.contains(&old_master_id)
                && (prefer_broker_id.is_none() || prefer_broker_id == Some(old_master_id))
            {
                return Some(old_master_id);
            }
        }

        // If a preferred broker is specified, check if it's valid
        if let Some(preferred_id) = prefer_broker_id {
            return if valid_brokers.contains(&preferred_id) {
                Some(preferred_id)
            } else {
                // Preferred broker is not valid, election fails
                None
            };
        }

        // Sort brokers by (epoch, max_offset, election_priority) if live info is available
        if let Some(ref getter) = self.broker_live_info_getter {
            let mut broker_infos: Vec<BrokerLiveInfo> = valid_brokers
                .iter()
                .map(|&broker_id| getter.get(cluster_name, broker_name, broker_id))
                .collect();

            // Sort in descending order by epoch, then offset, then ascending by priority
            broker_infos.sort_by(compare_broker_info);

            if let Some(best) = broker_infos.first() {
                return Some(best.broker_id());
            }
        }

        // Fallback: elect any available broker (deterministic by using iter)
        valid_brokers.iter().next().copied()
    }
}

/// Compares two BrokerLiveInfo instances for election sorting.
///
/// Sorting rules (matching Java implementation):
/// 1. Higher epoch is better (descending)
/// 2. If epochs equal, higher max_offset is better (descending)
/// 3. If offsets equal, lower election_priority is better (ascending)
///
/// # Arguments
/// * `a` - First broker info
/// * `b` - Second broker info
///
/// # Returns
/// Ordering for sorting
fn compare_broker_info(a: &BrokerLiveInfo, b: &BrokerLiveInfo) -> Ordering {
    // Compare epoch (descending - higher epoch first)
    match b.epoch().cmp(&a.epoch()) {
        Ordering::Equal => {
            // If epochs are equal, compare max_offset (descending - higher offset first)
            match b.max_offset().cmp(&a.max_offset()) {
                Ordering::Equal => {
                    // If offsets are equal, compare priority (ascending - lower priority first)
                    let a_priority = a.election_priority().unwrap_or(i32::MAX);
                    let b_priority = b.election_priority().unwrap_or(i32::MAX);
                    a_priority.cmp(&b_priority)
                }
                other => other,
            }
        }
        other => other,
    }
}

impl ElectPolicy for DefaultElectPolicy {
    /// Elects a new master broker from available replicas.
    ///
    /// The election process:
    /// 1. First tries to elect from sync_state_brokers (fully synchronized replicas)
    /// 2. If that fails, falls back to all_replica_brokers
    /// 3. Returns None if no valid candidate is found in either set
    ///
    /// # Arguments
    /// * `cluster_name` - The cluster name
    /// * `broker_name` - The broker group name
    /// * `sync_state_brokers` - Brokers in the sync state set (fully synchronized)
    /// * `all_replica_brokers` - All available replica brokers
    /// * `old_master` - The previous master broker ID
    /// * `broker_id` - Preferred broker ID for election
    ///
    /// # Returns
    /// The elected broker ID, or None if election fails
    fn elect(
        &self,
        cluster_name: &str,
        broker_name: &str,
        sync_state_brokers: &HashSet<i64>,
        all_replica_brokers: &HashSet<i64>,
        old_master: Option<i64>,
        broker_id: Option<i64>,
    ) -> Option<i64> {
        // Try to elect from sync state brokers first
        // Note: Always call try_elect even if empty, to match Java behavior
        // where null check differs from emptiness check
        if let Some(master) = self.try_elect(cluster_name, broker_name, sync_state_brokers, old_master, broker_id) {
            return Some(master);
        }

        // Fallback to all replica brokers
        self.try_elect(cluster_name, broker_name, all_replica_brokers, old_master, broker_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::*;
    use crate::helper::broker_valid_predicate::BrokerValidPredicate;

    // Mock implementations for testing

    struct MockValidPredicate {
        valid_brokers: HashSet<i64>,
    }

    impl BrokerValidPredicate for MockValidPredicate {
        fn check(&self, _cluster_name: &str, _broker_name: &str, broker_id: Option<i64>) -> bool {
            broker_id.is_some_and(|id| self.valid_brokers.contains(&id))
        }
    }

    #[test]
    fn test_elect_with_no_brokers() {
        let policy = DefaultElectPolicy::default_instance();
        let sync_brokers = HashSet::new();
        let all_brokers = HashSet::new();

        let result = policy.elect("test-cluster", "test-broker", &sync_brokers, &all_brokers, None, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_elect_old_master_still_valid() {
        let mut valid_brokers = HashSet::new();
        valid_brokers.insert(1);
        valid_brokers.insert(2);
        valid_brokers.insert(3);

        let predicate = Arc::new(MockValidPredicate {
            valid_brokers: valid_brokers.clone(),
        });

        let policy = DefaultElectPolicy::new(Some(predicate), None);

        let result = policy.elect(
            "test-cluster",
            "test-broker",
            &valid_brokers,
            &HashSet::new(),
            Some(2),
            None,
        );

        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_elect_with_preferred_broker_valid() {
        let mut valid_brokers = HashSet::new();
        valid_brokers.insert(1);
        valid_brokers.insert(2);
        valid_brokers.insert(3);

        let predicate = Arc::new(MockValidPredicate {
            valid_brokers: valid_brokers.clone(),
        });

        let policy = DefaultElectPolicy::new(Some(predicate), None);

        let result = policy.elect(
            "test-cluster",
            "test-broker",
            &valid_brokers,
            &HashSet::new(),
            Some(1),
            Some(3),
        );

        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_elect_with_preferred_broker_invalid() {
        let mut valid_brokers = HashSet::new();
        valid_brokers.insert(1);
        valid_brokers.insert(2);

        let predicate = Arc::new(MockValidPredicate {
            valid_brokers: valid_brokers.clone(),
        });

        let policy = DefaultElectPolicy::new(Some(predicate), None);

        let result = policy.elect(
            "test-cluster",
            "test-broker",
            &valid_brokers,
            &HashSet::new(),
            Some(1),
            Some(99), // Invalid preferred broker
        );

        assert!(result.is_none());
    }

    #[test]
    fn test_elect_fallback_to_all_replicas() {
        let sync_brokers = HashSet::new(); // Empty sync brokers

        let mut all_brokers = HashSet::new();
        all_brokers.insert(1);
        all_brokers.insert(2);

        let mut valid_brokers = HashSet::new();
        valid_brokers.insert(1);
        valid_brokers.insert(2);

        let predicate = Arc::new(MockValidPredicate { valid_brokers });

        let policy = DefaultElectPolicy::new(Some(predicate), None);

        let result = policy.elect("test-cluster", "test-broker", &sync_brokers, &all_brokers, None, None);

        // Should fallback and elect some broker (either 1 or 2)
        assert!(result.is_some());
        assert!(result == Some(1) || result == Some(2));
    }

    #[test]
    fn test_elect_all_brokers_invalid() {
        let mut brokers = HashSet::new();
        brokers.insert(1);
        brokers.insert(2);
        brokers.insert(3);

        let valid_brokers = HashSet::new(); // No valid brokers

        let predicate = Arc::new(MockValidPredicate { valid_brokers });

        let policy = DefaultElectPolicy::new(Some(predicate), None);

        let result = policy.elect("test-cluster", "test-broker", &brokers, &HashSet::new(), None, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_elect_without_predicate() {
        let mut brokers = HashSet::new();
        brokers.insert(1);
        brokers.insert(2);

        let policy = DefaultElectPolicy::default_instance();

        let result = policy.elect("test-cluster", "test-broker", &brokers, &HashSet::new(), None, None);

        assert!(result.is_some()); // Should elect some broker
    }

    #[test]
    fn test_old_master_priority_over_others_without_preference() {
        let mut valid_brokers = HashSet::new();
        valid_brokers.insert(1);
        valid_brokers.insert(2);
        valid_brokers.insert(3);

        let predicate = Arc::new(MockValidPredicate {
            valid_brokers: valid_brokers.clone(),
        });

        let policy = DefaultElectPolicy::new(Some(predicate), None);

        // Old master should be elected when valid and no preference given
        let result = policy.elect(
            "test-cluster",
            "test-broker",
            &valid_brokers,
            &HashSet::new(),
            Some(2),
            None,
        );

        assert_eq!(result, Some(2));
    }

    #[test]
    fn test_preferred_broker_overrides_old_master() {
        let mut valid_brokers = HashSet::new();
        valid_brokers.insert(1);
        valid_brokers.insert(2);
        valid_brokers.insert(3);

        let predicate = Arc::new(MockValidPredicate {
            valid_brokers: valid_brokers.clone(),
        });

        let policy = DefaultElectPolicy::new(Some(predicate), None);

        // Preferred broker should override old master
        let result = policy.elect(
            "test-cluster",
            "test-broker",
            &valid_brokers,
            &HashSet::new(),
            Some(2), // old master
            Some(3), // preferred broker
        );

        assert_eq!(result, Some(3));
    }

    #[test]
    fn test_sync_brokers_priority_over_all_brokers() {
        let mut sync_brokers = HashSet::new();
        sync_brokers.insert(1);

        let mut all_brokers = HashSet::new();
        all_brokers.insert(1);
        all_brokers.insert(2);
        all_brokers.insert(3);

        let mut valid_brokers = HashSet::new();
        valid_brokers.insert(1);
        valid_brokers.insert(2);
        valid_brokers.insert(3);

        let predicate = Arc::new(MockValidPredicate { valid_brokers });

        let policy = DefaultElectPolicy::new(Some(predicate), None);

        // Should elect from sync_brokers first
        let result = policy.elect("test-cluster", "test-broker", &sync_brokers, &all_brokers, None, None);

        assert_eq!(result, Some(1));
    }
}
