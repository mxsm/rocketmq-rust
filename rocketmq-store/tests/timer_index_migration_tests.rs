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

use rocketmq_store::test_support::run_timer_index_migration_probe;

#[test]
fn migration_resumes_without_changing_owner_and_supports_rollback() {
    let probe = run_timer_index_migration_probe();
    assert!(probe.interrupted_owner_is_rocks);
    assert!(probe.resumed_from_checkpoint);
    assert_eq!(probe.compared_records, 13);
    assert!(probe.bulk_overlay_complete);
    assert!(probe.cutover_owner_is_segmented);
    assert!(probe.rollback_standby_received_increment);
    assert!(probe.rollback_restored_rocks);
}
