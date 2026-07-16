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

use rocketmq_store_local::mapped_file::queue_state::MappedFileQueueRuntimeState;

#[test]
fn queue_runtime_state_preserves_initial_values_and_signed_offset_round_trips() {
    let state = MappedFileQueueRuntimeState::default();
    assert_eq!(state.flushed_where(), 0);
    assert_eq!(state.committed_where(), 0);
    assert_eq!(state.store_timestamp(), 0);

    state.set_flushed_where(-1);
    state.set_committed_where(41);
    state.set_store_timestamp(43);

    assert_eq!(state.flushed_where(), -1);
    assert_eq!(state.committed_where(), 41);
    assert_eq!(state.store_timestamp(), 43);
}

#[test]
fn queue_runtime_state_commit_lock_serializes_access() {
    let state = MappedFileQueueRuntimeState::default();
    let guard = state.commit_lock().lock();
    assert!(state.commit_lock().try_lock().is_none());
    drop(guard);
    assert!(state.commit_lock().try_lock().is_some());
}
