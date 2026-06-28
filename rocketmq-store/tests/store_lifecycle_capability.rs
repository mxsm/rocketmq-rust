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

use rocketmq_store::platform::classify_file_preallocate_result;
use rocketmq_store::platform::current_store_platform_capability;
use rocketmq_store::platform::FilePreallocateOutcome;
use rocketmq_store::platform::PREALLOCATE_UNSUPPORTED_ERRNO;

#[test]
fn platform_capability_snapshot_reports_page_size_and_lock_limit() {
    let capability = current_store_platform_capability();

    assert!(!capability.os_name.is_empty());
    assert!(capability.page_size > 0);
    assert!(capability.memory_lock_limit_bytes.is_some());
}

#[test]
fn unsupported_preallocation_errno_is_reported_as_degraded() {
    let outcome = classify_file_preallocate_result(-1, PREALLOCATE_UNSUPPORTED_ERRNO);

    assert_eq!(
        outcome,
        FilePreallocateOutcome::Unsupported {
            errno: PREALLOCATE_UNSUPPORTED_ERRNO
        }
    );
    assert!(outcome.is_degraded());
}
