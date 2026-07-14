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

/// Returns whether recovery must truncate ConsumeQueue data at the supplied CommitLog offset.
#[inline]
pub fn should_truncate_recovery_consume_queue(max_phy_offset: i64, truncate_offset: u64) -> bool {
    max_phy_offset < 0 || u64::try_from(max_phy_offset).is_ok_and(|value| value >= truncate_offset)
}
