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

use rocketmq_store::consume_queue::mapped_file_queue::FlushProgress as LegacyFlushProgress;
use rocketmq_store_local::flush::FlushProgress as CanonicalFlushProgress;

fn accept_canonical(_progress: CanonicalFlushProgress) {}

#[test]
fn legacy_flush_progress_is_the_canonical_local_type() {
    let progress = LegacyFlushProgress {
        appended: 128,
        durable_before: 64,
        durable: 96,
        store_timestamp: 7,
    };

    accept_canonical(progress);
}
