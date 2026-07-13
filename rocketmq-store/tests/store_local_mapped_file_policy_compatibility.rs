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

use rocketmq_store::log_file::mapped_file::default_mapped_file_impl::OS_PAGE_SIZE as LEGACY_OS_PAGE_SIZE;
use rocketmq_store_local::mapped_file::kernel::OS_PAGE_SIZE as CANONICAL_OS_PAGE_SIZE;

const _: [(); CANONICAL_OS_PAGE_SIZE as usize] = [(); LEGACY_OS_PAGE_SIZE as usize];

#[test]
fn legacy_page_size_path_preserves_the_canonical_fixed_value() {
    assert_eq!(LEGACY_OS_PAGE_SIZE, CANONICAL_OS_PAGE_SIZE);
    assert_eq!(LEGACY_OS_PAGE_SIZE, 1024 * 4);
}
