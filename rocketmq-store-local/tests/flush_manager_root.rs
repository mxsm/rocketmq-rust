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

use rocketmq_store_local::flush::root::FlushManagerRoot;

#[test]
fn flush_manager_root_preserves_exclusive_adapter_identity() {
    let root = FlushManagerRoot::new(String::from("flush-adapter"));

    assert_eq!(root.adapter(), "flush-adapter");
    assert_eq!(root.into_adapter(), "flush-adapter");
}

#[test]
fn flush_manager_root_exposes_one_mutable_adapter_owner() {
    let mut root = FlushManagerRoot::new(vec![1_u64, 2]);

    root.adapter_mut().push(3);

    assert_eq!(root.adapter(), &[1, 2, 3]);
}
