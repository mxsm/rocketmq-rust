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

#[test]
fn staging_copy_uses_disjoint_allocations() {
    let staging = *b"mapped-write-lease";
    let mut destination = [0_u8; 18];

    // SAFETY: `staging` and `destination` are distinct live allocations, and the destination is
    // exactly large enough for the copied source range. This avoids requiring an operating-system
    // mmap, which Miri does not emulate.
    unsafe {
        std::ptr::copy_nonoverlapping(staging.as_ptr(), destination.as_mut_ptr(), staging.len());
    }

    assert_eq!(destination, staging);
}
