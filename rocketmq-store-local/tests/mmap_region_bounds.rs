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

use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use memmap2::MmapMut;
use rocketmq_store_local::mapped_file::MmapRangeError;
use rocketmq_store_local::mapped_file::MmapRegionSlice;

#[derive(Debug, Clone, Copy)]
enum Expected {
    Valid(usize),
    Overflow,
    OutOfBounds,
}

#[test]
fn safe_region_construction_never_panics_and_classifies_invalid_ranges() {
    let mapping_len = 8;
    let mmap = Arc::new(MmapMut::map_anon(mapping_len).expect("create anonymous mapping"));
    let cases = [
        (0, 0, Expected::Valid(0)),
        (0, mapping_len, Expected::Valid(mapping_len)),
        (mapping_len, 0, Expected::Valid(0)),
        (mapping_len, 1, Expected::OutOfBounds),
        (usize::MAX, 1, Expected::Overflow),
        (1, usize::MAX, Expected::Overflow),
    ];

    for (offset, len, expected) in cases {
        let outcome = catch_unwind(AssertUnwindSafe({
            let mmap = Arc::clone(&mmap);
            move || MmapRegionSlice::try_new(mmap, offset, len)
        }));
        let result = outcome.unwrap_or_else(|_| panic!("safe constructor panicked for offset={offset}, len={len}"));

        match expected {
            Expected::Valid(expected_len) => {
                let region =
                    result.unwrap_or_else(|error| panic!("valid range offset={offset}, len={len} returned {error}"));
                assert_eq!(region.as_ref().len(), expected_len);
            }
            Expected::Overflow => {
                assert!(
                    matches!(result, Err(MmapRangeError::Overflow { offset: actual_offset, len: actual_len })
                        if actual_offset == offset && actual_len == len),
                    "expected overflow for offset={offset}, len={len}"
                );
            }
            Expected::OutOfBounds => {
                assert!(
                    matches!(
                        result,
                        Err(MmapRangeError::OutOfBounds {
                            offset: actual_offset,
                            len: actual_len,
                            mapping_len: actual_mapping_len,
                        }) if actual_offset == offset
                            && actual_len == len
                            && actual_mapping_len == mapping_len
                    ),
                    "expected out-of-bounds for offset={offset}, len={len}"
                );
            }
        }
    }
}
