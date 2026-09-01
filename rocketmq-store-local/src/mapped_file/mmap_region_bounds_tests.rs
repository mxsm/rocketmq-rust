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

use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::MappedFileError;
use crate::mapped_file::NativeMappedMemory;
use cheetah_string::CheetahString;
use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;

#[derive(Debug, Clone, Copy)]
enum Expected {
    Valid(usize),
    Overflow,
    NotReadable,
}

#[test]
fn safe_region_construction_never_panics_and_classifies_invalid_ranges() {
    let mapping_len = 8;
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("00000000000000000000");
    let mapped_file = DefaultMappedFile::<NativeMappedMemory>::try_new(
        CheetahString::from(path.to_string_lossy().into_owned()),
        mapping_len as u64,
    )
    .expect("mapped file");
    mapped_file.set_wrote_position(mapping_len as i32);
    assert!(mapped_file.try_seal_readable().expect("seal read-only generation"));
    let cases = [
        (0, 0, Expected::Valid(0)),
        (0, mapping_len, Expected::Valid(mapping_len)),
        (mapping_len, 0, Expected::Valid(0)),
        (mapping_len, 1, Expected::NotReadable),
        (usize::MAX, 1, Expected::Overflow),
        (1, usize::MAX, Expected::Overflow),
    ];

    for (offset, len, expected) in cases {
        let outcome = catch_unwind(AssertUnwindSafe(|| mapped_file.try_mapped_read_lease(offset, len)));
        let result = outcome.unwrap_or_else(|_| panic!("safe constructor panicked for offset={offset}, len={len}"));

        match expected {
            Expected::Valid(expected_len) => {
                let region = result
                    .unwrap_or_else(|error| panic!("valid range offset={offset}, len={len} returned {error}"))
                    .expect("sealed readable range");
                assert_eq!(region.as_ref().len(), expected_len);
            }
            Expected::Overflow => {
                assert!(
                    matches!(
                        result,
                        Err(MappedFileError::OutOfBounds {
                            offset: actual_offset,
                            size: actual_len,
                            file_size: 8,
                        }) if actual_offset == offset && actual_len == len
                    ),
                    "expected overflow for offset={offset}, len={len}"
                );
            }
            Expected::NotReadable => {
                assert!(
                    matches!(result, Ok(None)),
                    "expected unreadable range for offset={offset}, len={len}"
                );
            }
        }
    }
}
