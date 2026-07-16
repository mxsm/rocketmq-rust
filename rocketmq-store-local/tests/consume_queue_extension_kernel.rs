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

use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_store_local::consume_queue::extension::*;

#[test]
fn extension_unit_round_trip_preserves_bitmap_and_exact_size() {
    let mut original = CqExtUnit::new(12345, 67890, Some(vec![1, 2, 3, 4]));
    let encoded = original.write();
    let mut restored = CqExtUnit::default();

    assert_eq!(encoded.len(), MIN_EXT_UNIT_SIZE as usize + 4);
    assert!(restored.read(&mut Bytes::from(encoded)));
    assert_eq!(restored, original);
}

#[test]
fn extension_unit_rejects_missing_invalid_and_truncated_records() {
    let mut unit = CqExtUnit::default();

    assert!(!unit.read(&mut Bytes::new()));
    assert!(!unit.read(&mut Bytes::from_static(&[0, 0])));
    assert!(!unit.read(&mut Bytes::from_static(&[0, 20, 1, 2])));
}

#[test]
fn extension_unit_mutators_write_to_and_skip_remain_compatible() {
    let mut unit = CqExtUnit::default();
    unit.set_tags_code(99);
    unit.set_msg_store_time(1234);
    unit.set_filter_bit_map(Some(vec![5, 6]));
    let mut encoded = BytesMut::new();

    assert_eq!(unit.write_to(&mut encoded), MIN_EXT_UNIT_SIZE as usize + 2);
    assert_eq!(unit.tags_code(), 99);
    assert_eq!(unit.msg_store_time(), 1234);
    assert_eq!(unit.bit_map_size(), 2);
    assert_eq!(unit.filter_bit_map(), Some([5_u8, 6].as_slice()));

    let mut skipped = CqExtUnit::default();
    let mut bytes = encoded.freeze();
    skipped.read_by_skip(&mut bytes);
    assert_eq!(skipped.size(), MIN_EXT_UNIT_SIZE + 2);
    assert!(bytes.is_empty());
}

#[test]
fn extension_address_decoration_round_trips_the_real_offset() {
    for offset in [0, 64, MAX_CQ_EXT_REAL_OFFSET] {
        let address = decorate_cq_ext_offset(offset);
        assert!(is_cq_ext_address(address));
        assert_eq!(undecorate_cq_ext_address(address), offset);
    }
}

#[test]
fn extension_recovery_scan_stops_before_dirty_or_partial_tail() {
    let mut first = CqExtUnit::new(1, 10, None);
    let mut second = CqExtUnit::new(2, 20, Some(vec![3, 4]));
    let mut bytes = first.write();
    bytes.extend_from_slice(&second.write());
    let valid = bytes.len();
    bytes.extend_from_slice(&[0x7f, 0xff, 0xaa]);

    assert_eq!(scan_cq_ext_recovery(&bytes), valid);
    assert_eq!(scan_cq_ext_recovery(&bytes[..valid - 1]), first.size() as usize);
}

#[test]
fn extension_append_and_file_retention_plans_keep_legacy_boundaries() {
    assert_eq!(plan_cq_ext_append(64, 40, 20), CqExtAppendPlan::Append);
    assert_eq!(
        plan_cq_ext_append(64, 41, 20),
        CqExtAppendPlan::FillToEnd { blank_size: 19 }
    );
    assert!(cq_ext_capacity_available(0, MIN_EXT_UNIT_SIZE as i32));
    assert!(!cq_ext_capacity_available(MAX_CQ_EXT_REAL_OFFSET, 1));
    assert!(cq_ext_file_before_min(0, 64, 65));
    assert!(!cq_ext_file_before_min(0, 64, 64));
}
