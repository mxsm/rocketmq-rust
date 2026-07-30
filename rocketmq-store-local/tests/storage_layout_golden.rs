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

//! Checked-in compatibility fixtures for persisted Local Store layouts.

use bytes::Bytes;
use rocketmq_store_local::commit_log::record::is_blank_message;
use rocketmq_store_local::commit_log::record::BLANK_MAGIC_CODE;
use rocketmq_store_local::commit_log::record::MESSAGE_MAGIC_CODE;
use rocketmq_store_local::consume_queue::record::ConsumeQueueRecord;
use rocketmq_store_local::index::codec::IndexEntry;
use rocketmq_store_local::index::codec::IndexHeaderRecord;
use rocketmq_store_local::index::codec::IndexSlot;

fn decode_hex_fixture(fixture: &str) -> Vec<u8> {
    let encoded = fixture
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    assert_eq!(encoded.len() % 2, 0, "fixture must contain complete hex pairs");
    encoded
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).expect("hex high nibble");
            let low = (pair[1] as char).to_digit(16).expect("hex low nibble");
            ((high << 4) | low) as u8
        })
        .collect()
}

fn next_case_value(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(2_862_933_555_777_941_757)
        .wrapping_add(3_037_000_493);
    *state
}

#[test]
fn commit_log_markers_match_java_big_endian_layout() {
    let message = decode_hex_fixture(include_str!("fixtures/commit_log_message_v1.hex"));
    let blank = decode_hex_fixture(include_str!("fixtures/commit_log_blank.hex"));

    assert_eq!(&message[..4], &8_i32.to_be_bytes());
    assert_eq!(&message[4..], &MESSAGE_MAGIC_CODE.to_be_bytes());
    assert_eq!(&blank[..4], &8_i32.to_be_bytes());
    assert_eq!(&blank[4..], &BLANK_MAGIC_CODE.to_be_bytes());
    assert!(is_blank_message(&Bytes::from(blank)));
    assert!(!is_blank_message(&Bytes::from(message)));
}

#[test]
fn consume_queue_fixture_matches_the_20_byte_record_contract() {
    let fixture = decode_hex_fixture(include_str!("fixtures/consume_queue_v1.hex"));
    let expected = ConsumeQueueRecord::new(0x0102_0304_0506_0708, 0x1122_3344, 0x5152_5354_5556_5758);

    assert_eq!(fixture, expected.encode());
    assert_eq!(ConsumeQueueRecord::decode(&fixture), Some(expected));
}

#[test]
fn index_fixtures_match_header_slot_and_entry_contracts() {
    let header = IndexHeaderRecord {
        begin_timestamp: 0x0102_0304_0506_0708,
        end_timestamp: 0x1112_1314_1516_1718,
        begin_phy_offset: 0x2122_2324_2526_2728,
        end_phy_offset: 0x3132_3334_3536_3738,
        hash_slot_count: 0x4142_4344,
        index_count: 0x5152_5354,
    };
    let slot = IndexSlot(-7);
    let entry = IndexEntry::new(0x0102_0304, 0x1112_1314_1516_1718, -2, 0x2122_2324);
    let header_fixture = decode_hex_fixture(include_str!("fixtures/index_header_v1.hex"));
    let slot_fixture = decode_hex_fixture(include_str!("fixtures/index_slot_v1.hex"));
    let entry_fixture = decode_hex_fixture(include_str!("fixtures/index_entry_v1.hex"));

    assert_eq!(header_fixture, header.encode());
    assert_eq!(IndexHeaderRecord::decode(&header_fixture), Some(header));
    assert_eq!(slot_fixture, slot.encode());
    assert_eq!(IndexSlot::decode(&slot_fixture), Some(slot));
    assert_eq!(entry_fixture, entry.encode());
    assert_eq!(IndexEntry::decode(&entry_fixture), Some(entry));
}

#[test]
fn deterministic_storage_records_round_trip_and_preserve_offsets() {
    const SEED: u64 = 0x524d_5153_544f_5245;
    let mut state = SEED;
    let mut physical_offset = 0_i64;
    let mut consume_queue = Vec::new();
    let mut expected_records = Vec::new();

    for case in 0..64 {
        let message_size = ((next_case_value(&mut state) & 0x0fff) + 1) as i32;
        let tags_code = next_case_value(&mut state) as i64;
        let record = ConsumeQueueRecord::new(physical_offset, message_size, tags_code);
        consume_queue.extend_from_slice(&record.encode());
        expected_records.push(record);
        physical_offset = record.physical_end_offset();

        let slot = IndexSlot(next_case_value(&mut state) as i32);
        assert_eq!(
            IndexSlot::decode(&slot.encode()),
            Some(slot),
            "seed={SEED:#018x} case={case}"
        );
        let entry = IndexEntry::new(
            next_case_value(&mut state) as i32,
            record.physical_offset,
            (next_case_value(&mut state) & i32::MAX as u64) as i32,
            case,
        );
        assert_eq!(
            IndexEntry::decode(&entry.encode()),
            Some(entry),
            "seed={SEED:#018x} case={case}"
        );
        let header = IndexHeaderRecord {
            begin_timestamp: next_case_value(&mut state) as i64,
            end_timestamp: next_case_value(&mut state) as i64,
            begin_phy_offset: record.physical_offset,
            end_phy_offset: record.physical_end_offset(),
            hash_slot_count: ((next_case_value(&mut state) & 0x7fff) + 1) as i32,
            index_count: case + 1,
        };
        assert_eq!(
            IndexHeaderRecord::decode(&header.encode()),
            Some(header),
            "seed={SEED:#018x} case={case}"
        );
    }

    for (index, expected) in expected_records.iter().copied().enumerate() {
        let relative_offset = index * 20;
        let decoded = ConsumeQueueRecord::decode_at(&consume_queue, relative_offset)
            .unwrap_or_else(|| panic!("seed={SEED:#018x} record={index} failed to decode"));
        assert_eq!(decoded, expected, "seed={SEED:#018x} record={index}");
        if let Some(next) = expected_records.get(index + 1) {
            assert_eq!(
                decoded.physical_end_offset(),
                next.physical_offset,
                "seed={SEED:#018x} record={index} introduced a physical-offset hole"
            );
        }
    }
    assert!(ConsumeQueueRecord::decode_at(&consume_queue, consume_queue.len()).is_none());
}
