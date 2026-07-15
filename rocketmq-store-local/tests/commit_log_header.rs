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
use rocketmq_store_local::commit_log::header::probe_store_timestamp;
use rocketmq_store_local::commit_log::header::store_timestamp_from_frame;
use rocketmq_store_local::commit_log::header::MESSAGE_MAGIC_CODE;
use rocketmq_store_local::commit_log::header::MESSAGE_MAGIC_CODE_V2;

#[test]
fn probe_reads_v1_ipv4_fields_in_fixed_header_order() {
    let timestamp = 1_721_234_567_890_i64;
    let mut reads = Vec::new();

    let actual = probe_store_timestamp(|offset, len| {
        reads.push((offset, len));
        match (offset, len) {
            (4, 4) => Some(Bytes::copy_from_slice(&MESSAGE_MAGIC_CODE.to_be_bytes())),
            (36, 4) => Some(Bytes::copy_from_slice(&0x20_i32.to_be_bytes())),
            (56, 8) => Some(Bytes::copy_from_slice(&timestamp.to_be_bytes())),
            _ => None,
        }
    });

    assert_eq!(Some(timestamp), actual);
    assert_eq!(vec![(4, 4), (36, 4), (56, 8)], reads);
}

#[test]
fn probe_accepts_v2_and_selects_ipv6_timestamp_position() {
    let timestamp = 1_721_234_567_891_i64;
    let mut reads = Vec::new();

    let actual = probe_store_timestamp(|offset, len| {
        reads.push((offset, len));
        match (offset, len) {
            (4, 4) => Some(Bytes::copy_from_slice(&MESSAGE_MAGIC_CODE_V2.to_be_bytes())),
            (36, 4) => Some(Bytes::copy_from_slice(&0x10_i32.to_be_bytes())),
            (68, 8) => Some(Bytes::copy_from_slice(&timestamp.to_be_bytes())),
            _ => None,
        }
    });

    assert_eq!(Some(timestamp), actual);
    assert_eq!(vec![(4, 4), (36, 4), (68, 8)], reads);
}

#[test]
fn missing_magic_falls_back_to_zero_and_short_circuits() {
    let mut reads = Vec::new();

    let actual = probe_store_timestamp(|offset, len| {
        reads.push((offset, len));
        None
    });

    assert_eq!(None, actual);
    assert_eq!(vec![(4, 4)], reads);
}

#[test]
fn invalid_magic_short_circuits_before_sys_flag() {
    let mut reads = Vec::new();

    let actual = probe_store_timestamp(|offset, len| {
        reads.push((offset, len));
        Some(Bytes::copy_from_slice(&0x1234_5678_i32.to_be_bytes()))
    });

    assert_eq!(None, actual);
    assert_eq!(vec![(4, 4)], reads);
}

#[test]
fn missing_sys_flag_falls_back_to_ipv4_and_missing_timestamp_falls_back_to_zero() {
    let mut reads = Vec::new();

    let actual = probe_store_timestamp(|offset, len| {
        reads.push((offset, len));
        match (offset, len) {
            (4, 4) => Some(Bytes::copy_from_slice(&MESSAGE_MAGIC_CODE.to_be_bytes())),
            _ => None,
        }
    });

    assert_eq!(None, actual);
    assert_eq!(vec![(4, 4), (36, 4), (56, 8)], reads);
}

#[test]
fn zero_timestamp_is_not_a_recovery_candidate() {
    let actual = probe_store_timestamp(|offset, len| match (offset, len) {
        (4, 4) => Some(Bytes::copy_from_slice(&MESSAGE_MAGIC_CODE.to_be_bytes())),
        (36, 4) => Some(Bytes::copy_from_slice(&0_i32.to_be_bytes())),
        (56, 8) => Some(Bytes::copy_from_slice(&0_i64.to_be_bytes())),
        _ => None,
    });

    assert_eq!(None, actual);
}

#[test]
#[should_panic]
fn probe_panics_when_provider_returns_a_short_present_field() {
    let _ = probe_store_timestamp(|offset, _| match offset {
        4 => Some(Bytes::from_static(&[0; 3])),
        _ => None,
    });
}

#[test]
fn strict_frame_reader_decodes_ipv4_and_ipv6_big_endian_timestamps() {
    let ipv4_timestamp = 0x0102_0304_0506_0708_i64;
    let mut ipv4_frame = [0_u8; 76];
    ipv4_frame[36..40].copy_from_slice(&0x20_i32.to_be_bytes());
    ipv4_frame[56..64].copy_from_slice(&ipv4_timestamp.to_be_bytes());
    assert_eq!(ipv4_timestamp, store_timestamp_from_frame(&ipv4_frame));

    let ipv6_timestamp = 0x1112_1314_1516_1718_i64;
    let mut ipv6_frame = [0_u8; 76];
    ipv6_frame[36..40].copy_from_slice(&0x10_i32.to_be_bytes());
    ipv6_frame[68..76].copy_from_slice(&ipv6_timestamp.to_be_bytes());
    assert_eq!(ipv6_timestamp, store_timestamp_from_frame(&ipv6_frame));
}

#[test]
#[should_panic]
fn strict_frame_reader_panics_when_sys_flag_is_short() {
    let _ = store_timestamp_from_frame(&[0; 39]);
}

#[test]
#[should_panic]
fn strict_frame_reader_panics_when_selected_timestamp_is_short() {
    let mut frame = [0_u8; 75];
    frame[36..40].copy_from_slice(&0x10_i32.to_be_bytes());
    let _ = store_timestamp_from_frame(&frame);
}
