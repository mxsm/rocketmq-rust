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

use rocketmq_model::codec::bytes_to_string;
use rocketmq_model::codec::string_to_bytes;
use rocketmq_model::codec::write_int;
use rocketmq_model::codec::write_short;

#[test]
fn bytes_and_mixed_case_hex_have_expected_values() {
    assert_eq!(bytes_to_string(&[0x00, 0x0f, 0x10, 0x7f, 0x80, 0xff]), "000F107F80FF");
    assert_eq!(string_to_bytes("aBcD09fE"), Some(vec![0xab, 0xcd, 0x09, 0xfe]));
}

#[test]
fn every_byte_round_trips_and_empty_inputs_keep_their_distinction() {
    let corpus: Vec<u8> = (0..=u8::MAX).collect();
    let expected: String = corpus.iter().map(|byte| format!("{byte:02X}")).collect();

    assert_eq!(bytes_to_string(&corpus).len(), corpus.len() * 2);
    assert_eq!(bytes_to_string(&corpus), expected);
    assert_eq!(string_to_bytes(&expected), Some(corpus));
    assert_eq!(bytes_to_string(&[]), "");
    assert_eq!(string_to_bytes(""), None);
}

#[test]
fn malformed_hex_is_rejected_without_panicking() {
    for value in ["0", "GG", "12 3", "\u{e9}"] {
        assert_eq!(string_to_bytes(value), None, "{value:?} should be rejected");
    }
}

#[test]
fn fixed_width_writes_encode_signed_boundaries() {
    let mut int_buffer = vec!['x'; 12];
    write_int(&mut int_buffer, 2, 0);
    assert_eq!(int_buffer[2..10].iter().collect::<String>(), "00000000");
    write_int(&mut int_buffer, 2, -1);
    assert_eq!(int_buffer[2..10].iter().collect::<String>(), "FFFFFFFF");
    write_int(&mut int_buffer, 2, i32::MIN);
    assert_eq!(int_buffer[2..10].iter().collect::<String>(), "80000000");
    assert_eq!(&int_buffer[..2], &['x'; 2]);
    assert_eq!(&int_buffer[10..], &['x'; 2]);

    let mut short_buffer = vec!['x'; 8];
    write_short(&mut short_buffer, 4, 0);
    assert_eq!(short_buffer[4..].iter().collect::<String>(), "0000");
    write_short(&mut short_buffer, 4, -1);
    assert_eq!(short_buffer[4..].iter().collect::<String>(), "FFFF");
    write_short(&mut short_buffer, 4, i16::MAX);
    assert_eq!(short_buffer[4..].iter().collect::<String>(), "7FFF");
    assert_eq!(&short_buffer[..4], &['x'; 4]);
}
