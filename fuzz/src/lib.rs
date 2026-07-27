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

use std::borrow::Cow;

/// Decodes a reviewable `hex:` corpus seed, or borrows arbitrary fuzzer bytes unchanged.
pub fn corpus_bytes(input: &[u8]) -> Cow<'_, [u8]> {
    let Some(encoded) = input.strip_prefix(b"hex:") else {
        return Cow::Borrowed(input);
    };
    let encoded = encoded
        .iter()
        .copied()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    if encoded.len() % 2 != 0 {
        return Cow::Borrowed(input);
    }

    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.chunks_exact(2) {
        let Some(high) = hex_nibble(pair[0]) else {
            return Cow::Borrowed(input);
        };
        let Some(low) = hex_nibble(pair[1]) else {
            return Cow::Borrowed(input);
        };
        decoded.push((high << 4) | low);
    }
    Cow::Owned(decoded)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::corpus_bytes;

    #[test]
    fn hex_corpus_is_decoded_and_arbitrary_input_is_borrowed() {
        assert_eq!(corpus_bytes(b"hex:00 ff 10").as_ref(), &[0x00, 0xff, 0x10]);
        assert_eq!(corpus_bytes(b"arbitrary").as_ref(), b"arbitrary");
        assert_eq!(corpus_bytes(b"hex:0").as_ref(), b"hex:0");
    }
}
