// Copyright 2026 The RocketMQ Rust Authors
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

/// Borrows extension-field text from a payload that interleaves text with
/// binary length prefixes or JSON punctuation.
///
/// Checking every key and value on its own costs one validator call per
/// string, which dominates a scan of short fields. This type validates the
/// payload in runs instead. Length-prefix bytes below `0x80` are ASCII, so a
/// typical payload is a single UTF-8 run and each field is a constant-time,
/// boundary-checked slice of it. A field outside the current run starts a new
/// run at that field, and a field no run can serve is validated on its own.
/// The result is therefore always the one per-field validation would produce.
pub(super) struct TextRuns<'a> {
    payload: &'a [u8],
    run_start: usize,
    run: &'a str,
}

impl<'a> TextRuns<'a> {
    #[inline]
    pub(super) const fn new(payload: &'a [u8]) -> Self {
        Self {
            payload,
            run_start: 0,
            run: "",
        }
    }

    /// Returns `payload[start..end]` as text.
    ///
    /// Returns `None` when the range is outside the payload or is not valid UTF-8.
    #[inline]
    pub(super) fn text(&mut self, start: usize, end: usize) -> Option<&'a str> {
        self.text_in_run(start, end)
            .or_else(|| self.text_in_new_run(start, end))
    }

    #[inline]
    fn text_in_run(&self, start: usize, end: usize) -> Option<&'a str> {
        let start = start.checked_sub(self.run_start)?;
        let end = end.checked_sub(self.run_start)?;
        self.run.get(start..end)
    }

    #[inline(never)]
    fn text_in_new_run(&mut self, start: usize, end: usize) -> Option<&'a str> {
        let field = self.payload.get(start..end)?;
        let rest = self.payload.get(start..)?;
        self.run = match std::str::from_utf8(rest) {
            Ok(run) => run,
            // The prefix up to the first invalid byte is valid by definition.
            Err(error) => std::str::from_utf8(&rest[..error.valid_up_to()]).unwrap_or_default(),
        };
        self.run_start = start;
        self.text_in_run(start, end).or_else(|| std::str::from_utf8(field).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serves_every_field_of_an_ascii_payload_from_one_run() {
        let payload = b"\x00\x03key\x00\x00\x00\x05value\x00\x01k\x00\x00\x00\x00";
        let mut runs = TextRuns::new(payload);

        assert_eq!(runs.text(2, 5), Some("key"));
        assert_eq!((runs.run_start, runs.run.len()), (2, payload.len() - 2));
        assert_eq!(runs.text(9, 14), Some("value"));
        assert_eq!(runs.text(16, 17), Some("k"));
        assert_eq!(runs.text(21, 21), Some(""));
        assert_eq!(runs.run_start, 2);
    }

    #[test]
    fn starts_a_new_run_after_a_length_byte_that_is_not_text() {
        // A 200-byte value has the length prefix 00 00 00 C8; C8 is not UTF-8 before ASCII.
        let mut payload = b"\x00\x01k\x00\x00\x00\xc8".to_vec();
        payload.extend(std::iter::repeat_n(b'v', 200));
        payload.extend_from_slice(b"\x00\x01n\x00\x00\x00\x01x");
        let mut runs = TextRuns::new(&payload);

        assert_eq!(runs.text(2, 3), Some("k"));
        assert_eq!((runs.run_start, runs.run.len()), (2, 4));
        assert_eq!(runs.text(7, 207).map(str::len), Some(200));
        assert_eq!(runs.run_start, 7);
        assert_eq!(runs.text(209, 210), Some("n"));
        assert_eq!(runs.text(214, 215), Some("x"));
        assert_eq!(runs.run_start, 7);
    }

    #[test]
    fn keeps_multibyte_text_and_rejects_ranges_that_are_not_text() {
        let payload = "\u{0}\u{6}主题\u{0}\u{0}\u{0}\u{4}🚀".as_bytes();
        let mut runs = TextRuns::new(payload);

        assert_eq!(runs.text(2, 8), Some("主题"));
        assert_eq!(runs.text(12, 16), Some("🚀"));
        // A range that splits a character is not text even though it lies inside a valid run.
        assert_eq!(runs.text(2, 4), None);
        assert_eq!(runs.text(13, 16), None);
        assert_eq!(runs.text(12, 17), None);
        assert_eq!(runs.text(17, 16), None);

        let invalid = b"\x00\x02\xff\xfe\x00\x00\x00\x01v";
        let mut runs = TextRuns::new(invalid);
        assert_eq!(runs.text(2, 4), None);
        assert_eq!(runs.text(8, 9), Some("v"));
    }
}
