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

//! Apache RocketMQ 5.x timer files, byte for byte: the `timerlog` unit and blank, the
//! `timerwheel` slot and the `timercheck` record. Integers are big-endian, as Java's `ByteBuffer`
//! writes them.

use thiserror::Error;

use crate::timer::slot::Slot;

/// Bytes of one TimerLog unit (`TimerLog.UNIT_SIZE`).
pub const JAVA_TIMER_LOG_UNIT_SIZE: usize = 52;

/// Bytes of the blank record that pads the end of a TimerLog file (`TimerLog.MIN_BLANK_LEN`).
pub const JAVA_TIMER_LOG_MIN_BLANK_LEN: usize = 16;

/// Magic of a TimerLog blank record (`TimerLog.BLANK_MAGIC_CODE`, `0xBBCCDDEE ^ 1880681586 + 8` in
/// Java, where `+` binds first).
pub const JAVA_TIMER_LOG_BLANK_MAGIC: i32 = (0xBBCC_DDEE_u32 as i32) ^ (1_880_681_586 + 8);

/// Bytes of one TimerWheel slot (`Slot.SIZE`).
pub const JAVA_TIMER_WHEEL_SLOT_SIZE: usize = 32;

/// Bytes of the timer checkpoint record (`TimerCheckpoint.encode`).
pub const JAVA_TIMER_CHECKPOINT_SIZE: usize = 56;

/// Bytes Java maps for the `timercheck` file, one OS page.
pub const JAVA_TIMER_CHECKPOINT_FILE_SIZE: usize = 4_096;

/// A Java timer file record that cannot be decoded.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum JavaTimerFormatError {
    /// Fewer bytes than the record needs.
    #[error("{record} needs {needed} bytes, got {actual}")]
    Truncated {
        record: &'static str,
        needed: usize,
        actual: usize,
    },
    /// A TimerLog unit whose size field is not [`JAVA_TIMER_LOG_UNIT_SIZE`].
    #[error("TimerLog unit declares {0} bytes, expected 52")]
    UnitSize(i32),
    /// A TimerLog unit whose magic sets bits above the four magic flags.
    #[error("TimerLog unit magic {0:#x} is not valid")]
    UnitMagic(i32),
}

/// Whether `magic` only uses the four TimerLog magic flags (`TimerMessageStore.isMagicOK`).
pub const fn is_java_timer_magic_valid(magic: i32) -> bool {
    (magic | 0xF) == 0xF
}

/// Java `String.hashCode()` of `topic`, which a TimerLog unit stores for metrics
/// (`TimerMessageStore.hashTopicForMetrics`).
pub fn java_topic_hash(topic: &str) -> i32 {
    topic
        .encode_utf16()
        .fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit)))
}

/// One TimerLog unit as `TimerMessageStore.doEnqueue` writes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JavaTimerLogUnit {
    /// Position of the previous unit of the same slot, or `-1` for the first one.
    pub prev_pos: i64,
    /// `MAGIC_DEFAULT` plus the `MAGIC_ROLL` and `MAGIC_DELETE` flags.
    pub magic: i32,
    /// Enqueue write time when the unit was written.
    pub curr_write_time_ms: i64,
    /// Slot time minus `curr_write_time_ms`.
    pub delay_ms: i32,
    /// CommitLog offset of the source message.
    pub commit_log_offset: i64,
    /// CommitLog size of the source message.
    pub size: i32,
    /// [`java_topic_hash`] of the real topic.
    pub topic_hash: i32,
    /// Reserved, written as `0`.
    pub reserved: i64,
}

impl JavaTimerLogUnit {
    /// Time of the slot the unit was enqueued into.
    pub const fn slot_time_ms(&self) -> i64 {
        self.curr_write_time_ms.saturating_add(self.delay_ms as i64)
    }

    /// Encodes the unit, with its size field.
    pub fn encode(&self) -> [u8; JAVA_TIMER_LOG_UNIT_SIZE] {
        let mut bytes = [0u8; JAVA_TIMER_LOG_UNIT_SIZE];
        bytes[0..4].copy_from_slice(&(JAVA_TIMER_LOG_UNIT_SIZE as i32).to_be_bytes());
        bytes[4..12].copy_from_slice(&self.prev_pos.to_be_bytes());
        bytes[12..16].copy_from_slice(&self.magic.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.curr_write_time_ms.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.delay_ms.to_be_bytes());
        bytes[28..36].copy_from_slice(&self.commit_log_offset.to_be_bytes());
        bytes[36..40].copy_from_slice(&self.size.to_be_bytes());
        bytes[40..44].copy_from_slice(&self.topic_hash.to_be_bytes());
        bytes[44..52].copy_from_slice(&self.reserved.to_be_bytes());
        bytes
    }
}

/// What a TimerLog holds at a unit position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JavaTimerLogRecord {
    /// A timer unit.
    Unit(JavaTimerLogUnit),
    /// The blank that pads the rest of the file; `length` counts it and everything after it.
    Blank { length: i32 },
}

impl JavaTimerLogRecord {
    /// Decodes the record at the start of `bytes`, as Java recovery does: a blank magic ends
    /// the file, and a unit must have size 52 and a valid magic.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerFormatError`] when `bytes` is shorter than the record, or when a unit
    /// has the wrong size or magic.
    pub fn decode(bytes: &[u8]) -> Result<Self, JavaTimerFormatError> {
        let header = bytes
            .get(..JAVA_TIMER_LOG_MIN_BLANK_LEN)
            .ok_or(JavaTimerFormatError::Truncated {
                record: "TimerLog record",
                needed: JAVA_TIMER_LOG_MIN_BLANK_LEN,
                actual: bytes.len(),
            })?;
        let size = read_i32(header, 0);
        let magic = read_i32(header, 12);
        if magic == JAVA_TIMER_LOG_BLANK_MAGIC {
            return Ok(Self::Blank { length: size });
        }
        if size != JAVA_TIMER_LOG_UNIT_SIZE as i32 {
            return Err(JavaTimerFormatError::UnitSize(size));
        }
        if !is_java_timer_magic_valid(magic) {
            return Err(JavaTimerFormatError::UnitMagic(magic));
        }
        let unit = bytes
            .get(..JAVA_TIMER_LOG_UNIT_SIZE)
            .ok_or(JavaTimerFormatError::Truncated {
                record: "TimerLog unit",
                needed: JAVA_TIMER_LOG_UNIT_SIZE,
                actual: bytes.len(),
            })?;
        Ok(Self::Unit(JavaTimerLogUnit {
            prev_pos: read_i64(unit, 4),
            magic,
            curr_write_time_ms: read_i64(unit, 16),
            delay_ms: read_i32(unit, 24),
            commit_log_offset: read_i64(unit, 28),
            size: read_i32(unit, 36),
            topic_hash: read_i32(unit, 40),
            reserved: read_i64(unit, 44),
        }))
    }
}

/// Encodes the blank Java writes at `wrote_position` of a TimerLog file of `file_size` bytes
/// before it moves to the next file: the remaining length, a zero long and the blank magic.
pub fn encode_java_timer_log_blank(file_size: usize, wrote_position: usize) -> [u8; JAVA_TIMER_LOG_MIN_BLANK_LEN] {
    let remaining = i32::try_from(file_size.saturating_sub(wrote_position)).unwrap_or(i32::MAX);
    let mut bytes = [0u8; JAVA_TIMER_LOG_MIN_BLANK_LEN];
    bytes[0..4].copy_from_slice(&remaining.to_be_bytes());
    bytes[12..16].copy_from_slice(&JAVA_TIMER_LOG_BLANK_MAGIC.to_be_bytes());
    bytes
}

/// Whether a record of `len` bytes leaves no room for the blank in a TimerLog file of
/// `file_size` bytes written up to `wrote_position`, so Java pads the file and starts the next
/// one (`TimerLog.append`).
pub const fn java_timer_log_needs_next_file(file_size: usize, wrote_position: usize, len: usize) -> bool {
    len + JAVA_TIMER_LOG_MIN_BLANK_LEN > file_size.saturating_sub(wrote_position)
}

/// Offset of the last unit a TimerLog file of `file_size` bytes can hold, which Java checks
/// before deleting the file (`TimerLog.getOffsetForLastUnit`). `None` when not even one unit and
/// the blank fit.
pub const fn java_timer_log_last_unit_offset(file_size: usize) -> Option<usize> {
    if file_size < JAVA_TIMER_LOG_MIN_BLANK_LEN + JAVA_TIMER_LOG_UNIT_SIZE {
        return None;
    }
    Some(
        file_size
            - (file_size - JAVA_TIMER_LOG_MIN_BLANK_LEN) % JAVA_TIMER_LOG_UNIT_SIZE
            - JAVA_TIMER_LOG_MIN_BLANK_LEN
            - JAVA_TIMER_LOG_UNIT_SIZE,
    )
}

/// Bytes of a `timerwheel` file with `slots_total` slots per round; the wheel holds two rounds
/// (`TimerWheel.wheelLength`).
pub const fn java_timer_wheel_len(slots_total: usize) -> usize {
    slots_total * 2 * JAVA_TIMER_WHEEL_SLOT_SIZE
}

/// Index of the slot that holds `time_ms` (`TimerWheel.getSlotIndex`).
pub fn java_timer_wheel_slot_index(time_ms: i64, precision_ms: i64, slots_total: usize) -> usize {
    let rounds_len = i64::try_from(slots_total.saturating_mul(2)).unwrap_or(i64::MAX).max(1);
    time_ms.div_euclid(precision_ms.max(1)).rem_euclid(rounds_len) as usize
}

/// Encodes `slot` as Java stores it: its time in precision units, then the positions, the count
/// and the magic.
pub fn encode_java_timer_wheel_slot(slot: Slot, precision_ms: i64) -> [u8; JAVA_TIMER_WHEEL_SLOT_SIZE] {
    let mut bytes = [0u8; JAVA_TIMER_WHEEL_SLOT_SIZE];
    bytes[0..8].copy_from_slice(&slot.time_ms.div_euclid(precision_ms.max(1)).to_be_bytes());
    bytes[8..16].copy_from_slice(&slot.first_pos.to_be_bytes());
    bytes[16..24].copy_from_slice(&slot.last_pos.to_be_bytes());
    bytes[24..28].copy_from_slice(&slot.num.to_be_bytes());
    bytes[28..32].copy_from_slice(&slot.magic.to_be_bytes());
    bytes
}

/// Decodes the slot for `time_ms` from its 32 wheel bytes. `None` when the bytes hold another
/// round's slot or were never written, where Java's `TimerWheel.getSlot` returns `-1`.
///
/// # Errors
///
/// Returns [`JavaTimerFormatError::Truncated`] when `bytes` is shorter than a slot.
pub fn decode_java_timer_wheel_slot(
    bytes: &[u8],
    time_ms: i64,
    precision_ms: i64,
) -> Result<Option<Slot>, JavaTimerFormatError> {
    let bytes = bytes
        .get(..JAVA_TIMER_WHEEL_SLOT_SIZE)
        .ok_or(JavaTimerFormatError::Truncated {
            record: "TimerWheel slot",
            needed: JAVA_TIMER_WHEEL_SLOT_SIZE,
            actual: bytes.len(),
        })?;
    let precision_ms = precision_ms.max(1);
    let slot_time_ms = read_i64(bytes, 0).saturating_mul(precision_ms);
    if slot_time_ms != time_ms.div_euclid(precision_ms).saturating_mul(precision_ms) {
        return Ok(None);
    }
    Ok(Some(Slot::new_with_num_magic(
        slot_time_ms,
        read_i64(bytes, 8),
        read_i64(bytes, 16),
        read_i32(bytes, 24),
        read_i32(bytes, 28),
    )))
}

/// The `timercheck` record (`TimerCheckpoint`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JavaTimerCheckpoint {
    pub last_read_time_ms: i64,
    pub last_timer_log_flush_pos: i64,
    pub last_timer_queue_offset: i64,
    pub master_timer_queue_offset: i64,
    pub data_version_state: i64,
    pub data_version_timestamp: i64,
    pub data_version_counter: i64,
}

impl JavaTimerCheckpoint {
    /// Encodes the 56-byte record that starts the `timercheck` page.
    pub fn encode(&self) -> [u8; JAVA_TIMER_CHECKPOINT_SIZE] {
        let mut bytes = [0u8; JAVA_TIMER_CHECKPOINT_SIZE];
        for (index, value) in [
            self.last_read_time_ms,
            self.last_timer_log_flush_pos,
            self.last_timer_queue_offset,
            self.master_timer_queue_offset,
            self.data_version_state,
            self.data_version_timestamp,
            self.data_version_counter,
        ]
        .into_iter()
        .enumerate()
        {
            bytes[index * 8..index * 8 + 8].copy_from_slice(&value.to_be_bytes());
        }
        bytes
    }

    /// Decodes the record at the start of `bytes`. Like Java, a record of only the four offsets
    /// leaves the data version at zero.
    ///
    /// # Errors
    ///
    /// Returns [`JavaTimerFormatError::Truncated`] when `bytes` cannot hold the four offsets.
    pub fn decode(bytes: &[u8]) -> Result<Self, JavaTimerFormatError> {
        if bytes.len() < 32 {
            return Err(JavaTimerFormatError::Truncated {
                record: "timer checkpoint",
                needed: 32,
                actual: bytes.len(),
            });
        }
        let version = |offset: usize| bytes.get(offset..offset + 8).map_or(0, |value| read_i64(value, 0));
        Ok(Self {
            last_read_time_ms: read_i64(bytes, 0),
            last_timer_log_flush_pos: read_i64(bytes, 8),
            last_timer_queue_offset: read_i64(bytes, 16),
            master_timer_queue_offset: read_i64(bytes, 24),
            data_version_state: version(32),
            data_version_timestamp: version(40),
            data_version_counter: version(48),
        })
    }
}

fn read_i32(bytes: &[u8], offset: usize) -> i32 {
    i32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("fixed i32 field"))
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("fixed i64 field"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_log_unit_bytes_follow_java_do_enqueue() {
        let unit = JavaTimerLogUnit {
            prev_pos: -1,
            magic: 0b0011,
            curr_write_time_ms: 0x0102_0304_0506_0708,
            delay_ms: 0x0A0B_0C0D,
            commit_log_offset: 0x1112_1314_1516_1718,
            size: 0x2122_2324,
            topic_hash: java_topic_hash("hello"),
            reserved: 0,
        };
        let encoded = unit.encode();
        #[rustfmt::skip]
        let expected: [u8; 52] = [
            0, 0, 0, 52,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0, 0, 0, 3,
            1, 2, 3, 4, 5, 6, 7, 8,
            0x0A, 0x0B, 0x0C, 0x0D,
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
            0x21, 0x22, 0x23, 0x24,
            0x05, 0xE9, 0x18, 0xD2,
            0, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(encoded, expected);
        assert_eq!(JavaTimerLogRecord::decode(&encoded), Ok(JavaTimerLogRecord::Unit(unit)));
        assert_eq!(unit.slot_time_ms(), 0x0102_0304_0506_0708 + 0x0A0B_0C0D);
    }

    #[test]
    fn timer_log_records_are_checked_like_java_recovery() {
        assert_eq!(JAVA_TIMER_LOG_BLANK_MAGIC as u32, 0xCBD4_3194);
        let blank = encode_java_timer_log_blank(1_000, 940);
        assert_eq!(blank, [0, 0, 0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0xCB, 0xD4, 0x31, 0x94]);
        assert_eq!(
            JavaTimerLogRecord::decode(&blank),
            Ok(JavaTimerLogRecord::Blank { length: 60 })
        );

        let unit = JavaTimerLogUnit {
            prev_pos: 0,
            magic: 1,
            curr_write_time_ms: 0,
            delay_ms: 0,
            commit_log_offset: 0,
            size: 64,
            topic_hash: 0,
            reserved: 0,
        };
        let mut wrong_size = unit.encode();
        wrong_size[3] = 40;
        assert_eq!(
            JavaTimerLogRecord::decode(&wrong_size),
            Err(JavaTimerFormatError::UnitSize(40))
        );
        let mut wrong_magic = unit.encode();
        wrong_magic[15] = 0x11;
        assert_eq!(
            JavaTimerLogRecord::decode(&wrong_magic),
            Err(JavaTimerFormatError::UnitMagic(0x11))
        );
        assert!(matches!(
            JavaTimerLogRecord::decode(&unit.encode()[..20]),
            Err(JavaTimerFormatError::Truncated { needed: 52, .. })
        ));
    }

    #[test]
    fn timer_log_files_end_where_java_starts_the_next_one() {
        // A 1 MiB file holds 20164 units; after the last one 48 bytes remain, enough for the blank
        // but not for another unit and its blank.
        let file_size = 1024 * 1024;
        assert_eq!(java_timer_log_last_unit_offset(file_size), Some(20_163 * 52));
        assert!(!java_timer_log_needs_next_file(file_size, 20_163 * 52, 52));
        assert!(java_timer_log_needs_next_file(file_size, 20_164 * 52, 52));
        assert_eq!(java_timer_log_last_unit_offset(60), None);
    }

    #[test]
    fn topic_hash_is_java_string_hash_code() {
        assert_eq!(java_topic_hash(""), 0);
        assert_eq!(java_topic_hash("hello"), 99_162_322);
        assert_eq!(java_topic_hash("Hello World"), -862_545_276);
        assert_eq!(java_topic_hash("中"), 0x4E2D);
        // A character outside the BMP hashes as its two UTF-16 surrogates.
        assert_eq!(java_topic_hash("😀"), 0xD83D * 31 + 0xDE00);
    }

    #[test]
    fn wheel_slots_store_time_in_precision_units_and_reject_other_rounds() {
        let precision_ms = 1_000;
        let slots_total = 7 * 86_400;
        assert_eq!(java_timer_wheel_len(slots_total), 38_707_200);

        let slot = Slot::new_with_num_magic(1_700_000_000_000, 520, 1_040, 3, 1);
        let encoded = encode_java_timer_wheel_slot(slot, precision_ms);
        assert_eq!(&encoded[0..8], &1_700_000_000i64.to_be_bytes());
        assert_eq!(
            decode_java_timer_wheel_slot(&encoded, 1_700_000_000_400, precision_ms),
            Ok(Some(slot))
        );
        let next_round_ms = 1_700_000_000_000 + (slots_total as i64 * 2) * precision_ms;
        assert_eq!(
            java_timer_wheel_slot_index(next_round_ms, precision_ms, slots_total),
            java_timer_wheel_slot_index(1_700_000_000_000, precision_ms, slots_total)
        );
        assert_eq!(
            decode_java_timer_wheel_slot(&encoded, next_round_ms, precision_ms),
            Ok(None)
        );
        assert_eq!(
            decode_java_timer_wheel_slot(&[0; 32], 1_700_000_000_000, precision_ms),
            Ok(None)
        );
        assert_eq!(
            java_timer_wheel_slot_index(1_700_000_000_000, precision_ms, slots_total),
            (1_700_000_000 % (slots_total as i64 * 2)) as usize
        );
    }

    #[test]
    fn checkpoint_record_is_seven_longs_and_tolerates_a_missing_version() {
        let checkpoint = JavaTimerCheckpoint {
            last_read_time_ms: 1,
            last_timer_log_flush_pos: 2,
            last_timer_queue_offset: 3,
            master_timer_queue_offset: 4,
            data_version_state: 5,
            data_version_timestamp: 6,
            data_version_counter: 7,
        };
        let encoded = checkpoint.encode();
        for (index, value) in (1..=7i64).enumerate() {
            assert_eq!(&encoded[index * 8..index * 8 + 8], &value.to_be_bytes());
        }
        let mut page = [0u8; JAVA_TIMER_CHECKPOINT_FILE_SIZE];
        page[..JAVA_TIMER_CHECKPOINT_SIZE].copy_from_slice(&encoded);
        assert_eq!(JavaTimerCheckpoint::decode(&page), Ok(checkpoint));

        let offsets_only = JavaTimerCheckpoint::decode(&encoded[..32]).unwrap();
        assert_eq!(offsets_only.master_timer_queue_offset, 4);
        assert_eq!(offsets_only.data_version_counter, 0);
        assert!(JavaTimerCheckpoint::decode(&encoded[..31]).is_err());
    }
}
