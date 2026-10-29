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

use super::checked_body;
use super::entry::NativeIndexEntry;
use super::entry::NATIVE_INDEX_ENTRY_SIZE;
use super::layout::NativeBucketId;
use super::layout::NativeBucketKind;
use super::push_checksum;
use super::FieldReader;
use super::NativeFormatError;
use crate::timer::storage_format::crc32c;

/// Size of the header at the start of every bucket file.
pub const NATIVE_FILE_HEADER_SIZE: usize = 64;
/// Size of a frame header.
pub const NATIVE_FRAME_HEADER_SIZE: usize = 16;
/// Entries one frame holds at most, so that a full frame is exactly 4 KiB.
pub const NATIVE_FRAME_MAX_ENTRIES: usize = 85;

const FILE_MAGIC: u32 = 0x544E_424B;
const FILE_VERSION: u16 = 1;
const FRAME_MAGIC: u32 = 0x544E_4652;
const FRAME_SORTED: u16 = 1;

/// The header of a bucket file, naming the bucket it belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeFileHeader {
    pub bucket: NativeBucketId,
}

impl NativeFileHeader {
    /// Encodes the header.
    pub fn encode(&self) -> [u8; NATIVE_FILE_HEADER_SIZE] {
        let mut output = Vec::with_capacity(NATIVE_FILE_HEADER_SIZE);
        output.extend_from_slice(&FILE_MAGIC.to_be_bytes());
        output.extend_from_slice(&FILE_VERSION.to_be_bytes());
        output.extend_from_slice(&(NATIVE_FILE_HEADER_SIZE as u16).to_be_bytes());
        output.push(self.bucket.kind() as u8);
        output.resize(16, 0);
        output.extend_from_slice(&self.bucket.start_ms().to_be_bytes());
        output.extend_from_slice(&self.bucket.end_ms().to_be_bytes());
        output.resize(NATIVE_FILE_HEADER_SIZE - 4, 0);
        push_checksum(&mut output);
        let mut header = [0u8; NATIVE_FILE_HEADER_SIZE];
        header.copy_from_slice(&output);
        header
    }

    /// Decodes the header at the start of a bucket file.
    ///
    /// # Errors
    ///
    /// Returns [`NativeFormatError`] when the header is short, of another version or corrupt,
    /// or names a window that is not one bucket of its kind.
    pub fn decode(bytes: &[u8]) -> Result<Self, NativeFormatError> {
        let record = "bucket file header";
        let bytes = bytes
            .get(..NATIVE_FILE_HEADER_SIZE)
            .ok_or(NativeFormatError::Truncated {
                record,
                needed: NATIVE_FILE_HEADER_SIZE,
                actual: bytes.len(),
            })?;
        let mut reader = FieldReader::new(record, bytes);
        let magic = reader.u32()?;
        if magic != FILE_MAGIC {
            return Err(NativeFormatError::BadMagic { record, found: magic });
        }
        let version = reader.u16()?;
        if version != FILE_VERSION {
            return Err(NativeFormatError::UnsupportedVersion { record, found: version });
        }
        checked_body(record, bytes)?;
        let invalid = |field| NativeFormatError::InvalidField { record, field };
        if usize::from(reader.u16()?) != NATIVE_FILE_HEADER_SIZE {
            return Err(invalid("size"));
        }
        let kind = NativeBucketKind::from_code(reader.u8()?).ok_or(invalid("bucket kind"))?;
        reader.take(16 - reader.position())?;
        let start_ms = reader.i64()?;
        let end_ms = reader.i64()?;
        let bucket = NativeBucketId::starting_at(kind, start_ms)
            .filter(|bucket| bucket.end_ms() == end_ms)
            .ok_or(invalid("window"))?;
        Ok(Self { bucket })
    }
}

/// Whether a frame's entries are in delivery order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeFrameOrder {
    /// In the order they were indexed, as hour and day buckets append them.
    Arrival,
    /// Sorted by `(due_ms, cq_offset)`, as minute runs are written.
    Sorted,
}

/// One decoded frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeFrame {
    pub order: NativeFrameOrder,
    pub entries: Vec<NativeIndexEntry>,
}

/// Appends `entries` to `output` as frames of at most [`NATIVE_FRAME_MAX_ENTRIES`].
///
/// A frame header holds its magic, entry count and flags, then the CRC32C of those 8 bytes and
/// the entries, then 4 reserved bytes.
///
/// # Errors
///
/// Returns [`NativeFormatError::InvalidField`] when an entry is invalid, or when `order` is
/// [`NativeFrameOrder::Sorted`] and the entries are not. `output` is unchanged on error.
pub fn encode_native_frames(
    entries: &[NativeIndexEntry],
    order: NativeFrameOrder,
    output: &mut Vec<u8>,
) -> Result<(), NativeFormatError> {
    if order == NativeFrameOrder::Sorted && !is_sorted(entries) {
        return Err(NativeFormatError::InvalidField {
            record: "frame",
            field: "order",
        });
    }
    let start = output.len();
    for chunk in entries.chunks(NATIVE_FRAME_MAX_ENTRIES) {
        if let Err(error) = encode_frame(chunk, order, output) {
            output.truncate(start);
            return Err(error);
        }
    }
    Ok(())
}

fn encode_frame(
    entries: &[NativeIndexEntry],
    order: NativeFrameOrder,
    output: &mut Vec<u8>,
) -> Result<(), NativeFormatError> {
    let flags = match order {
        NativeFrameOrder::Arrival => 0,
        NativeFrameOrder::Sorted => FRAME_SORTED,
    };
    let mut body = Vec::with_capacity(8 + entries.len() * NATIVE_INDEX_ENTRY_SIZE);
    body.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
    // A chunk never exceeds `NATIVE_FRAME_MAX_ENTRIES`.
    body.extend_from_slice(&(entries.len() as u16).to_be_bytes());
    body.extend_from_slice(&flags.to_be_bytes());
    for entry in entries {
        entry.encode_into(&mut body)?;
    }
    output.extend_from_slice(&body[..8]);
    output.extend_from_slice(&crc32c(&body).to_be_bytes());
    output.extend_from_slice(&[0; 4]);
    output.extend_from_slice(&body[8..]);
    Ok(())
}

/// Decodes the frames that fill `bytes`, a bucket file after its header.
///
/// # Errors
///
/// Returns [`NativeFormatError`] at the first frame that is cut short, corrupt, of an unknown
/// flag, or marked sorted without being sorted. Recovery cuts a bucket file to the length its
/// checkpoint recorded first, so an error here is damage inside the checkpointed part.
pub fn decode_native_frames(bytes: &[u8]) -> Result<Vec<NativeFrame>, NativeFormatError> {
    let mut frames = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        let (frame, length) = decode_native_frame(rest)?;
        frames.push(frame);
        rest = &rest[length..];
    }
    Ok(frames)
}

/// Decodes the frame at the start of `bytes` and returns it with its encoded length.
///
/// # Errors
///
/// Returns [`NativeFormatError::Truncated`] when `bytes` ends inside the frame, and the errors
/// of [`decode_native_frames`] for a damaged frame.
pub fn decode_native_frame(bytes: &[u8]) -> Result<(NativeFrame, usize), NativeFormatError> {
    let record = "frame";
    let mut reader = FieldReader::new(record, bytes);
    let magic = reader.u32()?;
    if magic != FRAME_MAGIC {
        return Err(NativeFormatError::BadMagic { record, found: magic });
    }
    let count = usize::from(reader.u16()?);
    let flags = reader.u16()?;
    let checksum = reader.u32()?;
    reader.take(4)?;
    let invalid = |field| NativeFormatError::InvalidField { record, field };
    if count == 0 || count > NATIVE_FRAME_MAX_ENTRIES {
        return Err(invalid("entry count"));
    }
    let order = match flags {
        0 => NativeFrameOrder::Arrival,
        FRAME_SORTED => NativeFrameOrder::Sorted,
        _ => return Err(invalid("flags")),
    };
    let entry_bytes = reader.take(count * NATIVE_INDEX_ENTRY_SIZE)?;
    let mut covered = Vec::with_capacity(8 + entry_bytes.len());
    covered.extend_from_slice(&bytes[..8]);
    covered.extend_from_slice(entry_bytes);
    if crc32c(&covered) != checksum {
        return Err(NativeFormatError::ChecksumMismatch { record });
    }
    let entries = entry_bytes
        .chunks_exact(NATIVE_INDEX_ENTRY_SIZE)
        .map(NativeIndexEntry::decode)
        .collect::<Result<Vec<_>, _>>()?;
    if order == NativeFrameOrder::Sorted && !is_sorted(&entries) {
        return Err(invalid("order"));
    }
    Ok((NativeFrame { order, entries }, reader.position()))
}

fn is_sorted(entries: &[NativeIndexEntry]) -> bool {
    entries.windows(2).all(|pair| pair[0].cursor() <= pair[1].cursor())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timer::native_format::NativeLocator;

    fn entry(index: i64) -> NativeIndexEntry {
        NativeIndexEntry {
            due_ms: 1_700_000_000_000 + index,
            cq_offset: index,
            locator: NativeLocator::CommitLog { offset: index * 100 },
            generation: 0,
            key_hash: index as u64,
            size: 100,
            lane: (index % 4) as u16,
        }
    }

    #[test]
    fn a_bucket_file_header_round_trips_and_checks_its_window() {
        let bucket = NativeBucketId::containing(NativeBucketKind::Minute, 1_700_000_000_000).unwrap();
        let header = NativeFileHeader { bucket };
        let bytes = header.encode();
        assert_eq!(NativeFileHeader::decode(&bytes), Ok(header));

        let mut corrupt = bytes;
        corrupt[30] ^= 1;
        assert_eq!(
            NativeFileHeader::decode(&corrupt),
            Err(NativeFormatError::ChecksumMismatch {
                record: "bucket file header"
            })
        );
        let mut foreign = bytes;
        foreign[0] = 0;
        assert!(matches!(
            NativeFileHeader::decode(&foreign),
            Err(NativeFormatError::BadMagic { .. })
        ));
    }

    #[test]
    fn entries_split_into_4_kib_frames_and_decode_back() {
        let entries: Vec<_> = (0..200).map(entry).collect();
        let mut bytes = Vec::new();
        encode_native_frames(&entries, NativeFrameOrder::Sorted, &mut bytes).unwrap();
        // 85 + 85 + 30 entries; a full frame is exactly 4 KiB.
        assert_eq!(
            bytes.len(),
            2 * 4096 + NATIVE_FRAME_HEADER_SIZE + 30 * NATIVE_INDEX_ENTRY_SIZE
        );
        let frames = decode_native_frames(&bytes).unwrap();
        assert_eq!(
            frames.iter().map(|frame| frame.entries.len()).collect::<Vec<_>>(),
            [85, 85, 30]
        );
        assert!(frames.iter().all(|frame| frame.order == NativeFrameOrder::Sorted));
        let decoded: Vec<_> = frames.into_iter().flat_map(|frame| frame.entries).collect();
        assert_eq!(decoded, entries);
        assert_eq!(decode_native_frames(&[]), Ok(Vec::new()));
    }

    #[test]
    fn damaged_or_cut_frames_are_reported() {
        let mut bytes = Vec::new();
        encode_native_frames(&[entry(1), entry(2)], NativeFrameOrder::Arrival, &mut bytes).unwrap();

        let mut corrupt = bytes.clone();
        corrupt[NATIVE_FRAME_HEADER_SIZE + 3] ^= 1;
        assert_eq!(
            decode_native_frames(&corrupt),
            Err(NativeFormatError::ChecksumMismatch { record: "frame" })
        );
        assert!(matches!(
            decode_native_frames(&bytes[..bytes.len() - 1]),
            Err(NativeFormatError::Truncated { record: "frame", .. })
        ));
        // Flags are covered by the checksum, so a flipped order bit is caught there.
        let mut reordered = bytes.clone();
        reordered[7] = 1;
        assert!(decode_native_frames(&reordered).is_err());
    }

    #[test]
    fn a_sorted_frame_must_be_sorted() {
        let unsorted = [entry(2), entry(1)];
        let mut bytes = Vec::new();
        assert_eq!(
            encode_native_frames(&unsorted, NativeFrameOrder::Sorted, &mut bytes),
            Err(NativeFormatError::InvalidField {
                record: "frame",
                field: "order"
            })
        );
        assert!(bytes.is_empty());
        encode_native_frames(&unsorted, NativeFrameOrder::Arrival, &mut bytes).unwrap();
        assert_eq!(decode_native_frames(&bytes).unwrap()[0].entries, unsorted);
    }
}
