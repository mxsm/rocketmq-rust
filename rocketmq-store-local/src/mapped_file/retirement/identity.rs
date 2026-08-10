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

use std::num::NonZeroU64;
use std::path::Path;
use std::path::PathBuf;

use thiserror::Error;

/// Stable identity for one initialized store root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StoreUuid([u8; 16]);

impl StoreUuid {
    /// Validates an opaque 128-bit store UUID.
    pub(crate) fn new(bytes: [u8; 16]) -> Result<Self, IdentityError> {
        if bytes == [0; 16] {
            return Err(IdentityError::ZeroStoreUuid);
        }
        Ok(Self(bytes))
    }

    /// Returns the UUID's exact persisted byte representation.
    pub(crate) fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Identity of one path incarnation within a store root.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct FileIncarnationId {
    store_uuid: StoreUuid,
    create_seq: NonZeroU64,
}

impl FileIncarnationId {
    /// Creates an incarnation from its store UUID and monotonic creation sequence.
    pub(crate) fn new(store_uuid: StoreUuid, create_seq: u64) -> Result<Self, IdentityError> {
        let create_seq = NonZeroU64::new(create_seq).ok_or(IdentityError::ZeroCreateSequence)?;
        Ok(Self { store_uuid, create_seq })
    }

    /// Returns the owning store UUID.
    pub(crate) fn store_uuid(self) -> StoreUuid {
        self.store_uuid
    }

    /// Returns the non-zero creation sequence as its wire value.
    pub(crate) fn create_seq(self) -> u64 {
        self.create_seq.get()
    }
}

/// Monotonic identifier for one durable retirement ticket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct TicketId(NonZeroU64);

impl TicketId {
    /// Validates a ticket identifier.
    pub(crate) fn new(value: u64) -> Result<Self, IdentityError> {
        NonZeroU64::new(value).map(Self).ok_or(IdentityError::ZeroTicketId)
    }

    /// Returns the ticket's non-zero wire value.
    pub(crate) fn get(self) -> u64 {
        self.0.get()
    }
}

/// Validated Unix physical-file identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct UnixPhysicalFileKey {
    device: u64,
    inode: u64,
}

impl UnixPhysicalFileKey {
    /// Returns the device identifier.
    pub(crate) fn device(self) -> u64 {
        self.device
    }

    /// Returns the inode identifier.
    pub(crate) fn inode(self) -> u64 {
        self.inode
    }
}

/// Validated Windows physical-file identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct WindowsPhysicalFileKey {
    volume_serial: u64,
    file_id: [u8; 16],
}

impl WindowsPhysicalFileKey {
    /// Returns the volume serial number.
    pub(crate) fn volume_serial(self) -> u64 {
        self.volume_serial
    }

    /// Returns the opaque `FILE_ID_128` bytes without endian conversion.
    pub(crate) fn file_id(self) -> [u8; 16] {
        self.file_id
    }
}

/// Platform physical-file identity observed through an open file handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum PhysicalFileKey {
    /// Unix `(st_dev, st_ino)` identity.
    Unix(UnixPhysicalFileKey),
    /// Windows `(VolumeSerialNumber, FILE_ID_128)` identity.
    Windows(WindowsPhysicalFileKey),
}

impl PhysicalFileKey {
    /// Preserves an opaque Unix physical-file identity.
    pub(crate) fn unix(device: u64, inode: u64) -> Self {
        Self::Unix(UnixPhysicalFileKey { device, inode })
    }

    /// Preserves a Windows physical-file identity without interpreting file-ID byte order.
    pub(crate) fn windows(volume_serial: u64, file_id: [u8; 16]) -> Self {
        Self::Windows(WindowsPhysicalFileKey { volume_serial, file_id })
    }
}

/// Canonical store-root-relative path persisted with `/` separators.
///
/// The representation is UTF-8 and byte-exact. Version 1 deliberately performs no Unicode
/// normalization or case folding; operating-system aliases must be rejected by handle-based
/// physical identity checks rather than string comparison.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct StoreRelativePath(Box<str>);

impl StoreRelativePath {
    /// Maximum encoded path length accepted by the retirement format.
    pub(crate) const MAX_BYTES: usize = 4096;
    /// Maximum encoded component length accepted by the retirement format.
    pub(crate) const MAX_COMPONENT_BYTES: usize = 255;

    /// Validates and owns a store-root-relative path.
    pub(crate) fn new(value: &str) -> Result<Self, IdentityError> {
        if value.is_empty() {
            return Err(IdentityError::EmptyStoreRelativePath);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(IdentityError::StoreRelativePathTooLong {
                length: value.len(),
                maximum: Self::MAX_BYTES,
            });
        }
        if value.starts_with('/') {
            return Err(IdentityError::AbsoluteStoreRelativePath);
        }
        if value.contains('\\') {
            return Err(IdentityError::StoreRelativePathContainsBackslash);
        }
        if value.contains('\0') {
            return Err(IdentityError::StoreRelativePathContainsNul);
        }
        if value.contains(':') {
            return Err(IdentityError::StoreRelativePathContainsColon);
        }
        if value.chars().any(|character| character.is_ascii_control()) {
            return Err(IdentityError::StoreRelativePathContainsAsciiControl);
        }

        for segment in value.split('/') {
            match segment {
                "" => return Err(IdentityError::EmptyStoreRelativePathSegment),
                "." => return Err(IdentityError::CurrentStoreRelativePathSegment),
                ".." => return Err(IdentityError::ParentStoreRelativePathSegment),
                _ => {}
            }
            if segment.len() > Self::MAX_COMPONENT_BYTES {
                return Err(IdentityError::StoreRelativePathComponentTooLong {
                    length: segment.len(),
                    maximum: Self::MAX_COMPONENT_BYTES,
                });
            }
            if segment.ends_with('.') || segment.ends_with(' ') {
                return Err(IdentityError::StoreRelativePathComponentHasWindowsTrimSuffix);
            }
            let device_stem = segment.split('.').next().unwrap_or(segment);
            if is_windows_reserved_device_name(device_stem) {
                return Err(IdentityError::WindowsReservedStoreRelativePathComponent);
            }
        }

        Ok(Self(value.into()))
    }

    /// Returns the exact canonical UTF-8 representation.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the exact canonical wire bytes.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Verifies that the final path component is the v1 numeric segment name.
    pub(crate) fn validate_segment_binding(&self, segment_offset: u64) -> Result<(), IdentityError> {
        let expected = format!("{segment_offset:020}");
        if self.basename() != expected {
            return Err(IdentityError::CanonicalSegmentPathIdentityMismatch);
        }
        Ok(())
    }

    /// Verifies the same-directory v1 create name against all persisted identity fields.
    pub(crate) fn validate_create_binding(
        &self,
        create_file_path: &Self,
        incarnation: FileIncarnationId,
        segment_offset: u64,
        create_nonce: &[u8; 16],
    ) -> Result<(), IdentityError> {
        self.validate_segment_binding(segment_offset)?;
        let nonce = encode_lower_hex(create_nonce);
        let expected = format!(
            ".create.i{:016x}.s{segment_offset:020}.n{nonce}",
            incarnation.create_seq()
        );
        if !self.is_sibling_named(create_file_path, &expected) {
            return Err(IdentityError::CreateFilePathIdentityMismatch);
        }
        Ok(())
    }

    /// Derives the only valid same-directory v1 create-file path for this segment.
    pub(crate) fn create_file_path(
        &self,
        incarnation: FileIncarnationId,
        segment_offset: u64,
        create_nonce: &[u8; 16],
    ) -> Result<Self, IdentityError> {
        self.validate_segment_binding(segment_offset)?;
        let nonce = encode_lower_hex(create_nonce);
        let basename = format!(
            ".create.i{:016x}.s{segment_offset:020}.n{nonce}",
            incarnation.create_seq()
        );
        let raw = match self.0.rsplit_once('/') {
            Some((parent, _)) => format!("{parent}/{basename}"),
            None => basename,
        };
        Self::new(&raw)
    }

    /// Verifies the same-directory v1 tombstone name against every deletion authority field.
    pub(crate) fn validate_tombstone_binding(
        &self,
        tombstone_path: &Self,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        segment_offset: u64,
        mapping_generation: u64,
        retirement_nonce: &[u8; 16],
    ) -> Result<(), IdentityError> {
        self.validate_segment_binding(segment_offset)?;
        let expected = tombstone_basename(
            ticket_id,
            incarnation,
            segment_offset,
            mapping_generation,
            retirement_nonce,
        );
        if !self.is_sibling_named(tombstone_path, &expected) {
            return Err(IdentityError::TombstonePathIdentityMismatch);
        }
        Ok(())
    }

    /// Derives the only valid same-directory v1 tombstone path for this segment.
    pub(crate) fn tombstone_path(
        &self,
        ticket_id: TicketId,
        incarnation: FileIncarnationId,
        segment_offset: u64,
        mapping_generation: u64,
        retirement_nonce: &[u8; 16],
    ) -> Result<Self, IdentityError> {
        self.validate_segment_binding(segment_offset)?;
        let basename = tombstone_basename(
            ticket_id,
            incarnation,
            segment_offset,
            mapping_generation,
            retirement_nonce,
        );
        let raw = match self.0.rsplit_once('/') {
            Some((parent, _)) => format!("{parent}/{basename}"),
            None => basename,
        };
        Self::new(&raw)
    }

    /// Appends validated path segments below `root` without platform prefix reinterpretation.
    ///
    /// This is a lexical containment operation. Symlink and junction containment is established
    /// later while opening each component under the reserved store-root handle.
    pub(crate) fn join_under(&self, root: &Path) -> PathBuf {
        let mut joined = root.to_path_buf();
        for segment in self.0.split('/') {
            joined.push(segment);
        }
        joined
    }

    fn basename(&self) -> &str {
        self.0
            .rsplit_once('/')
            .map_or(self.0.as_ref(), |(_, basename)| basename)
    }

    fn is_sibling_named(&self, candidate: &Self, expected_basename: &str) -> bool {
        match self.0.rsplit_once('/') {
            Some((parent, _)) => candidate
                .0
                .strip_prefix(parent)
                .and_then(|suffix| suffix.strip_prefix('/'))
                .is_some_and(|basename| basename == expected_basename && !basename.contains('/')),
            None => candidate.0.as_ref() == expected_basename,
        }
    }
}

fn tombstone_basename(
    ticket_id: TicketId,
    incarnation: FileIncarnationId,
    segment_offset: u64,
    mapping_generation: u64,
    retirement_nonce: &[u8; 16],
) -> String {
    let nonce = encode_lower_hex(retirement_nonce);
    format!(
        ".delete.t{:016x}.i{:016x}.s{segment_offset:020}.m{mapping_generation:016x}.n{nonce}",
        ticket_id.get(),
        incarnation.create_seq()
    )
}

fn encode_lower_hex(value: &[u8; 16]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in value {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn is_windows_reserved_device_name(component: &str) -> bool {
    if component.eq_ignore_ascii_case("CON")
        || component.eq_ignore_ascii_case("PRN")
        || component.eq_ignore_ascii_case("AUX")
        || component.eq_ignore_ascii_case("NUL")
    {
        return true;
    }

    let bytes = component.as_bytes();
    bytes.len() == 4
        && matches!(bytes[3], b'1'..=b'9')
        && (component[..3].eq_ignore_ascii_case("COM") || component[..3].eq_ignore_ascii_case("LPT"))
}

/// Validation failure for a retirement identity value.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum IdentityError {
    /// Store UUID was the reserved all-zero value.
    #[error("store UUID must not be all zero")]
    ZeroStoreUuid,
    /// Incarnation creation sequence was zero.
    #[error("file incarnation creation sequence must be non-zero")]
    ZeroCreateSequence,
    /// Retirement ticket identifier was zero.
    #[error("retirement ticket identifier must be non-zero")]
    ZeroTicketId,
    /// Store-relative path was empty.
    #[error("store-relative path must not be empty")]
    EmptyStoreRelativePath,
    /// Store-relative path exceeded the encoded byte limit.
    #[error("store-relative path is too long: length={length}, maximum={maximum}")]
    StoreRelativePathTooLong {
        /// Actual UTF-8 byte length.
        length: usize,
        /// Maximum accepted UTF-8 byte length.
        maximum: usize,
    },
    /// Store-relative path began at a filesystem root.
    #[error("store-relative path must not be absolute")]
    AbsoluteStoreRelativePath,
    /// Store-relative path used a non-canonical platform separator or prefix.
    #[error("store-relative path must not contain a backslash")]
    StoreRelativePathContainsBackslash,
    /// Store-relative path contained a NUL byte.
    #[error("store-relative path must not contain NUL")]
    StoreRelativePathContainsNul,
    /// Store-relative path contained a Windows drive, prefix, or alternate-stream delimiter.
    #[error("store-relative path must not contain a colon")]
    StoreRelativePathContainsColon,
    /// Store-relative path contained an ASCII control character.
    #[error("store-relative path must not contain an ASCII control character")]
    StoreRelativePathContainsAsciiControl,
    /// Store-relative path contained an empty component.
    #[error("store-relative path must not contain an empty segment")]
    EmptyStoreRelativePathSegment,
    /// Store-relative path contained a current-directory component.
    #[error("store-relative path must not contain a '.' segment")]
    CurrentStoreRelativePathSegment,
    /// Store-relative path contained a parent-directory component.
    #[error("store-relative path must not contain a '..' segment")]
    ParentStoreRelativePathSegment,
    /// Store-relative path component exceeded the encoded byte limit.
    #[error("store-relative path component is too long: length={length}, maximum={maximum}")]
    StoreRelativePathComponentTooLong {
        /// Actual UTF-8 byte length.
        length: usize,
        /// Maximum accepted UTF-8 byte length.
        maximum: usize,
    },
    /// Store-relative path component would be trimmed by Windows namespace rules.
    #[error("store-relative path component must not end in a dot or space")]
    StoreRelativePathComponentHasWindowsTrimSuffix,
    /// Store-relative path contained a Windows reserved device component.
    #[error("store-relative path must not contain a Windows reserved device component")]
    WindowsReservedStoreRelativePathComponent,
    /// Canonical segment basename did not encode its persisted segment offset.
    #[error("canonical segment path does not encode its persisted segment offset")]
    CanonicalSegmentPathIdentityMismatch,
    /// Create path did not encode its incarnation, segment offset, nonce, or parent directory.
    #[error("create-file path does not match its persisted identity fields")]
    CreateFilePathIdentityMismatch,
    /// Tombstone path did not encode its ticket, incarnation, generation, nonce, or parent directory.
    #[error("tombstone path does not match its persisted identity fields")]
    TombstonePathIdentityMismatch,
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hash;
    use std::hash::Hasher;
    use std::path::Path;

    use super::*;

    fn hash_of(value: &impl Hash) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn store_uuid(last_byte: u8) -> StoreUuid {
        let mut bytes = [0; 16];
        bytes[15] = last_byte;
        StoreUuid::new(bytes).expect("test UUID is non-zero")
    }

    #[test]
    fn store_uuid_rejects_zero_and_preserves_opaque_bytes() {
        assert_eq!(StoreUuid::new([0; 16]), Err(IdentityError::ZeroStoreUuid));

        let expected = [
            0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
        ];
        let uuid = StoreUuid::new(expected).expect("non-zero UUID is valid");
        assert_eq!(uuid.as_bytes(), &expected);
    }

    #[test]
    fn file_incarnation_rejects_zero_sequence_and_has_value_semantics() {
        let uuid = store_uuid(1);
        assert_eq!(FileIncarnationId::new(uuid, 0), Err(IdentityError::ZeroCreateSequence));

        let first = FileIncarnationId::new(uuid, 1).expect("positive sequence is valid");
        let same = FileIncarnationId::new(uuid, 1).expect("positive sequence is valid");
        let next = FileIncarnationId::new(uuid, 2).expect("positive sequence is valid");
        assert_eq!(first.store_uuid(), uuid);
        assert_eq!(first.create_seq(), 1);
        assert_eq!(first, same);
        assert_eq!(hash_of(&first), hash_of(&same));
        assert_ne!(first, next);
        assert_ne!(hash_of(&first), hash_of(&next));
    }

    #[test]
    fn ticket_id_rejects_zero_and_accepts_full_nonzero_range() {
        assert_eq!(TicketId::new(0), Err(IdentityError::ZeroTicketId));

        let first = TicketId::new(1).expect("one is valid");
        let max = TicketId::new(u64::MAX).expect("maximum non-zero value is valid");
        assert_eq!(first.get(), 1);
        assert_eq!(max.get(), u64::MAX);
        assert_eq!(first, TicketId::new(1).expect("one remains valid"));
        assert_eq!(hash_of(&first), hash_of(&TicketId::new(1).expect("one remains valid")));
        assert_ne!(first, max);
    }

    #[test]
    fn unix_physical_key_preserves_opaque_fields_including_zero() {
        let key = PhysicalFileKey::unix(0, 7);
        let PhysicalFileKey::Unix(key) = key else {
            panic!("Unix constructor must create the Unix variant");
        };
        assert_eq!(key.device(), 0);
        assert_eq!(key.inode(), 7);
    }

    #[test]
    fn windows_physical_key_preserves_opaque_fields_including_zero() {
        let file_id = [0; 16];
        let key = PhysicalFileKey::windows(0, file_id);
        let PhysicalFileKey::Windows(key) = key else {
            panic!("Windows constructor must create the Windows variant");
        };
        assert_eq!(key.volume_serial(), 0);
        assert_eq!(key.file_id(), file_id);
    }

    #[test]
    fn physical_keys_have_platform_independent_equality_and_hashing() {
        let unix = PhysicalFileKey::unix(3, 11);
        let same_unix = PhysicalFileKey::unix(3, 11);
        let other_unix = PhysicalFileKey::unix(3, 12);
        let mut file_id = [0; 16];
        file_id[15] = 11;
        let windows = PhysicalFileKey::windows(3, file_id);

        assert_eq!(unix, same_unix);
        assert_eq!(hash_of(&unix), hash_of(&same_unix));
        assert_ne!(unix, other_unix);
        assert_ne!(unix, windows);
    }

    #[test]
    fn store_relative_path_preserves_canonical_utf8_slash_bytes() {
        let path =
            StoreRelativePath::new("commitlog/主题/00000000000000000000").expect("canonical UTF-8 path is valid");
        assert_eq!(path.as_str(), "commitlog/主题/00000000000000000000");
        assert_eq!(path.as_bytes(), "commitlog/主题/00000000000000000000".as_bytes());
    }

    #[test]
    fn store_relative_path_enforces_byte_length_boundaries() {
        let maximum = std::iter::repeat_n("a".repeat(240), 17).collect::<Vec<_>>().join("/");
        let too_long = format!("{maximum}a");
        assert_eq!(maximum.len(), StoreRelativePath::MAX_BYTES);
        assert!(StoreRelativePath::new(&maximum).is_ok());
        assert_eq!(
            StoreRelativePath::new(&too_long),
            Err(IdentityError::StoreRelativePathTooLong {
                length: StoreRelativePath::MAX_BYTES + 1,
                maximum: StoreRelativePath::MAX_BYTES,
            })
        );

        let maximum_component = "界".repeat(StoreRelativePath::MAX_COMPONENT_BYTES / "界".len());
        assert_eq!(maximum_component.len(), 255);
        assert!(StoreRelativePath::new(&maximum_component).is_ok());
        let too_long_component = format!("{maximum_component}界");
        assert_eq!(
            StoreRelativePath::new(&too_long_component),
            Err(IdentityError::StoreRelativePathComponentTooLong {
                length: 258,
                maximum: StoreRelativePath::MAX_COMPONENT_BYTES,
            })
        );
    }

    #[test]
    fn store_relative_path_rejects_noncanonical_or_unsafe_forms() {
        let invalid = [
            ("", IdentityError::EmptyStoreRelativePath),
            ("/commitlog/1", IdentityError::AbsoluteStoreRelativePath),
            ("//server/share", IdentityError::AbsoluteStoreRelativePath),
            ("\\server\\share", IdentityError::StoreRelativePathContainsBackslash),
            ("commitlog\\1", IdentityError::StoreRelativePathContainsBackslash),
            ("commit\0log/1", IdentityError::StoreRelativePathContainsNul),
            ("C:/store/file", IdentityError::StoreRelativePathContainsColon),
            ("c:store/file", IdentityError::StoreRelativePathContainsColon),
            ("stream:name", IdentityError::StoreRelativePathContainsColon),
            (
                "commitlog/line\nfeed",
                IdentityError::StoreRelativePathContainsAsciiControl,
            ),
            ("commitlog//1", IdentityError::EmptyStoreRelativePathSegment),
            ("commitlog/", IdentityError::EmptyStoreRelativePathSegment),
            (".", IdentityError::CurrentStoreRelativePathSegment),
            ("commitlog/./1", IdentityError::CurrentStoreRelativePathSegment),
            ("..", IdentityError::ParentStoreRelativePathSegment),
            ("commitlog/../1", IdentityError::ParentStoreRelativePathSegment),
            (
                "commitlog/trailing.",
                IdentityError::StoreRelativePathComponentHasWindowsTrimSuffix,
            ),
            (
                "commitlog/trailing ",
                IdentityError::StoreRelativePathComponentHasWindowsTrimSuffix,
            ),
            (
                "commitlog/CON",
                IdentityError::WindowsReservedStoreRelativePathComponent,
            ),
            (
                "commitlog/com1.bin",
                IdentityError::WindowsReservedStoreRelativePathComponent,
            ),
            (
                "commitlog/Lpt9",
                IdentityError::WindowsReservedStoreRelativePathComponent,
            ),
        ];

        for (raw, expected) in invalid {
            assert_eq!(StoreRelativePath::new(raw), Err(expected), "raw={raw:?}");
        }
    }

    #[test]
    fn store_relative_path_allows_non_reserved_device_prefixes() {
        for raw in [
            "commitlog/COM0",
            "commitlog/COM10",
            "commitlog/CONSOLE",
            "commitlog/LPT0.bin",
        ] {
            assert!(StoreRelativePath::new(raw).is_ok(), "raw={raw:?}");
        }
    }

    #[test]
    fn store_relative_path_equality_and_hash_use_exact_canonical_bytes() {
        let first = StoreRelativePath::new("consumequeue/topic/0").expect("valid path");
        let same = StoreRelativePath::new("consumequeue/topic/0").expect("valid path");
        let case_alias = StoreRelativePath::new("consumequeue/Topic/0").expect("valid path");

        assert_eq!(first, same);
        assert_eq!(hash_of(&first), hash_of(&same));
        assert_ne!(first, case_alias);
        assert_ne!(hash_of(&first), hash_of(&case_alias));
    }

    #[test]
    fn join_under_appends_validated_segments_without_platform_reinterpretation() {
        let root = Path::new("store-root");
        let relative = StoreRelativePath::new("commitlog/00000000000000000000").expect("valid path");
        let joined = relative.join_under(root);

        assert_eq!(joined, root.join("commitlog").join("00000000000000000000"));
        assert!(joined.starts_with(root));
    }

    #[test]
    fn lifecycle_paths_bind_exactly_to_their_persisted_identity_fields() {
        let uuid = store_uuid(1);
        let incarnation = FileIncarnationId::new(uuid, 7).expect("test incarnation is nonzero");
        let ticket = TicketId::new(42).expect("test ticket is nonzero");
        let create_nonce = std::array::from_fn(|index| 0x20 + index as u8);
        let retirement_nonce = std::array::from_fn(|index| 0x40 + index as u8);
        let canonical = StoreRelativePath::new("commitlog/00000000000000000000").expect("canonical path is valid");
        let create = StoreRelativePath::new(
            "commitlog/.create.i0000000000000007.s00000000000000000000.n202122232425262728292a2b2c2d2e2f",
        )
        .expect("create path is valid");
        let tombstone = StoreRelativePath::new(
            "commitlog/.delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000003.n404142434445464748494a4b4c4d4e4f",
        )
        .expect("tombstone path is valid");

        assert_eq!(canonical.validate_segment_binding(0), Ok(()));
        assert_eq!(
            canonical.validate_create_binding(&create, incarnation, 0, &create_nonce),
            Ok(())
        );
        assert_eq!(
            canonical.create_file_path(incarnation, 0, &create_nonce),
            Ok(create.clone())
        );
        assert_eq!(
            canonical.validate_tombstone_binding(&tombstone, ticket, incarnation, 0, 3, &retirement_nonce),
            Ok(())
        );
        assert_eq!(
            canonical.tombstone_path(ticket, incarnation, 0, 3, &retirement_nonce),
            Ok(tombstone.clone())
        );

        let wrong_directory = StoreRelativePath::new(
            "consumequeue/.create.i0000000000000007.s00000000000000000000.n202122232425262728292a2b2c2d2e2f",
        )
        .expect("wrong-directory path is still lexically valid");
        assert_eq!(
            canonical.validate_create_binding(&wrong_directory, incarnation, 0, &create_nonce),
            Err(IdentityError::CreateFilePathIdentityMismatch)
        );
        assert_eq!(
            canonical.validate_segment_binding(1),
            Err(IdentityError::CanonicalSegmentPathIdentityMismatch)
        );
        assert_eq!(
            canonical.validate_tombstone_binding(&tombstone, ticket, incarnation, 0, 4, &retirement_nonce),
            Err(IdentityError::TombstonePathIdentityMismatch)
        );
    }
}
