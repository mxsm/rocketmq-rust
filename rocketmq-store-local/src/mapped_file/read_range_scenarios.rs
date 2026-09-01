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

use std::sync::Arc;

use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::NativeMappedMemory;
use crate::mapped_file::SelectMappedBufferSourceKind;
use bytes::Bytes;
use cheetah_string::CheetahString;

type NativeMappedFile = DefaultMappedFile<NativeMappedMemory>;

fn mapped_file(file_from_offset: u64) -> (tempfile::TempDir, Arc<NativeMappedFile>) {
    let directory = tempfile::tempdir().expect("temporary mapped-file directory");
    let path = directory.path().join(format!("{file_from_offset:020}"));
    let mapped_file =
        NativeMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64).expect("mapped file");
    (directory, Arc::new(mapped_file))
}

#[test]
fn mapped_read_range_keeps_retirement_fenced_until_the_final_alias_drops() {
    let (_directory, mapped_file) = mapped_file(128);
    assert!(mapped_file.append_message_bytes(b"owner-backed"));
    assert!(mapped_file.try_seal_readable().expect("seal mapped file"));

    let range = mapped_file
        .try_mapped_read_range(0, b"owner-backed".len())
        .expect("range selection")
        .expect("sealed range");
    let (left, right) = range.split_at(5).expect("checked split");
    let right_alias = right.clone();

    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
    MappedFile::shutdown(mapped_file.as_ref(), u64::MAX);
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
    assert_eq!(left.with_slice(<[u8]>::to_vec), b"owner");
    assert_eq!(right.with_slice(<[u8]>::to_vec), b"-backed");

    drop(range);
    drop(left);
    drop(right);
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
    drop(right_alias);
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
    assert!(mapped_file.lifecycle_snapshot().logical_cleanup_marked);
}

#[test]
fn mapped_read_range_retains_its_generation_across_a_mapping_swap() {
    let (_directory, mapped_file) = mapped_file(256);
    assert!(mapped_file.append_message_bytes(b"generation"));
    assert!(mapped_file.try_seal_readable().expect("seal mapped file"));

    let before = mapped_file
        .try_mapped_read_range(0, b"generation".len())
        .expect("range selection")
        .expect("sealed range");
    let before_generation = before.generation_id();
    assert!(mapped_file.swap_map());
    let after = mapped_file
        .try_mapped_read_range(0, b"generation".len())
        .expect("range selection")
        .expect("replacement range");

    assert_ne!(before_generation, after.generation_id());
    assert_eq!(before.with_slice(<[u8]>::to_vec), b"generation");
    assert_eq!(after.with_slice(<[u8]>::to_vec), b"generation");
}

#[test]
fn mapped_read_range_checks_readable_bounds_and_derived_coordinates() {
    let (_directory, mapped_file) = mapped_file(512);
    assert!(mapped_file.append_message_bytes(b"0123456789"));
    assert!(mapped_file
        .try_mapped_read_range(0, 10)
        .expect("active selection")
        .is_none());
    assert!(mapped_file.try_seal_readable().expect("seal mapped file"));
    assert!(mapped_file
        .try_mapped_read_range(8, 3)
        .expect("bounded selection")
        .is_none());

    let range = mapped_file
        .try_mapped_read_range(2, 6)
        .expect("range selection")
        .expect("sealed range");
    assert_eq!(range.start_offset(), 514);
    assert_eq!(range.file_from_offset(), 512);
    assert_eq!(range.file_offset(), 2);
    assert_eq!(range.len(), 6);
    assert!(!range.is_empty());
    assert_eq!(range.to_bytes().as_ref(), b"234567");

    let middle = range.slice(2, 3).expect("checked subrange");
    assert_eq!(middle.start_offset(), 516);
    assert_eq!(middle.file_offset(), 4);
    assert_eq!(middle.with_slice(<[u8]>::to_vec), b"456");
    assert!(range.slice(5, 2).is_none());
    assert!(range.slice(usize::MAX, 1).is_none());
    assert!(range.split_at(7).is_none());
}

#[test]
fn sealed_selection_is_range_backed_until_owned_bytes_are_requested() {
    let (_directory, mapped_file) = mapped_file(640);
    assert!(mapped_file.append_message_bytes(b"range-first"));
    assert!(mapped_file.try_seal_readable().expect("seal mapped file"));

    let mut selected = mapped_file
        .select_mapped_buffer(0, b"range-first".len() as i32)
        .expect("sealed selection");
    assert_eq!(selected.source_kind(), SelectMappedBufferSourceKind::MappedFile);
    assert!(selected.is_range_backed());
    assert!(!selected.has_byte_snapshot());
    assert_eq!(selected.get_buffer(), b"range-first");
    assert!(!selected.has_byte_snapshot());

    assert_eq!(selected.get_bytes_ref().map(Bytes::as_ref), Some(&b"range-first"[..]));
    assert!(selected.has_byte_snapshot());
    *selected.bytes_mut().expect("materialized compatibility bytes") = Bytes::from_static(b"Range-first");
    assert!(!selected.is_range_backed());
    assert_eq!(selected.source_kind(), SelectMappedBufferSourceKind::Bytes);
    assert_eq!(selected.get_buffer(), b"Range-first");
}

#[test]
fn range_selection_truncates_the_range_and_cached_compatibility_snapshot_together() {
    let (_directory, mapped_file) = mapped_file(768);
    assert!(mapped_file.append_message_bytes(b"truncate-range"));
    assert!(mapped_file.try_seal_readable().expect("seal mapped file"));

    let mut selected = mapped_file
        .select_mapped_buffer(0, b"truncate-range".len() as i32)
        .expect("sealed selection");
    assert_eq!(
        selected.get_bytes_ref().map(Bytes::as_ref),
        Some(&b"truncate-range"[..])
    );
    assert!(selected.try_truncate(8));

    assert_eq!(selected.size(), 8);
    assert_eq!(selected.get_buffer(), b"truncate");
    assert_eq!(selected.get_bytes_ref().map(Bytes::as_ref), Some(&b"truncate"[..]));
}

#[test]
fn mapped_read_range_converts_to_a_file_range_without_releasing_admission() {
    let (_directory, mapped_file) = mapped_file(896);
    assert!(mapped_file.append_message_bytes(b"file-range"));
    assert!(mapped_file.try_seal_readable().expect("seal mapped file"));

    let range = mapped_file
        .try_mapped_read_range(2, 4)
        .expect("range selection")
        .expect("sealed range");
    let file_range = range.try_into_file_range().expect("checked file range");
    assert_eq!(file_range.position(), 2);
    assert_eq!(file_range.len(), 4);

    MappedFile::shutdown(mapped_file.as_ref(), u64::MAX);
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
    drop(file_range);
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
}
