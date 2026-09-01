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

use std::fs::File;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::OnceLock;

use crate::mapped_file::kernel::ReferenceResource;
use crate::mapped_file::DefaultMappedFile;
use crate::mapped_file::MappedFile;
use crate::mapped_file::MappedFileAdmissionState;
use crate::mapped_file::MappedFileError;
use crate::mapped_file::MappedMemory;
use crate::mapped_file::MappedWriteLease;
use crate::mapped_file::NativeMappedMemory;
use crate::mapped_file::ReadOnlyMappedMemory;
use cheetah_string::CheetahString;
use memmap2::Mmap;
use memmap2::MmapMut;

fn mapped_path(directory: &tempfile::TempDir) -> CheetahString {
    CheetahString::from(
        directory
            .path()
            .join("00000000000000000000")
            .to_string_lossy()
            .into_owned(),
    )
}

fn eager_file(size: u64) -> (tempfile::TempDir, Arc<DefaultMappedFile>) {
    let directory = tempfile::tempdir().expect("temporary mapped-file directory");
    let mapped_file =
        DefaultMappedFile::<NativeMappedMemory>::try_new(mapped_path(&directory), size).expect("eager mapped file");
    (directory, Arc::new(mapped_file))
}

#[test]
fn normal_shutdown_detaches_eager_mapping_and_file_owner_exactly_once() {
    let (directory, mapped_file) = eager_file(16);
    let path = directory.path().join("00000000000000000000");
    let metrics = mapped_file.get_metrics().expect("mapped-file metrics");
    assert_eq!(metrics.mapped_generations_live(), 1);
    assert_eq!(metrics.mapped_bytes_live(), 16);
    assert_eq!(metrics.file_owners_live(), 1);

    MappedFile::shutdown(mapped_file.as_ref(), 0);

    assert!(path.exists(), "normal shutdown must not remove the namespace");
    assert!(!mapped_file.is_mapped());
    assert_eq!(metrics.mapped_generations_live(), 0);
    assert_eq!(metrics.mapped_bytes_live(), 0);
    assert_eq!(metrics.file_owners_live(), 0);
    assert_eq!(metrics.physical_mapping_drop_total(), 1);
    assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    assert_eq!(metrics.lifecycle_detach_total(), 1);
    assert!(ReferenceResource::is_cleanup_over(mapped_file.as_ref()));

    MappedFile::shutdown(mapped_file.as_ref(), 0);
    assert!(mapped_file.try_detach_physical_owners().is_detached());
    assert_eq!(metrics.physical_mapping_drop_total(), 1);
    assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    assert_eq!(metrics.lifecycle_detach_total(), 1);
}

#[test]
fn normal_shutdown_of_never_mapped_lazy_file_closes_only_the_file_owner() {
    let directory = tempfile::tempdir().expect("temporary mapped-file directory");
    let path = directory.path().join("00000000000000000000");
    let mapped_file = DefaultMappedFile::<NativeMappedMemory>::try_new_lazy_read_only(mapped_path(&directory), 16)
        .expect("lazy mapped file");
    let metrics = mapped_file.get_metrics().expect("mapped-file metrics");
    assert!(!mapped_file.is_mapped());
    assert_eq!(metrics.mapped_generations_live(), 0);
    assert_eq!(metrics.file_owners_live(), 1);

    MappedFile::shutdown(&mapped_file, 0);

    assert!(path.exists(), "normal shutdown must preserve the namespace");
    assert!(!mapped_file.is_mapped());
    assert_eq!(metrics.mapped_generations_live(), 0);
    assert_eq!(metrics.physical_mapping_drop_total(), 0);
    assert_eq!(metrics.file_owners_live(), 0);
    assert_eq!(metrics.physical_file_owner_drop_total(), 1);
    assert_eq!(metrics.lifecycle_detach_total(), 1);
}

#[test]
fn final_mapped_read_alias_drop_releases_physical_owners_and_unblocks_delete() {
    let (directory, mapped_file) = eager_file(32);
    let path = directory.path().join("00000000000000000000");
    assert!(mapped_file.append_message_bytes(b"sealed-data"));
    assert!(mapped_file.try_seal_readable().expect("seal mapped file"));
    let metrics = mapped_file.get_metrics().expect("mapped-file metrics");
    assert_eq!(metrics.mapped_generations_live(), 1);
    assert_eq!(metrics.file_owners_live(), 1);

    let lease = mapped_file
        .try_mapped_read_lease(0, b"sealed-data".len())
        .expect("read admission")
        .expect("sealed generation");
    let lease_clone = lease.clone();
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);

    MappedFile::shutdown(mapped_file.as_ref(), 0);
    assert!(path.exists());
    assert!(mapped_file.is_mapped());
    assert_eq!(metrics.mapped_generations_live(), 1);
    assert_eq!(metrics.file_owners_live(), 1);
    assert!(!mapped_file.try_destroy(0).is_namespace_removed());
    assert_eq!(lease.as_ref(), b"sealed-data");

    drop(lease);
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
    assert_eq!(lease_clone.as_ref(), b"sealed-data");
    drop(lease_clone);

    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
    assert!(!mapped_file.is_mapped());
    assert_eq!(metrics.mapped_generations_live(), 0);
    assert_eq!(metrics.file_owners_live(), 0);
    assert_eq!(metrics.lifecycle_detach_total(), 1);
    assert!(mapped_file.try_destroy(0).is_namespace_removed());
    assert!(!path.exists());
}

#[test]
fn active_writable_generation_never_exposes_a_cross_call_mapped_slice() {
    let (_directory, mapped_file) = eager_file(16);
    assert!(mapped_file.append_message_bytes(b"data"));

    assert!(mapped_file
        .try_mapped_read_lease(0, 4)
        .expect("active reads remain admitted")
        .is_none());
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 0);
}

#[test]
fn seal_waits_for_admitted_writer_then_rejects_new_writes() {
    let (_directory, mapped_file) = eager_file(16);
    let mut write = mapped_file.reserve_write(4).expect("write admitted before seal");
    write.buffer_mut().copy_from_slice(b"data");
    let (sealed_tx, sealed_rx) = mpsc::sync_channel(1);

    std::thread::scope(|scope| {
        let seal_target = Arc::clone(&mapped_file);
        scope.spawn(move || {
            sealed_tx
                .send(seal_target.try_seal_readable())
                .expect("report seal result");
        });

        for _ in 0..10_000 {
            if mapped_file.lifecycle_snapshot().state == MappedFileAdmissionState::SealedReadable {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(
            mapped_file.lifecycle_snapshot().state,
            MappedFileAdmissionState::SealedReadable
        );
        assert!(matches!(sealed_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(write.commit(4, None).expect("admitted writer completes"), 4);
        assert!(sealed_rx.recv().expect("seal worker completes").expect("seal succeeds"));
    });

    let write_error = match mapped_file.reserve_write(1) {
        Ok(_) => panic!("sealed or closing lifecycle rejects writers"),
        Err(error) => error,
    };
    assert!(matches!(
        std::error::Error::source(&write_error).and_then(|source| source.downcast_ref::<MappedFileError>()),
        Some(MappedFileError::Unavailable {
            state: MappedFileAdmissionState::SealedReadable,
            ..
        })
    ));
    let read = mapped_file
        .try_mapped_read_lease(0, 4)
        .expect("read admission")
        .expect("read-only generation");
    assert_eq!(read.as_ref(), b"data");
}

struct LazyRaceControl {
    initializer_started: Barrier,
    release_initializer: Barrier,
    candidate_drops: AtomicUsize,
}

static LAZY_RACE_CONTROL: OnceLock<Arc<LazyRaceControl>> = OnceLock::new();

struct BlockingMappedMemory(MmapMut);
struct BlockingReadOnlyMappedMemory(Mmap);

impl Drop for BlockingMappedMemory {
    fn drop(&mut self) {
        if let Some(control) = LAZY_RACE_CONTROL.get() {
            control.candidate_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}

// SAFETY: the mapping remains stable for the value lifetime and DefaultMappedFile serializes all
// mutation. The barriers only delay construction before publication.
unsafe impl MappedMemory for BlockingMappedMemory {
    type ReadOnly = BlockingReadOnlyMappedMemory;

    unsafe fn map_mut(file: &File) -> io::Result<Self> {
        let control = LAZY_RACE_CONTROL.get().expect("lazy race control installed");
        control.initializer_started.wait();
        control.release_initializer.wait();
        // SAFETY: DefaultMappedFile keeps the segment sized and owned for the candidate lifetime.
        unsafe { MmapMut::map_mut(file).map(Self) }
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }

    fn as_mut_ptr(&self) -> *mut u8 {
        self.0.as_ptr().cast_mut()
    }

    fn flush(&self) -> io::Result<()> {
        self.0.flush()
    }

    fn flush_range(&self, offset: usize, len: usize) -> io::Result<()> {
        self.0.flush_range(offset, len)
    }
}

// SAFETY: this mapping is immutable, stable, and exposes no mutation path.
unsafe impl ReadOnlyMappedMemory for BlockingReadOnlyMappedMemory {
    unsafe fn map(file: &File) -> io::Result<Self> {
        // SAFETY: DefaultMappedFile keeps the segment stable while this generation is live.
        unsafe { Mmap::map(file).map(Self) }
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

#[test]
fn close_winning_lazy_publication_drops_candidate_and_never_late_publishes() {
    let control = Arc::new(LazyRaceControl {
        initializer_started: Barrier::new(2),
        release_initializer: Barrier::new(2),
        candidate_drops: AtomicUsize::new(0),
    });
    LAZY_RACE_CONTROL
        .set(Arc::clone(&control))
        .unwrap_or_else(|_| panic!("lazy race test control installed more than once"));
    let directory = tempfile::tempdir().expect("temporary mapped-file directory");
    let mapped_file = Arc::new(
        DefaultMappedFile::<BlockingMappedMemory>::try_new_lazy_read_only(mapped_path(&directory), 16)
            .expect("lazy mapped file"),
    );

    let worker = {
        let mapped_file = Arc::clone(&mapped_file);
        std::thread::spawn(move || mapped_file.with_mapped_slice(<[u8]>::len))
    };
    control.initializer_started.wait();
    MappedFile::shutdown(mapped_file.as_ref(), 0);
    assert_eq!(
        mapped_file.lifecycle_snapshot().state,
        MappedFileAdmissionState::Closing
    );
    assert_eq!(mapped_file.lifecycle_snapshot().active_leases, 1);
    control.release_initializer.wait();

    assert!(matches!(
        worker.join().expect("lazy initializer thread"),
        Err(MappedFileError::Unavailable {
            state: MappedFileAdmissionState::Closing,
            ..
        })
    ));
    assert!(!mapped_file.is_mapped());
    assert_eq!(control.candidate_drops.load(Ordering::SeqCst), 1);
    let metrics = mapped_file.get_metrics().expect("mapped-file metrics");
    assert_eq!(metrics.mapped_generations_live(), 0);
    assert_eq!(metrics.file_owners_live(), 0);
    assert_eq!(metrics.lifecycle_detach_total(), 1);
    assert_eq!(mapped_file.lazy_mmap_stats().map_operations, 0);
    assert_eq!(mapped_file.lazy_mmap_stats().map_failures, 1);
}
