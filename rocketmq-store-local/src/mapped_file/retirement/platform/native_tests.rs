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
use std::fs::OpenOptions;

use tempfile::TempDir;

#[cfg(any(target_os = "linux", windows))]
use super::apply_namespace_transition;
use super::physical_file_key;
#[cfg(any(target_os = "linux", windows))]
use super::NamespaceMutationAuthorization;
use super::NamespaceRetirementRequest;
use super::NamespaceTicketBinding;
use super::NamespaceTransition;
#[cfg(any(target_os = "linux", windows))]
use super::NamespaceTransitionOutcome;
use super::VerifiedNamespaceRoot;
use crate::mapped_file::retirement::codec::RetirementReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::identity::TicketId;
#[cfg(any(target_os = "linux", windows))]
use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;
#[cfg(any(target_os = "linux", windows))]
use crate::mapped_file::retirement::writer::{IncarnationAllocationPlan, ManagedLedgerWriter};

fn store_uuid() -> StoreUuid {
    StoreUuid::new([1; 16]).expect("test Store UUID is valid")
}

fn request(physical_key: PhysicalFileKey) -> NamespaceRetirementRequest {
    let incarnation = FileIncarnationId::new(store_uuid(), 7).expect("test incarnation is valid");
    let ticket = TicketId::new(42).expect("test ticket is valid");
    let binding = NamespaceTicketBinding::new(
        ticket,
        incarnation,
        RetirementReason::TtlExpired,
        0,
        3,
        1024,
        [0x44; 16],
    )
    .expect("test ticket binding is valid");
    let canonical = StoreRelativePath::new("commitlog/00000000000000000000").expect("canonical test path is valid");
    let tombstone = StoreRelativePath::new(
        "commitlog/.delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000003.n44444444444444444444444444444444",
    )
    .expect("tombstone test path is valid");
    NamespaceRetirementRequest::new(binding, physical_key, canonical, tombstone)
        .expect("test reservation request is valid")
}

struct Fixture {
    _store: TempDir,
    root: VerifiedNamespaceRoot,
    canonical: std::path::PathBuf,
    tombstone: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let store = tempfile::tempdir().expect("create temporary Store root");
        let commitlog = store.path().join("commitlog");
        std::fs::create_dir(&commitlog).expect("create commitlog directory");
        let canonical = commitlog.join("00000000000000000000");
        let tombstone = commitlog.join(
            ".delete.t000000000000002a.i0000000000000007.s00000000000000000000.m0000000000000003.n44444444444444444444444444444444",
        );
        let root_handle = open_root_handle(store.path()).expect("open Store root handle");
        let root = VerifiedNamespaceRoot::open(root_handle, store_uuid()).expect("verify Store root handle");
        Self {
            _store: store,
            root,
            canonical,
            tombstone,
        }
    }

    fn create_target(&self) -> File {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.canonical)
            .expect("create canonical target");
        file.set_len(1024).expect("set canonical target length");
        file
    }

    #[cfg(any(target_os = "linux", windows))]
    fn advance(
        &self,
        request: NamespaceRetirementRequest,
        transition: NamespaceTransition,
    ) -> NamespaceTransitionOutcome {
        let authorization = NamespaceMutationAuthorization::for_test(&request, transition);
        let reservation = self
            .root
            .reserve(request, transition)
            .expect("construct verified path reservation");
        apply_namespace_transition(reservation, authorization)
    }
}

#[cfg(any(target_os = "linux", windows))]
fn creation_writer() -> ManagedLedgerWriter<ModelLedgerIo> {
    ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), [0x33; 16], 2, 100, 77, 0, true, 5)
        .expect("construct managed creation writer")
}

#[cfg(any(target_os = "linux", windows))]
fn creation_plan(
    create_sequence: u64,
    segment_offset: u64,
    expected_length: u64,
) -> (StoreRelativePath, StoreRelativePath, IncarnationAllocationPlan) {
    let incarnation = FileIncarnationId::new(store_uuid(), create_sequence).expect("test incarnation is valid");
    let canonical =
        StoreRelativePath::new(&format!("commitlog/{segment_offset:020}")).expect("canonical creation path is valid");
    let nonce = [create_sequence as u8; 16];
    let create = canonical
        .create_file_path(incarnation, segment_offset, &nonce)
        .expect("create-file path is valid");
    let plan = IncarnationAllocationPlan::new(
        incarnation,
        segment_offset,
        expected_length,
        nonce,
        canonical.clone(),
        create.clone(),
    )
    .expect("creation plan is valid");
    (canonical, create, plan)
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn durable_creation_publishes_the_exact_handle_without_replacing_a_name() {
    let fixture = Fixture::new();
    let (canonical, create, plan) = creation_plan(8, 1_000_000, 4096);
    let canonical_on_disk = canonical.join_under(fixture._store.path());
    let create_on_disk = create.join_under(fixture._store.path());
    let mut writer = creation_writer();

    let allocated = writer.append_allocate_incarnation(plan).expect("Allocate is durable");
    let created = fixture
        .root
        .create_incarnation_temp(&allocated)
        .expect("create and sync unique temp file");
    let key = created.physical_key();
    assert_eq!(std::fs::metadata(&create_on_disk).expect("temp metadata").len(), 4096);

    let bound = writer.append_bind_incarnation(allocated, key).expect("Bind is durable");
    let verified = fixture
        .root
        .publish_bound_incarnation(created, &bound)
        .expect("publish and reopen canonical file");
    assert_eq!(verified.physical_key(), key);
    assert_eq!(verified.into_file().metadata().expect("canonical metadata").len(), 4096);
    assert!(canonical_on_disk.is_file());
    assert!(!create_on_disk.exists());

    let published = writer.append_publish_incarnation(bound).expect("Publish is durable");
    assert_eq!(published.physical_key(), key);
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn durable_creation_refuses_an_existing_canonical_name_before_creating_temp() {
    let fixture = Fixture::new();
    let (canonical, create, plan) = creation_plan(9, 2_000_000, 4096);
    let canonical_on_disk = canonical.join_under(fixture._store.path());
    let create_on_disk = create.join_under(fixture._store.path());
    std::fs::write(&canonical_on_disk, b"existing incarnation").expect("install canonical collision");
    let mut writer = creation_writer();
    let allocated = writer.append_allocate_incarnation(plan).expect("Allocate is durable");

    let error = fixture
        .root
        .create_incarnation_temp(&allocated)
        .expect_err("canonical collision must fail closed");
    assert_eq!(error.stage(), super::IncarnationCreationStage::VerifyNames);
    assert_eq!(
        std::fs::read(&canonical_on_disk).expect("read canonical collision"),
        b"existing incarnation"
    );
    assert!(!create_on_disk.exists());
}

#[cfg(any(target_os = "linux", windows))]
#[test]
fn durable_creation_rejects_a_mismatched_binding_before_namespace_publish() {
    let fixture = Fixture::new();
    let (canonical, create, plan) = creation_plan(10, 3_000_000, 4096);
    let canonical_on_disk = canonical.join_under(fixture._store.path());
    let create_on_disk = create.join_under(fixture._store.path());
    let mut writer = creation_writer();
    let allocated = writer.append_allocate_incarnation(plan).expect("Allocate is durable");
    let created = fixture
        .root
        .create_incarnation_temp(&allocated)
        .expect("create and sync unique temp file");
    let wrong_key = match created.physical_key() {
        PhysicalFileKey::Unix(_) => PhysicalFileKey::unix(0x77, 0x88),
        PhysicalFileKey::Windows(_) => PhysicalFileKey::windows(0x77, [0x88; 16]),
    };
    let bound = writer
        .append_bind_incarnation(allocated, wrong_key)
        .expect("synthetic mismatched Bind is durable");

    let error = fixture
        .root
        .publish_bound_incarnation(created, &bound)
        .expect_err("mismatched binding must fail before rename");
    assert_eq!(error.stage(), super::IncarnationCreationStage::VerifyNames);
    assert!(!canonical_on_disk.exists());
    assert!(create_on_disk.is_file());
}

#[cfg(windows)]
fn open_root_handle(path: &std::path::Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    use windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
    use windows::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
    use windows::Win32::Storage::FileSystem::FILE_SHARE_DELETE;
    use windows::Win32::Storage::FileSystem::FILE_SHARE_READ;
    use windows::Win32::Storage::FileSystem::FILE_SHARE_WRITE;

    OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(path)
}

#[cfg(not(windows))]
fn open_root_handle(path: &std::path::Path) -> std::io::Result<File> {
    File::open(path)
}

#[cfg(windows)]
mod windows_tests {
    use std::os::windows::fs::OpenOptionsExt;

    use windows::Win32::Storage::FileSystem::FILE_SHARE_READ;
    use windows::Win32::Storage::FileSystem::FILE_SHARE_WRITE;

    use super::*;
    use crate::mapped_file::retirement::platform::NamespaceFailureClass;
    use crate::mapped_file::retirement::platform::NamespacePolicyViolation;
    use crate::mapped_file::retirement::platform::NamespaceTransitionOutcome;

    #[test]
    fn unique_tombstone_rename_and_removal_retry_idempotently() {
        let fixture = Fixture::new();
        let target = fixture.create_target();
        let key = physical_file_key(&target).expect("capture target key");
        drop(target);
        let unknown = fixture.canonical.parent().expect("commitlog parent").join("unknown");
        let unknown_directory = fixture
            .canonical
            .parent()
            .expect("commitlog parent")
            .join("foreign-directory");
        std::fs::write(&unknown, b"preserve").expect("create unknown sibling");
        std::fs::create_dir(&unknown_directory).expect("create unknown directory");

        let moved = fixture.advance(request(key), NamespaceTransition::MoveToTombstone);
        assert!(
            matches!(moved, NamespaceTransitionOutcome::Tombstoned(_)),
            "unexpected move outcome: {moved:?}"
        );
        assert!(!fixture.canonical.exists());
        assert!(fixture.tombstone.exists());

        let repeated_move = fixture.advance(request(key), NamespaceTransition::MoveToTombstone);
        assert!(matches!(repeated_move, NamespaceTransitionOutcome::Tombstoned(_)));

        let removed = fixture.advance(request(key), NamespaceTransition::RemoveTombstone);
        assert!(matches!(
            removed,
            NamespaceTransitionOutcome::NamespaceAbsentVerified(_)
        ));
        assert!(!fixture.tombstone.exists());

        let repeated_remove = fixture.advance(request(key), NamespaceTransition::RemoveTombstone);
        assert!(matches!(
            repeated_remove,
            NamespaceTransitionOutcome::NamespaceAbsentVerified(_)
        ));
        assert!(!fixture.tombstone.exists());
        assert_eq!(std::fs::read(&unknown).expect("read unknown sibling"), b"preserve");
        assert!(unknown_directory.is_dir());
    }

    #[test]
    fn replacement_does_not_block_removal_of_the_exact_old_tombstone() {
        let fixture = Fixture::new();
        let original = fixture.create_target();
        let original_key = physical_file_key(&original).expect("capture original key");
        drop(original);
        std::fs::rename(&fixture.canonical, &fixture.tombstone).expect("stage exact old tombstone");
        std::fs::write(&fixture.canonical, b"replacement").expect("create replacement");
        let replacement = File::open(&fixture.canonical).expect("open replacement");
        let replacement_key = physical_file_key(&replacement).expect("capture replacement key");
        drop(replacement);
        let unknown = fixture.canonical.parent().expect("commitlog parent").join("unknown");
        std::fs::write(&unknown, b"preserve").expect("create unknown sibling");

        let result = fixture.advance(request(original_key), NamespaceTransition::RemoveTombstone);
        assert!(
            matches!(result, NamespaceTransitionOutcome::NamespaceAbsentVerified(_)),
            "unexpected removal outcome: {result:?}"
        );
        assert_eq!(
            std::fs::read(&fixture.canonical).expect("read replacement"),
            b"replacement"
        );
        assert!(!fixture.tombstone.exists());
        assert_ne!(replacement_key, original_key);
        assert_eq!(std::fs::read(unknown).expect("read unknown sibling"), b"preserve");
    }

    #[test]
    fn live_owner_without_delete_sharing_is_retryable_and_then_converges() {
        let fixture = Fixture::new();
        let target = fixture.create_target();
        let key = physical_file_key(&target).expect("capture target key");
        drop(target);
        let blocker = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0)
            .open(&fixture.canonical)
            .expect("open target without delete sharing");

        let blocked = fixture.advance(request(key), NamespaceTransition::MoveToTombstone);
        assert!(matches!(
            blocked,
            NamespaceTransitionOutcome::Retryable(ref failure)
                if failure.class() == NamespaceFailureClass::SharingViolation
        ));
        assert!(fixture.canonical.exists());
        assert!(!fixture.tombstone.exists());

        drop(blocker);
        let moved = fixture.advance(request(key), NamespaceTransition::MoveToTombstone);
        assert!(matches!(moved, NamespaceTransitionOutcome::Tombstoned(_)));
        let removed = fixture.advance(request(key), NamespaceTransition::RemoveTombstone);
        assert!(matches!(
            removed,
            NamespaceTransitionOutcome::NamespaceAbsentVerified(_)
        ));
    }

    #[test]
    fn direct_unlink_remains_a_typed_policy_rejection() {
        let fixture = Fixture::new();
        let original = fixture.create_target();
        let original_key = physical_file_key(&original).expect("capture original key");
        drop(original);

        let rejected = fixture
            .root
            .reserve(request(original_key), NamespaceTransition::DirectUnlink);
        assert!(matches!(
            rejected,
            Err(NamespaceTransitionOutcome::Rejected(
                NamespacePolicyViolation::UnsupportedTransition {
                    transition: NamespaceTransition::DirectUnlink
                }
            ))
        ));
        assert!(fixture.canonical.exists());
    }
}

#[cfg(target_os = "linux")]
mod linux_tests {
    use std::io::Read;
    use std::io::Seek;
    use std::io::Write;
    use std::os::unix::fs::symlink;

    use super::*;
    use crate::mapped_file::retirement::platform::NamespacePolicyViolation;

    #[test]
    fn unique_tombstone_rename_and_removal_retry_idempotently() {
        let fixture = Fixture::new();
        let target = fixture.create_target();
        let key = physical_file_key(&target).expect("capture target key");
        drop(target);

        let moved = fixture.advance(request(key), NamespaceTransition::MoveToTombstone);
        assert!(matches!(moved, NamespaceTransitionOutcome::Tombstoned(_)));
        assert!(!fixture.canonical.exists());
        assert!(fixture.tombstone.exists());

        let repeated_move = fixture.advance(request(key), NamespaceTransition::MoveToTombstone);
        assert!(matches!(repeated_move, NamespaceTransitionOutcome::Tombstoned(_)));

        let removed = fixture.advance(request(key), NamespaceTransition::RemoveTombstone);
        assert!(matches!(
            removed,
            NamespaceTransitionOutcome::NamespaceAbsentVerified(_)
        ));
        assert!(!fixture.tombstone.exists());

        let repeated_remove = fixture.advance(request(key), NamespaceTransition::RemoveTombstone);
        assert!(matches!(
            repeated_remove,
            NamespaceTransitionOutcome::NamespaceAbsentVerified(_)
        ));
    }

    #[test]
    fn direct_unlink_preserves_a_live_owner_and_unknown_siblings() {
        const PAYLOAD: &[u8] = b"owner remains live";

        let fixture = Fixture::new();
        let mut owner = fixture.create_target();
        owner.write_all(PAYLOAD).expect("write target");
        owner.sync_all().expect("sync target bytes");
        let key = physical_file_key(&owner).expect("capture target key");
        let unknown = fixture.canonical.parent().expect("commitlog parent").join("unknown");
        std::fs::write(&unknown, b"preserve").expect("create unknown sibling");

        let outcome = fixture.advance(request(key), NamespaceTransition::DirectUnlink);

        assert!(matches!(
            outcome,
            NamespaceTransitionOutcome::NamespaceAbsentVerified(_)
        ));
        assert!(!fixture.canonical.exists());
        owner.rewind().expect("rewind live owner");
        let mut bytes = Vec::new();
        owner.read_to_end(&mut bytes).expect("read live unlinked owner");
        let mut expected = vec![0; 1024];
        expected[..PAYLOAD.len()].copy_from_slice(PAYLOAD);
        assert_eq!(bytes, expected);
        assert_eq!(std::fs::read(unknown).expect("read unknown sibling"), b"preserve");
    }

    #[test]
    fn symlink_target_is_rejected_without_touching_its_referent() {
        let fixture = Fixture::new();
        let outside = fixture._store.path().join("outside");
        std::fs::write(&outside, b"preserve").expect("create referent");
        let referent = File::open(&outside).expect("open referent");
        let key = physical_file_key(&referent).expect("capture referent key");
        drop(referent);
        symlink(&outside, &fixture.canonical).expect("create canonical symlink");

        let outcome = fixture.advance(request(key), NamespaceTransition::DirectUnlink);

        assert!(matches!(
            outcome,
            NamespaceTransitionOutcome::Rejected(NamespacePolicyViolation::UnexpectedEntryType { .. })
        ));
        assert!(fixture.canonical.is_symlink());
        assert_eq!(std::fs::read(outside).expect("read referent"), b"preserve");
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
#[test]
fn unsupported_targets_fail_before_constructing_a_root_capability() {
    let store = tempfile::tempdir().expect("create Store root");
    let handle = File::open(store.path()).expect("open Store root");

    assert!(matches!(
        VerifiedNamespaceRoot::open(handle, store_uuid()),
        Err(super::NamespaceTransitionOutcome::Unsupported { .. })
    ));
}
