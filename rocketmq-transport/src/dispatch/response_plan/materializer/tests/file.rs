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

use super::*;

#[tokio::test]
async fn file_regions_preserve_order_offsets_and_bounded_chunk_progress() {
    let (harness, control) = ControlHarness::new("legacy-materializer-file-regions", None);
    let first_len = FILE_REGION_READ_CHUNK_BYTES + 17;
    let contents = (0..first_len + 64).map(|index| (index % 251) as u8).collect::<Vec<_>>();
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(&contents).expect("write file contents");
    let accesses = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = Arc::new(CountingLease {
        file,
        accesses: Arc::clone(&accesses),
        drops: Arc::clone(&drops),
    });
    let first = FileRegion::try_new(lease.clone(), 3, first_len as u64).expect("first file region");
    let second = FileRegion::try_new(lease.clone(), 1, 29).expect("second file region");
    let regions = FileRegionSequence::try_new(vec![first, second]).expect("file region sequence");
    assert_eq!(accesses.load(Ordering::SeqCst), 2);
    let expected = [&contents[3..3 + first_len], &contents[1..30]].concat();
    let expected_len = expected.len();
    let (receiver, _) = handoff(
        ResponsePlan::file_regions(response_head(91, 21), regions).expect("file plan"),
        control,
    )
    .await;

    let command = receiver
        .receive_command(limits(expected_len, 2), harness.blocking())
        .await
        .expect("file materialization");
    assert_eq!(command.body().expect("body").as_ref(), expected);
    assert_eq!(accesses.load(Ordering::SeqCst), 5);
    drop(command);
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn exact_frame_failure_precedes_file_access_and_destination_allocation() {
    let (harness, control) = ControlHarness::new("legacy-materializer-file-preflight", None);
    let body = b"file-preflight";
    let (regions, lease, accesses, drops) = counting_region(body);
    let head = response_head(92, 22).set_remark("frame preflight");
    let encoded_len = FrameLimits::java_compatibility()
        .encode_frame_head(head.clone(), body.len())
        .expect("oracle frame head")
        .encoded_len();
    let frame_limits = FrameLimits::try_new(
        encoded_len - 1,
        FrameLimits::java_compatibility().max_header_bytes,
        body.len(),
        8,
    )
    .expect("one-over frame profile");
    let materialization_limits =
        LegacyMaterializationLimits::try_new(frame_limits, body.len(), 1).expect("materialization limits");
    let (receiver, _) = handoff(ResponsePlan::file_regions(head, regions).expect("file plan"), control).await;

    assert!(matches!(
        receiver
            .receive_command(materialization_limits, harness.blocking())
            .await,
        Err(LegacyLocalMaterializationError::Frame { .. })
    ));
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn truncation_after_region_validation_returns_typed_short_read_without_partial_command() {
    let (harness, control) = ControlHarness::new("legacy-materializer-short-file", None);
    let (regions, lease, accesses, drops) = counting_region(b"validated-length");
    lease.file.set_len(4).expect("truncate leased file after validation");
    let (receiver, _) = handoff(
        ResponsePlan::file_regions(response_head(93, 23), regions).expect("file plan"),
        control,
    )
    .await;

    let error = expect_materialization_error(
        receiver.receive_command(limits(16, 1), harness.blocking()).await,
        "short file must fail",
    );
    let LegacyLocalMaterializationError::FileIo { source } = error else {
        panic!("short file should retain its io source");
    };
    assert_eq!(source.kind(), io::ErrorKind::UnexpectedEof);
    assert!(accesses.load(Ordering::SeqCst) >= 2);
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[test]
fn checked_positional_reads_handle_offsets_partial_ranges_and_overflow() {
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(b"0123456789").expect("write file contents");
    let mut buffer = [0_u8; 4];

    let read = read_file_region_chunk(&file, &mut buffer, 2, 3).expect("checked positional read");
    assert_eq!(read, 4);
    assert_eq!(&buffer, b"5678");

    let partial = read_file_region_chunk(&file, &mut buffer, 8, 0).expect("partial positional read");
    assert_eq!(partial, 2);
    assert_eq!(&buffer[..partial], b"89");

    let overflow = read_file_region_chunk(&file, &mut buffer, u64::MAX, 1).expect_err("offset overflow");
    assert_eq!(overflow.kind(), io::ErrorKind::InvalidInput);
}
