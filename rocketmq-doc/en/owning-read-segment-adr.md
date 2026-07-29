# Owning Read Segment ADR

- Status: Accepted
- Date: 2026-07-29
- Owner: Store and HA maintainers
- Decision scope: copied reads, mapped-file leases, file ranges, sendfile, and transport fallback

## Context

The Store already has backend-neutral `LeasedBytes<MessageReadLease>` and
Local `SegmentLease`, `MmapRegionSlice`, and `FileRange` owners. One separate
`MappedBuffer::read_zero_copy` API nevertheless returned newly allocated
`Bytes`, included a manual copy branch, and claimed a measured speedup that had
not been established. Its name and documentation contradicted its ownership.

Transfer correctness also depends on proving that mapped data remains live
while a file range is in flight and that platforms without sendfile emit the
same frame.

## Decision

`MappedBuffer` exposes `read_copy`. It performs an owning allocation and copy,
and its tests prove that the returned allocation does not alias the mutable
mmap. The inaccurate `read` and `read_zero_copy` source APIs, hand-written
unsafe copy, and unsupported performance claim are removed. They are internal
Rust source contracts and receive no deprecated wrapper.

The canonical transfer contract is:

- copied `Bytes` for explicitly owning, long-lived, or parsing-oriented reads;
- `MessageReadLease` for backend-neutral Store capability results;
- `SegmentLease` for Local transfer planning;
- `FileRange` for Linux plaintext sendfile;
- owning bytes and vectored writes for TLS, Windows, or unavailable sendfile.

A mapped selection retains exactly one `DefaultMappedFile` hold. Conversion to
`SegmentLease` transfers that hold rather than acquiring another one. Drop
releases it exactly once. A live lease fences mapped-file destroy, unmap, and
recycle. The selection retains an immutable byte snapshot for fallback, while
the file range keeps both an open file and, when mapped-backed, the mapped-file
hold.

Published ranges are immutable. Appending after a range is published may write
only after its end and cannot change bytes already visible through the lease.
Recovery and truncation must first release selections and leases.

## Safety and platform behavior

`NativeMappedMemory` owns its mapping through `Arc<MmapMut>`;
`MmapRegionSlice` owns another `Arc` and bounds its dereference to the selected
region. Existing unsafe mapping and copy contracts retain adjacent `SAFETY`
invariants. This change removes the unsafe branch from `MappedBuffer` and adds
no new unsafe code.

Linux plaintext HA prefers `FileRange`/sendfile. The engine handles partial,
interrupted, and would-block writes. If a batch lacks file ranges, it falls
back to the owning byte snapshot and vectored output. The same owning fallback
is the normal path on TLS and platforms without sendfile. Tests compare the
complete header and payload frame between the file-range and byte paths.

## Compatibility

RocketMQ frame bytes, message bytes, offsets, error behavior, recovery, and HA
ordering do not change. Only inaccurate internal Rust source names are
removed. Public compatibility evidence classifies this as an approved
breaking source cleanup.

## Evidence

- `rocketmq-store-local/src/mapped_file/memory.rs`
- `rocketmq-store-local/src/mapped_file/select_result.rs`
- `rocketmq-store-local/src/transfer/segment.rs`
- `rocketmq-store-local/src/ha/transfer_engine/sendfile.rs`
- `rocketmq-store-local/tests/ha_transfer_boundary.rs`
- `rocketmq-store-local/tests/mapped_write_lease_loom.rs`
- `rocketmq-store-local/tests/mapped_write_lease_miri.rs`
- `rocketmq-store/benches/mapped_buffer_bench.rs`

### Same-host performance gate

The copied-read rename was measured on 2026-07-29 on the same Windows 11
26200 host (Intel Core i7-11700K, 8 cores/16 logical processors, 31.9 GiB
memory). Both revisions used Criterion 0.8, a 64 KiB mapped range, 10 samples,
2 seconds of warm-up, and 5 seconds of measurement:

| Revision and benchmark | Sample median | MAD | Maximum sample deviation | Criterion throughput |
|---|---:|---:|---:|---:|
| `d4e876f80`, `read/copy/65536` | 1,599.671 ns | 11.241 ns | 3.263% | 38.155 GiB/s |
| candidate, `copied_read/bytes_copy/65536` | 1,592.652 ns | 7.229 ns | 1.698% | 38.348 GiB/s |

The candidate sample median is 0.44% lower and throughput is 0.51% higher
using Criterion's point estimate, so the copied-read path is within the 5%
non-regression gate. Each operation still explicitly allocates and copies
65,536 payload bytes; this evidence does not relabel the operation as
zero-copy.

No new transfer path becomes the default in this decision. Lease, file-range,
sendfile, Windows, and TLS fallback behavior is therefore accepted on byte
parity, recovery, lifetime, Miri, Loom, and Linux fake-sendfile evidence
rather than an unsupported performance claim. A later default-path change
requires a separate target-hardware profile covering CPU, RSS, p99, lease hold
duration, recycle blocking, I/O amplification, and fallback reasons.
