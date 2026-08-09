# Mapped-file physical-owner API migration

- Status: Accepted for issue [#9141](https://github.com/mxsm/rocketmq-rust/issues/9141)
- Applies to: `rocketmq-store-local` and direct workspace consumers
- Compatibility decision: source-breaking migration; persisted records, message bytes, offsets, and HA frames are unchanged

## Why this migration is source-breaking

The former APIs could detach lifecycle admission from a borrowed mapping or operating-system file
handle. That made logical cleanup observable as complete while an mmap or cloned handle was still
live. The replacement API makes every cross-call mapping or file capability own both an operation
lease and the corresponding physical owner. Keeping the unsafe compatibility paths would preserve
the race this change is intended to remove.

## API mapping

| Previous API | Replacement | Behavior change |
|---|---|---|
| `MappedFile::get_file() -> &File` | `DefaultMappedFile` selection/transfer APIs, or `MappedFileStorage::with_file` for standalone scoped storage work | No file reference can outlive the scoped owner capture. HA transfer uses an owner-bound `FileRangeLease`. |
| `MappedFileStorage::file() -> &File` | `MappedFileStorage::with_file(...)` | The callback cannot return a value borrowing the file. Operations fail with `NotConnected` after owner detach. |
| `NativeMappedMemory: Clone` and `clone_mmap()` | `MappedReadLease` for sealed reads | Writable mappings are not cloneable. Only a read-only generation can expose a safe cross-call slice. |
| `MmapRegionSlice::try_new(Arc<MmapMut>, ...)` | `DefaultMappedFile::try_mapped_read_lease(...)` | Active writable files return `Ok(None)`; sealing must complete before a mapped read lease is available. Clone and split share one admission. |
| Public `MappedFileMapping<M>` lazy value container | `DefaultMappedFile` and its internal generation slot | The old standalone `new_eager`, `new_lazy`, `get`, and `get_or_try_init` API has no direct compatibility facade because it could publish or borrow an owner outside lifecycle admission. General-purpose lazy values should use `OnceLock` or another application-owned cell. |
| `MappedMemory::Region` and `MappedMemory::region(...)` | `MappedMemory::ReadOnly`, `MappedReadLease`, and internal owner-bound maintenance regions | Writable maintenance ranges expose no safe shared slice. Custom backends must provide a distinct read-only backend. |
| Safe `MappedMemory::map_mut` / `ReadOnlyMappedMemory::map` implementations | `unsafe fn` implementations with the documented file-stability contract | Callers must prove the file remains sized and cannot be mutated incompatibly until the owner-bound generation drops. |
| `SegmentLease::from_file_range(...)` | `SegmentLease::try_from_file_range(...)` | Range overflow and out-of-bounds input are typed errors instead of an unchecked or panicking construction. |
| Manually paired mapping/file owner and lifecycle release | `MappedReadLease`, `FileRangeLease`, or `SegmentLease::from_selection(...)` | Final `Drop` releases the physical owner before the lifecycle operation; callers cannot release twice. |

## Lifecycle semantics

- `shutdown` rejects new operations and detaches the mapping and canonical file-owner slots after
  admitted operations drain. It does not delete the segment path.
- Detach is exactly once, but it is not forced reclamation. An existing owner-bound lease may keep
  its generation or file handle alive until the lease's final `Drop`.
- Namespace deletion is attempted only after detach completes. A failed deletion remains retryable
  and does not authorize queue untracking.
- A lazy mapping candidate is published only while holding the same close-control boundary as
  `Closing`; a losing candidate is dropped and cannot remap later.
- Sealing rejects new writers, waits for admitted writers, flushes the writable generation, and
  publishes a distinct read-only generation before the sealing call succeeds.

## Packed lifecycle counter compatibility

The lifecycle now linearizes admission state, total leases, and writer leases in one packed
`AtomicUsize`. Total and writer counts each have a maximum of 32,767 on 32-bit targets and
2,147,483,647 on 64-bit targets. Exceeding either limit fails closed with the existing typed
`MappedFileError::LeaseCountOverflow`; it never wraps a counter or admits an untracked operation.

This architecture-dependent reduction from the former total-only packed counter is an accepted
runtime compatibility change. It does not alter persisted bytes, offsets, HA frames, or error
mapping, and it reuses the existing typed overflow surface. The new limits remain far above the
number of simultaneously sustainable mapped-file operations in supported deployments.

## Custom backend checklist

- Implement the new `MappedMemory::ReadOnly` associated type.
- Keep writable and read-only mapping addresses stable for their complete value lifetimes.
- Call the unsafe mapping constructors only when file length and mutation exclusion are guaranteed.
- Do not provide `Clone` or safe shared slices for writable mappings.
- Use checked owner-bound ranges; never derive a slice from a temporary generation or file owner.

## Rollback and deferred work

This change has no disk-format migration. Code rollback requires a process restart so no generation
or owner lease from the newer process remains live. Durable retirement tickets, incarnation-safe
path reuse, tombstone rename, crash replay, and the deletion reaper are intentionally deferred to
the M3 retirement protocol; a bare `NotFound` is not treated as proof that an old incarnation was
successfully retired.
