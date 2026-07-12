# M06-03e generic mmap lifecycle kernel report

## Final outcome

M06-03e makes `rocketmq-store-local::mapped_file::mapping` the canonical owner of the generic mapped-file
initialization state machine and lazy mmap statistics. `MappedFileMapping<M>` owns one `OnceLock<M>`, one
`parking_lot::Mutex<()>`, lazy enablement, and the four legacy operation/failure/total/last-latency atomics.

`DefaultMappedFile` now composes exactly one `MappedFileMapping<ArcMut<MmapMut>>`. Store retains the concrete mmap
type, both unsafe `MmapMut::map_mut` calls, `ArcMut`, the file handle, logging/metrics/observability, and all legacy
reference-returning signatures. `LazyMmapStats` is exact-re-exported from its previous Store path.

## RED -> GREEN evidence

All RED checks ran before production edits.

1. Local lifecycle behavior
   - RED: `cargo test -p rocketmq-store-local --test mapped_file_mapping` exited 101 with two E0432 errors because
     `rocketmq_store_local::mapped_file::mapping` did not exist.
   - GREEN: 7/7 tests pass. They cover eager zero statistics, lazy-uninitialized eligibility, successful
     initialization, failed attempt and successful retry, eight barrier-synchronized concurrent callers with one
     initializer, stable value identity, and monotonic counters without arbitrary sleeps.
2. Ownership and compatibility contract
   - RED: `python -m unittest scripts.tests.test_m06_store_local_contract` ran 46 tests and failed the two expected
     canonical-owner/module assertions because `mapping.rs` and the Local lifecycle owners were absent.
   - GREEN: 47/47 tests pass after adding the owner checks and a generic-struct scanner regression test. The final
     contract proves exact owner fields, exact Store composition and legacy signatures/re-export, and rejects
     renamed fields, type/import aliases, duplicate generic owners, alias/brace/glob re-exports, comments, and
     strings.
3. Store adapter compatibility
   - The type-identity test was present before the Local owner existed. GREEN preserves the exact legacy
     `LazyMmapStats` type and all 30/30 `DefaultMappedFile` focused tests, including eager/lazy initialization and
     destroy-before-first-access behavior.

The first GREEN refinement exposed two test-harness issues rather than behavior regressions: the contract's
struct-body scanner did not yet parse generic owners, and Store still used `Instant` for unrelated flush/warmup
metrics. The scanner gained a focused generic regression test, the existing `Instant` import was retained, and
the full focused set then passed.

## Lifecycle and statistics semantics

- Eager construction stores the supplied value immediately, reports mapped state, and returns all-zero lazy
  statistics (`eligible_files=0`, `mapped_files=0`, and zero counters).
- Lazy construction reports eligible=1/mapped=0. `get_or_try_init` checks the cell before and after taking the
  initialization mutex, so successful concurrent callers execute exactly one initializer.
- A successful lazy initializer increments operations exactly once and records saturating-to-`u64` elapsed
  milliseconds in total and last latency. Existing reads do not acquire the init lock or change counters.
- A failed initializer increments failures, stores no value, and permits a later retry. The fallible value is
  created before `OnceLock::get_or_init` while the lock is held; no new panic/unwrap/expect path is needed.
- `LazyMmapStats::saturating_add_assign` retains the exact prior fields, derives, saturating counter behavior, and
  latest-nonzero-latency behavior.

## Boundary and compatibility preservation

- Local is generic and contains no `ArcMut`, `rocketmq-rust`, Store, Common, Remoting, Broker, RocksDB, or Tiered
  edge. Its normal dependency tree is unchanged.
- Store retains both unsafe mmap creation sites and their operation-specific safety contracts. The eager
  constructor and lazy closure use the same storage handle and `ArcMut::new` behavior as before.
- `is_lazy_mmap_enabled`, `is_mapped`, `lazy_mmap_stats`, `get_mapped_file`, `get_mapped_file_mut`, and
  `get_mapped_file_arcmut` keep their exact public signatures. Zero-copy/reference lifetime behavior is unchanged.
- No second mapping, `RwLock`, `arc_lock`, feature, manifest dependency, persisted-layout change, runtime owner,
  or error mapping was introduced.

## ArcMut governance

The governed baseline remains 1,232 identities and 3,377 occurrences. Six changed occurrence fingerprints are
direct BASE-to-HEAD one-for-one relocations, with no intermediate approval:

- `acf169f0918de3dabff93aa8`: `3b2123c0e754a03714b9b05f` -> `10aa78f168c907ad4363c55f`
  (existing eager constructor in `try_new_inner`).
- `acf169f0918de3dabff93aa8`: `58339aed97bc72b445f45f3c` -> `c9d573ba9d3af5784bcf1def`
  (existing lazy constructor in `try_get_mapped_file_ref`).
- `f80c7a60a612919c6a52504e`: `8622d524765ef0a77c59a703` -> `730945afc8f430296c3b811e`
  (existing `ArcMut` field type in `DefaultMappedFile`).
- `f80c7a60a612919c6a52504e`: `339f4d5796c1feb6461807fa` -> `47c3d0ee2f97a9ae8d2dc8d6`
  (existing `ArcMut` return type in `try_get_mapped_file_ref`).
- `f77276860cc979566f9dbc1e`: `aade4a10d53ff89ec2766419` -> `527d46611cfe6efaad6ce88a`
  (same governed `DefaultMappedFile` shared-UnsafeCell wrapper).
- `22e398389b823d706a043784`: `4d553633a7d26ce689bc0cd1` -> `efc68e011aa65239229d3192`
  (same `DefaultMappedFile` struct type reference).

The baseline diff changes only those six occurrence objects (18 added/18 removed lines); unrelated line-number
churn from candidate generation was removed. Direct BASE/candidate comparison and the final guard both report
`ARC_MUT_GUARD_OK`.

## Validation

All commands ran from the repository root.

- `cargo test -p rocketmq-store-local` - exit 0; 48/48 unit, 6/6 kernel, 7/7 mapping, and 10/10 storage tests
  passed (71 total); nine existing Rustdoc examples remained ignored.
- `cargo test -p rocketmq-store --lib log_file::mapped_file::default_mapped_file_impl::tests` - exit 0; 30/30.
- `cargo test -p rocketmq-store lazy_mmap --lib` - exit 0; 5/5.
- `cargo test -p rocketmq-store --test m06_store_local_compatibility` - exit 0; 3/3.
- `cargo test -p rocketmq-store --test m06_store_local_commitlog_compatibility` - exit 0; 2/2.
- `cargo test -p rocketmq-store --test commitlog_recovery_tests` - exit 0; 9/9.
- `cargo test -p rocketmq-store --test commitlog_load_tests` - exit 0; 7/7, one stress test ignored.
- `python -m unittest scripts.tests.test_m06_store_local_contract` - exit 0; 47/47.
- Local exact no-default, fast-load, safe-load, fast+safe, and io_uring feature checks - all exit 0.
- `cargo tree -p rocketmq-store-local -e normal` - exit 0; no forbidden owner edge.
- `python -m unittest scripts.tests.test_architecture_dependency_guard` - exit 0; 35/35.
- architecture fixtures and baseline mode - exit 0; one clean/six violation fixtures and
  `ARCHITECTURE_DEPENDENCY_GUARD_OK`.
- ArcMut unit tests - exit 0; 63/63. ArcMut fixtures - exit 0; 24/24.
- ArcMut promotion/direct BASE comparison/final guard - exit 0; 1,232 identities, 3,377 occurrences,
  `ARC_MUT_GUARD_OK`.
- `cargo clippy -p rocketmq-store-local --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy -p rocketmq-store --all-targets --all-features -- -D warnings` - exit 0.
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` - exit 0. Windows emitted only
  the existing `linker_messages` notice, which explicitly ignores `-D warnings`, and the existing
  `proc-macro-error2` future-incompatibility notice.
- `$env:RUSTDOCFLAGS='-D warnings'; cargo doc -p rocketmq-store-local --no-deps --all-features` - exit 0.
- `cargo fmt --all -- --check` and `git diff --check` - recorded after the final report/checklist update.

No manifest changed, so AGENTS routing drift control was not triggered. Error hygiene was not triggered because
no error definition or mapping changed. Runtime audit was not triggered because no task, thread, runtime,
blocking, scheduler, or shutdown ownership changed.

## Non-goals and remaining work

This slice does not replace `ArcMut`, enable `arc_lock`, add an `RwLock` or second mapping, change zero-copy or
legacy getter types, move mmap unsafe creation, or move mapped-buffer/select-result, append/direct-write/flush/
warmup/memory-lock, `DefaultMappedFile`, `MappedFile`, messages/callbacks/config, CommitLog orchestration,
CQ/Index, or HA. PR-M06-03 and every M06 Exit Checklist item remain open.

## Rollback

Restore the Store-owned mapping cell/lock/lazy/stat fields and `LazyMmapStats`, remove the Local generic lifecycle
module and focused tests, restore the six prior ArcMut fingerprints, and keep the M06-03a-d Local owners intact.
No disk, wire, persisted-format, public-signature, or feature migration is required.
