# M11-12 ArcMut soundness evidence

> Snapshot: Issue #8647 / M11-12bc113 candidate
> Status: PASS for R23 technical audit; R18 and the HUMAN compatibility gate remain open
> Scope: bounded Miri evidence, Loom replacement model, and retained-wrapper decision

## Outcome

The R23 audit reaches a fail-closed decision: the stable `parking_lot::RwLock` backing is valid
when accessed through guards, but the public ArcMut compatibility facade cannot be approved as a
sound retained wrapper.

- A generated, isolated probe depends on the checked-out `rocketmq-rust` crate and therefore
  exercises the actual public ArcMut implementation without adding a governed repository Rust
  caller.
- The guarded `get_inner().read()`/`write()` path passes under Miri.
- Two ArcMut clones can create simultaneous mutable references through safe `DerefMut`; Miri
  reports undefined behavior because the second unique retag invalidates the first reference.
- The negative result is an expected contract, not a skipped or failed test. The probe runner
  requires both a non-zero exit and Miri's reference-aliasing undefined-behavior markers.
- A bounded Loom model proves the intended `Arc<RwLock<T>>` replacement serializes concurrent
  writers and releases every worker strong owner before root `try_unwrap`.

The technical review therefore rejects long-term retention of `mut_from_ref`, clone-safe
`AsMut`, and clone-safe `DerefMut`. R18 must delete the public facade after its existing
next-major two-cycle deprecation and Release Manager/HUMAN approval window. This audit does not
grant that compatibility approval or mark R18 complete.

## Evidence boundary

| Surface | Result | Claimed conclusion |
|---|---|---|
| ArcMut guarded backing | Miri PASS | Guarded stable backing access is valid in the bounded probe |
| ArcMut clone-safe mutable aliases | Miri expected UB | The safe reference escapes cannot be retained as a sound API |
| `Arc<RwLock<T>>` concurrent writers | Loom PASS | The proposed guarded replacement serializes both modeled writers |
| worker strong-owner release | Loom PASS | Joined workers release ownership and permit root `try_unwrap` |
| `Weak` upgrade/drop interleavings | not modeled | Loom 0.7.2 does not expose `Weak`; no result is claimed |
| R18 public compatibility deletion | not executed | Still requires the documented next-major and HUMAN/Release Manager gate |

The Loom model intentionally does not import ArcMut and is not presented as proof for the legacy
facade. It verifies only the safe replacement contract that R18 callers must adopt.

## Reproducible runner

`scripts/arc_mut_soundness_probe.py` fingerprints `rocketmq/Cargo.toml` and
`rocketmq/src/arc_mut.rs`, then writes its generated probe and logs below
`target/architecture-refactor/M11/arc-mut-soundness/<fingerprint>/`.

For snapshot `096500ef5369c6d3`, the runner used:

- Miri `0.1.0 (3659db0d3e 2026-07-05)`;
- Loom `0.7.2`;
- guarded Miri exit code `0`;
- alias Miri exit code `1`, classified as expected undefined behavior;
- Loom replacement model `2/2` passed.

The generated probe lockfile and complete command output remain untracked runtime evidence.
Classifier unit tests cover success, unexpected alias success, unrelated tool failure, and the
expected reference-aliasing diagnostic.

## Validation

| Command | Result |
|---|---|
| `python -m unittest scripts.tests.test_arc_mut_soundness_probe -v` | 4/4 passed |
| `cargo test -p rocketmq-rust --test arc_mut_replacement_loom` | 2/2 passed |
| `python scripts/arc_mut_soundness_probe.py` | guarded PASS, alias expected UB, Loom PASS |
| `python scripts/arc_mut_guard.py` | PASS at 20 identities / 58 occurrences |
| `python scripts/stable_surface_guard.py --mode target` | PASS with 0 nightly features |
| `RUSTFLAGS=-Cdebuginfo=0 cargo +stable check -p rocketmq-rust --all-targets` | PASS |
| `.\scripts\check-agents-routing.ps1` | PASS |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` | PASS |
| `git diff --check` | PASS |

## Decision and handoff

R23 is complete because the available Miri slice, bounded Loom replacement model, and
retained-wrapper audit have all produced an explicit result. The result is not approval to keep
ArcMut: there is no sound retained-wrapper candidate.

R18 owns the destructive compatibility change. Until its external window is satisfied, the
facade remains time-bounded compatibility debt, its reviewed baseline cannot grow, and new code
must use guarded standard ownership instead.

## Rollback

Reverting bc113 removes the probe runner, classifier tests, Loom model, dependency, and this
evidence. It does not make ArcMut sound and must not be used to reinterpret the Miri result.
