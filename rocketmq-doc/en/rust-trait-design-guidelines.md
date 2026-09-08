# Rust Trait Design Guidelines

This policy applies to new traits and to traits changed by the async,
security, store, client, and Proxy architecture work.

## Deletion test

Before adding a public trait, remove it from the proposed design. Keep the
trait only when at least two real implementations or a stable testing seam
need the same contract, or when the trait hides meaningful policy and
invariants. A single pass-through adapter, an empty marker, or a speculative
future implementation is not a public seam.

## Dispatch and async choice

- Use a concrete type or generics for static dispatch.
- Use native `async fn` for crate-private traits and public static-dispatch
  contracts. Add `Send + Sync` supertraits when implementations, rather than
  the returned future itself, cross task boundaries. Scope any
  `async_fn_in_trait` lint allowance to the trait and document the contract.
- Use `trait_variant` only when a public interface must guarantee a `Send`
  future. The generated naming convention must identify the `Send` variant.
- If a real object-safe seam is required, use an explicit boxed future or
  redesign a narrower interface. Do not add `#[async_trait]`.
- Existing `#[async_trait]` sites migrate only when their owning module is
  changed. Mechanical workspace-wide conversion is not required.

## Object safety, cancellation, and errors

A dynamic interface must state why dynamic dispatch is required, who owns
the object, and whether callers may retain it across tasks. Async operations
must document cancellation safety: dropping a future must not leave a
partially published state, leaked permit, or detached task. Methods return
the owning crate's typed error and preserve source chains; public library
traits do not expose `anyhow::Result`.

`Send` and `Sync` bounds express observed cross-task ownership, not a
convenient default. Avoid supertraits that force unrelated implementations
to acquire runtime, transport, or storage capabilities.

## Capability migration facades

A broad historical trait may remain temporarily when multiple production
consumers cannot move atomically. Keep the migration scope explicit:

- review method and consumer changes at the affected capability boundary;
- assign every remaining consumer an owner, reason, and deletion condition;
- permit removals without deprecated wrappers when the contract is internal;
- place new behavior in one narrow capability with only the operations the use
  case needs;
- do not combine narrow traits into an equivalent mandatory supertrait.

Backend conformance applies only to capabilities a backend claims to
implement. Unsupported optional behavior is explicit and must not be
represented by panic or a default no-op. Capability request/result types own
durability, cancellation, deadline, and typed-error semantics so adapters do
not infer them from a broad implementation type.

Validate Store capability changes with the affected Rust contracts, for example
`cargo test -p rocketmq-store --test capability_conformance_tests`. Select backend
features when their behavior changes. The completed migration's source-text gate
and baseline are retired.

## Review and ownership

Review existing macro sites when changing their owning module. Native `async fn`
is an approved implementation style; no historical identity inventory or baseline
refresh is required. Explain dynamic dispatch, `Send` requirements, and public
marker traits through their actual callers and invariants. Use compiler checks
and focused behavior tests for the affected crate.
