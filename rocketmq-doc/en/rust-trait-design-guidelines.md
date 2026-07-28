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
- Use native `async fn` for crate-private traits and for public static
  dispatch that does not promise a `Send` future. Scope any
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

## Inventory and ownership

Run `python scripts/trait_policy_guard.py` to compare production macro,
native-async, and empty-marker sites with the generated baseline. Run
`python scripts/trait_policy_guard.py --write-baseline` only after reviewing
every changed identity and decision.

The inventory assigns existing macro sites to their owning crate with a
migrate-on-touch decision. `MQAdminExtInner` passed the P2.4 deletion test and
was removed at the approved major-version boundary without replacement by
another empty trait. A new marker with no behavior is treated as fresh policy
debt, not as a compatible substitute.

P1 async ownership work and P2 interface work use this policy directly.
Their touched-domain inventory may decrease; any addition requires an
explicit contract justification and baseline review.
