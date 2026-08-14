# OPT-03: direct trait-object ownership stop decision

## Candidate

The candidate replaced `Option<Arc<Box<dyn CommandCustomHeader>>>` with `Option<Arc<dyn CommandCustomHeader>>`. This removes one allocation and one indirection when a command owns a typed custom header, but changes the `Arc` from a thin pointer to a fat pointer inside every `RemotingCommand`.

## Allocation and footprint evidence

The existing allocation probe was extended with a canonical `GetRouteInfoRequestHeader` construction case. Baseline and candidate were built separately in the same worktree and process profile.

| Metric | Baseline | Candidate | Result |
|---|---:|---:|---:|
| Typed command construction allocations | 2 | 1 | -1 allocation |
| Typed command construction bytes | 152 B | 136 B | -16 B |
| `size_of::<RemotingCommand>()` | 160 B | 168 B | +5.0% |
| RSS delta for 100,000 empty commands | 16,015,360 B | 16,814,080 B | +5.0% |

The clone probe includes construction of its source and reports the same allocation difference; cloning an already-created command continues to clone only the `Arc`. The candidate passed all 36 focused `remoting_command` tests before it was reverted.

## Decision

**STOP.** The allocation saving applies only when constructing a typed-header command, while the additional eight inline bytes apply to every command, including decoded and headerless traffic. The object and RSS increases exceed the 2% footprint budget, so latency benchmarking cannot make this candidate acceptable and was intentionally skipped under the evaluation short-circuit rule.

Production ownership remains `Arc<Box<dyn CommandCustomHeader>>`. The allocation probe stays in the benchmark harness so a future representation can be compared without reconstructing this evidence. Revisit only with a representation that preserves the 160-byte command footprint or with production traffic evidence showing typed-header allocation pressure dominates the universal object cost.
