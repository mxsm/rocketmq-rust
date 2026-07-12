# M04 protocol compatibility and ownership ledger

This ledger records the deliberate owner facades and the one M04 ownership-plan exception. It is part of the M04 compatibility contract and must be reviewed before removing any facade.

## Architecture exception: `RocketMqVersion`

M03 established `rocketmq-model` below `rocketmq-protocol` in the foundation DAG. Therefore `RocketMqVersion` remains canonically defined in `rocketmq-model`; `rocketmq-protocol::version` and `rocketmq-common::common::mq_version` are exact re-exports. Moving the definition into protocol would require model to depend on protocol and would reverse the approved foundation edge. The M04 text that listed `RocketMqVersion` among protocol-owned primitives is superseded by this dependency rule.

The M04 ownership guard asserts all four invariants: model does not depend on protocol, protocol depends on model, protocol re-exports the model version module, and common re-exports the same version values. Ordinal/name and exact-type compile tests freeze the wire contract.

## Remoting compatibility facades

| Surface | Canonical owner | Remoting compatibility | Removal condition |
|---|---|---|---|
| `RemotingCommand` root and deep paths | `rocketmq-protocol` | Exact re-export. Environment defaults are supplied by `remoting_command_facade` factories. | Factories may be retired only after the compatibility window and migration evidence. |
| `set_command_custom_header_origin(Option<ArcMut<_>>)` | `rocketmq-protocol` command, `rocketmq-remoting` synchronization adapter | Deprecated free function with the exact concrete legacy parameter. It cannot be inherent because Rust permits inherent methods only in the defining crate, and protocol must not depend on `rocketmq-rust`. This is the recorded source-compatibility exception; command type identity takes precedence. | Next major version after callers migrate to canonical header setters. |
| `DataVersion::new/next_version` clock behavior | `rocketmq-protocol` value with explicit timestamp mutation | `data_version_facade::new_data_version` and `DataVersionExt` own wall-clock reads. | After callers inject timestamps directly. |
| Elect-master and subscription defaults | `rocketmq-protocol` zero/explicit values | `header_facade` and `heartbeat_facade` supply current timestamps. | After callers construct explicit timestamps. |
| Subscription wrapper | `rocketmq-protocol` owned `HashMap` DTO | Legacy path retains `DashMap<CheetahString, Arc<_>>` plus bidirectional canonical conversion. | Next major version after public field users migrate. |
| Topic/static mapping wrapper | `rocketmq-protocol` owned `HashMap` DTO | Legacy path retains `DashMap<CheetahString, ArcMut<_>>` plus bidirectional canonical conversion. | Next major version after public field users migrate. |
| RPC response | `rocketmq-protocol` owned boxed header | Legacy path retains `ArcMut<Box<_>>` and converts at the owner boundary. | Next major version after public field users migrate. |
| Route slave fallback | `rocketmq-protocol` pure selection with caller-supplied index | `route_facade` chooses a random index and exposes the legacy extension method. | When all callers inject their chooser. |

## Protocol purity

Protocol schemas contain no `SystemTime`, `Instant`, environment, filesystem, Tokio, `ArcMut`, or common-owner imports. `LiteSubscription` creation and mutation require caller-supplied timestamps. Compression timing was removed from the canonical register-broker codec so measurement remains an owner concern.
