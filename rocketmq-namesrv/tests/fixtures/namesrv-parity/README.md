# NameServer parity fixture

`manifest.json` is the stable input contract for mixed Java/Rust NameServer
smoke runs. The harness records the fixture SHA-256, repository commit, mode,
seed, endpoints, response code, opaque value, body size, and body CRC32 under
`target/namesrv-parity/`.

The P0 smoke sequence is:

1. register a synthetic broker and Topic;
2. query the Topic route;
3. update the Topic registration with a new `DataVersion`;
4. query the updated route;
5. unregister the broker;
6. wait until the Topic route is absent.

The fixture is a compatibility smoke test, not a NameServer throughput result.
Full snapshot/delta/chunk differential corpora are introduced by the P2 plan.
