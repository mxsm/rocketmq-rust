# RocketMQ Rust 1.0.0 publication handoff

The publication handoff is a local candidate artifact for the **RocketMQ Rust Community Distribution**. It is an unofficial community distribution and is not an official Apache Software Foundation release.

The handoff records the exact crate packages, service archives, local OCI layouts, Helm package, legal metadata, SBOM, provenance, evidence, and release documents that passed candidate preparation. It also carries a portable readiness marker after the final lifecycle transaction completes.

The future OCI namespace is `ghcr.io/mxsm/rocketmq-rust`. Its presence is descriptive only. Preparing or validating this handoff does not authenticate to a registry, publish crates, create a remote tag or release, promote images, publish a Chart, or mark version 1.0.0 as released.

A later, separately authorized publication task must use the handoff unchanged, reacquire trusted candidate sources, rerun the complete semantic verification, inject minimal credentials through a protected environment, and verify every remote publication surface after execution. Any source, feature, binary, package, image, Chart, or metadata change requires a new release candidate.

`PUBLICATION_READY.json` means the candidate may enter that separate publication task. It does not mean RocketMQ Rust 1.0.0 has been published.
