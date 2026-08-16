# RocketMQ Rust 1.0 release identity

## Decision

The 1.0 core release is prepared as the **RocketMQ Rust Community Distribution**. It is an unofficial community distribution and is not an official Apache Software Foundation release.

The machine-readable decision is `distribution/release-identity.json`. Its allowed identity choices and required fields are closed by `distribution/release-identity.schema.json`.

## Frozen publication surfaces

The approved community identity establishes the following ownership and namespaces for local release preparation:

| Surface | Frozen identity |
|---|---|
| Crate registry | `crates.io`, owner `mxsm`, logical namespace `rocketmq`, package prefix `rocketmq-` |
| Source repository and homepage | `https://github.com/mxsm/rocketmq-rust` |
| OCI candidate namespace | `ghcr.io/mxsm/rocketmq-rust` |
| Helm chart | `rocketmq-rust`, owned by The RocketMQ Rust Authors, explicitly marked as not an official Apache release |
| License and notice | Apache-2.0; notice owner The RocketMQ Rust Authors; upstream owner The Apache Software Foundation |
| Approval scope | `core-release-1.0` |

These values identify candidate metadata only. They do not publish crates, images, charts, Git tags, GitHub releases, or other remote artifacts.

## Required consumers

The identity preflight is a prerequisite for these release-preparation surfaces:

- crate package planning;
- binary archive construction;
- OCI layout construction;
- Helm candidate construction;
- legal, SBOM, and provenance preparation;
- public staged metadata preparation.

Each consumer must read the frozen identity rather than infer ownership from a local login, repository URL, package name, or environment variable.

## Approval and change control

Revision 1 was approved by `mxsm` on 2026-08-16 for `core-release-1.0`. The approval is recorded as an explicit release-approver decision, not as an implementation-agent assumption.

Changing the identity kind, an owner, namespace, repository, legal owner, annotation, or effective scope requires all of the following:

1. increment `revision`;
2. record the same value in `approval.approved_revision`;
3. obtain a new release-approver decision and update the approval date;
4. rerun the preflight and every affected release-preparation consumer.

The guard deliberately does not use source, file, image, or artifact digests as approval evidence. Git review and the explicit revision approval are the governance boundary.

## Preflight

Run from the repository root:

```powershell
python scripts/release_identity_guard.py --identity distribution/release-identity.json --stage preflight
```

The preflight fails when the identity is unset, approval is incomplete or future-dated, the approved revision differs, a required namespace or consumer is missing, community metadata claims an official Apache release, repository/license/NOTICE ownership drifts, the schema is open-ended, or digest-style identity fields are introduced.
