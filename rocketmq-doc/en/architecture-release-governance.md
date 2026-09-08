# Architecture Release Governance

Cargo manifests and `cargo metadata` are the source of truth for package
relationships. The former release-plan, dependency-baseline, and exact-edge
transition gates have been retired.

The maintained dependency checker enforces package layering, forbidden dependency
directions, composition-facade direction, and production cycles. Adding a valid
dependency or package does not require a historical snapshot update.

```bash
python scripts/architecture_dependency_guard.py --scope core-release
```

Use the maintained release preparation and package tools for version alignment,
publication ordering, artifacts, and installation smoke tests. Actual release
qualification and artifact integrity remain part of the explicit release process.
