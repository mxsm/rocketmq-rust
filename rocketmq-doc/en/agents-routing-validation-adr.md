# ADR: AGENTS Routing Validation

## Status

Accepted; revised 2026-09-08 for scoped development validation.

## Context

The root `Cargo.toml` owns the main workspace. Standalone Cargo, Node, and Docusaurus projects
have different commands and platform requirements. Agents need reliable ownership routes without
automatically running every project, feature matrix, or historical architecture check.

Checking exact AGENTS sentences and command strings coupled the routing scripts to policy wording.
Invoking dependency metadata from a routing check also introduced Cargo/toolchain work for instruction
edits. Neither is needed to determine where a project belongs.

## Decision

Root `AGENTS.md` provides common engineering rules and one project map. The nearest local guide
selects project-specific checks and replaces the root validation fallback. It does not stack complete
root, local, and specialized profiles.

For normal development, validate compilation, formatting, and relevant behavior in the affected scope.
After those checks pass, finish unless new edits, failures, or a concrete unresolved risk justify more
work. Shared changes require relevant consumers, selected from actual API, feature, and behavior impact.

Keep specialist and integration commands in [the validation reference](agent-validation-reference.md).
Full-workspace checks, dependency metadata audits, and long-running suites belong to the corresponding
integration, CI, or release task. Instruction changes do not by themselves require Cargo or Node builds.
Routine development does not require SHA, fingerprints, a clean worktree, or historical baseline closure.

The equivalent `scripts/check-agents-routing.ps1` and `scripts/check-agents-routing.sh` check only:

- Known standalone project routes and their local instruction files.
- Discovered standalone Cargo projects with an explicit `[workspace]` and their root/local routes.
- Discovered `package.json` projects and their root/local routes.
- Required workflow files and the routing/reference documents.

The scripts do not enforce AGENTS wording, exact command profiles, workflow step contents, or dependency
trigger closure. They do not invoke Python, Cargo metadata, builds, tests, or architecture baseline checks.
The standalone metadata guard remains available as an explicit integration tool.

Run the lightweight router after changing project layout, AGENTS routing, or either routing script.
Use the appropriate platform command, plus `git diff --check`:

```powershell
.\scripts\check-agents-routing.ps1
git diff --check
```

```bash
bash ./scripts/check-agents-routing.sh
git diff --check
```

When editing the router, use `python -m unittest discover -s scripts/tests -p test_agents_routing.py`
to exercise both available shells against temporary repositories. On Windows, set `AGENTS_TEST_BASH`
to Git Bash's executable if the default `bash` is a WSL launcher; `AGENTS_TEST_PWSH` can select PowerShell.

## Consequences

Project ownership remains visible and missing instruction files are caught without native build setup.
Local work can finish with evidence proportional to the change. Security, compatibility, unsafe-code,
and task-lifecycle contracts still apply; simplifying development gates does not relax product boundaries.

Both routing implementations must stay aligned when project topology changes. Structural checks cannot
prove test adequacy or complete workflow path filters; use the separate integration tools when reviewing
those properties. Existing workflow definitions are unchanged by this policy.
