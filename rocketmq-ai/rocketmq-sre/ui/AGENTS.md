# AGENTS.md

## Scope

This file applies to `rocketmq-ai/rocketmq-sre/ui/`.

## Project role

- This is the standalone React, TypeScript, and Vite UI for RocketMQ-Rust AI SRE.
- It talks only to the versioned RocketMQ SRE Control Plane API.
- It must not call RocketMQ Dashboard mutation APIs or share Dashboard sessions.

## Working agreement

- Keep the interface dense, desktop-first, and operational.
- Read and diagnosis surfaces remain read-only. Phase 3 approval and execution
  controls must call only the versioned Control Plane API and must never expose
  shell, raw request, arbitrary patch, or target credential input.
- Keep API DTOs under `src/api/` and page orchestration under `src/pages/`.
- Never display credentials, tokens, message bodies, ACL/TLS material, or whole configurations.
- Do not commit `dist/`, logs, or local environment files.

## Native WebMCP

Use the [WebMCP draft](https://webmachinelearning.github.io/webmcp/) from the W3C Web Machine
Learning Community Group. The 24 November 2026 publication is a Draft Community Group Report
outside the W3C Standards Track. Its imperative API is the baseline for this UI.

- Each SRE UI page with a useful read-only task must expose that task as a native WebMCP tool.
  For a page limited to sign-in, approval, or execution, keep the consequential action in its
  existing human workflow. Expose a read-only summary only when the page already offers one.
- Feature-detect `document.modelContext?.registerTool` in a secure context. Register after the
  route, session, and required scope are ready. Keep normal navigation, forms, and error handling
  usable when the API is absent or registration is rejected.
- Give each tool a stable, unique name, a description of its scope and result, an object
  `inputSchema` with explicit bounds on accepted arguments, and an async `execute` callback.
  Validate arguments at invocation and set `annotations.readOnlyHint` only for side-effect-free
  reads. Starting an Ask SRE conversation, for example, creates a record. Return a small
  allowlisted result that preserves relevant freshness and partial-data indicators.
- Reuse the page's authenticated data provider and typed client in `src/api/`. Resolve tenant,
  cluster, and permissions from the current session and workspace. The Control Plane enforces
  authorization on every request. Keep tokens, arbitrary URLs, raw requests, and credentials out
  of tool inputs, results, and errors. For example, an incident-list tool can reuse
  `api.listIncidents(scope.clusterId, signal)` and return filtered incident IDs, severities, and
  `partial` for the selected cluster.
- Await `document.modelContext.registerTool(tool, { signal: registration.signal })` and abort
  the registration controller on unmount, sign-out, or scope change. Forward the separate
  `execute(input, { signal })` cancellation signal to async reads. Cancel or discard results
  from the previous scope even if a call was already running when its tool was removed.
- Chrome also experiments with `toolname` and `tooldescription` on HTML forms. These attributes
  are outside the current draft. If used, preserve manual submission for consequential actions
  and expose each operation once. See the [Chrome implementation notes](https://developer.chrome.com/docs/ai/webmcp/updates)
  and [declarative form guidance](https://developer.chrome.com/docs/ai/webmcp/declarative-api).

For a page change, verify that the active tool is discoverable and returns the same scoped data
as the UI. Cover invalid inputs, denied access, route remounts, stale results after a scope
change, and the manual path without WebMCP in focused tests. In a supporting browser, discover
the tool with `await document.modelContext.getTools()` and pass the returned tool object to
`await document.modelContext.executeTool(tool, input)`.

## Development validation

Reuse installed dependencies; run `npm ci` only when missing or when the lockfile changes.
Select checks for the actual change from this directory:

- `npm run check:api` for OpenAPI or API-client changes.
- `npm run lint` for changed frontend code.
- `npm run test -- --run` with the relevant existing test filter for behavior changes.
- `npm run build` for TypeScript, routes, shared UI, or build configuration changes.

The full list belongs to broad frontend integration/CI, not every small edit. Instruction-only changes
need only document checks. Preserve the Control Plane and sensitive-data boundaries above.
