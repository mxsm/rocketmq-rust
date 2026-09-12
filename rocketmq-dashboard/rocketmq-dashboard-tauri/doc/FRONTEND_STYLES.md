# Frontend style ownership

The desktop shell and migrated pages use document-level `--ops-*` tokens. Add page-specific rules beside the owning feature or page instead of restoring global selectors around the whole application.

## Entry-point order

`src/main.tsx` loads these layers in order:

| Layer | Responsibility |
| --- | --- |
| `index.css` | Existing checked-in utility output and reset, including `sr-only` and utility animations. It is not a live Tailwind compiler. |
| `styles/legacy-details.css` | Retained presentation for `components/ui/SideSheet.tsx`, exported through `components/Common.tsx`. This compatibility component is not used by the migrated routes. Keep its styles until that export is retired deliberately. |
| `styles/tokens.css` | Document colors, font stack, root height, focus ring and the token bridge consumed by shared Radix controls, including their portals. |
| `styles/shell.css` | `desktop-*` layout, navigation, toolbar, account menu and shell breakpoints. |
| `styles/controls.css` | Shared `ops-*` controls, table primitives, status/empty/error states, pagination and Radix portal behavior. It also owns the shared behavioral overrides for the compatibility detail sheet. |
| Feature/page CSS | Locally imported layout for the owning page, dialog or inspector. `styles/auth.css` is imported by the authentication layout and password dialog. |

The former `redesign.css` is removed after all page migrations have been combined. Its old `app-*`, `auth-*`, `dashboard-*`, NameServer/Proxy/Cluster and messaging/ACL/account selectors have no remaining component consumers. The shell no longer carries `app-shell`, `dashboard-main` or `app-nav-*` aliases. Unused legacy page animations and duplicate pagination rules are removed; pagination is styled in `controls.css`.

Keep the `LegacyButton`, `LegacyInput` and other consumed adapters: their filenames preserve existing import paths, while their rendered controls already use the shared design tokens. The Radix/Tailwind variable bridge remains necessary for utility-based controls and must not be removed solely because its comment mentions migration.

## Request and rendering ownership

- `useReadResource` owns a region's request. Repeated reads share the in-flight promise; cleanup invalidates the generation so late results cannot update a replacement page.
- NameServer and Proxy own their polling intervals in their hooks and stop their controllers on cleanup. The global toolbar registers the active page's existing read action.
- ACL and Monitors subscribe to connection changes only while mounted, invalidate old observations on scope changes, and invalidate a pre-write read before requesting the post-write observation.
- Storage's observation-age timer and the Sessions expiry timer update presentation only and are cleared on unmount.
- Dashboard analysis charts mount only when their section is expanded; each chart uses its existing responsive container. Preserve explicit chart dimensions and disabled data animation. Stored history uses cursor requests and explicit loading of older samples.
- Audit and Sessions use opaque cursor paging; entity lists retain their existing pagination and detail selection. Message body and Trace detail inspection remain scoped to the selected target.
- Accepted mutations retain their captured target in session-owned providers outside the route. A result from an earlier environment must remain labelled with that target and cannot refresh the new environment's list.

These are code-level ownership boundaries, not a substitute for browser interaction or performance measurements. Verify actual small-window layout, long content, keyboard focus, viewport changes and route transitions in the selected browser, then repeat the required Windows/WebView2 checks before final acceptance. Do not introduce virtualization or new rendering dependencies without a measured problem.
