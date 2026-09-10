# Desktop entity navigation

The app store exposes `openTopic(name, detail)`,
`openConsumer(group, detail, proxyAddress?)`, `openBroker(address, detail)`, and
`goBack()`. Targets are discriminated by entity kind, contain an exact identity
and detail selection, and belong to the current connection environment. They
are browsing state, never authorization evidence.

Topic route and consumer panels link to their corresponding entities; Consumer
topic details link back to Topic. Read-only detail actions create history entries.
The Back control restores the source entry, including list selection, filters,
search, and pagination. History is bounded to twenty entries and discarded on
environment change or logout. Stale Broker responses cannot reopen closed sheets.
Each target is resolved against the loaded catalog; an explicit missing target
shows a not-found message rather than selecting the first row.

Sessions and Audit are available from Governance. Monitors and Storage are
reserved navigation types; their entries stay hidden until their pages are
implemented.

Validate from the app root with `npm run build` and
`npx vitest run src/stores/navigation.test.ts src/services/auth-session.test.ts`.
