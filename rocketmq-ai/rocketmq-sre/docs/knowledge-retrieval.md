# Reviewed knowledge retrieval

Knowledge is read-only diagnostic context. A reviewed item, a caller's human
confirmation declaration, and a matching version requirement do not authorize
execution or attest to a physical target's version. AI material cannot be
imported as Validated; promotion requires the existing explicit review lifecycle.

## Search and completeness

`GET /v1/knowledge/search` requires an authorized cluster, an explicit semantic
RocketMQ version and a query of at most 500 UTF-8 bytes. The supplied version is
knowledge applicability context; the server does not infer physical version
attestation from it. Tenant-wide material remains readable alongside material
for the requested cluster.

PostgreSQL `simple` full-text ranking is combined with a case-insensitive,
literal whole-query substring fallback. This covers measured Chinese substring
cases without promising semantic, synonym or embedding retrieval. Punctuation
in the fallback is literal, including `%`, `_` and backslash.

The server materializes at most 1024 candidates plus one sentinel after the
tenant, scope, component and review-inclusion filters. It applies version and
eligibility rules within those candidates and returns eligible material before
ineligible context, preserving ranking within each class. This bound does not
limit how many underlying rows PostgreSQL scans. A transaction-local statement
timeout bounds the SQL operation; a timeout is unavailable, never a successful
empty search.

The response binds the actual tenant, cluster, trimmed query, component, version,
limit and inclusion flag to `observed_at`. `candidate_limit_reached` means the
candidate budget was exhausted; unseen applicable material may remain.
`results_truncated` reports additional matches inside the evaluated candidates.
Neither an exhausted empty result nor `partial=false` proves corpus absence.
The compatibility field `partial` means a returned row is ineligible.

Returned rows expose review status, conflict, version range, validity dates,
review due date and server exclusion reasons. Historical oversized text is
bounded and explicitly marked truncated and ineligible; its original stored
hash and bytes are not rewritten. Consumers must use `source_uri_display` or
hide the source. That display projection removes HTTP(S) userinfo, query and
fragment and rejects unsupported display schemes. The original declared URI
remains in existing API fields for compatibility.

The model consumer admits only server-eligible rows and retains their DataClass
classification for the existing provider policy. Internal prompt metadata
distinguishes an observed search, including its bounds, from unavailable
retrieval. Model queries also respect the 500-byte UTF-8 boundary. No external
model or embedding call is needed to evaluate retrieval.

## Curation and feedback

Direct Knowledge import and review require the literal `operator` role plus
the exact requested cluster authorization. Tenant-wide import or review also
requires the existing `sre-admin` role: both roles are necessary. Having access
to the currently listed clusters does not grant tenant-wide write authority.
Role and scope checks run before import chunking or writes.

The existing manually confirmed Postmortem publication path preserves its prior
role aliases, confirmation, immutable revisions and transactional revision CAS.
This compatibility exception does not grant direct Knowledge curation or
automatic publication of AI output.

Publication also keeps its literal single-chunk Markdown layout and full-content
hash. User text inside a valid confirmed revision is not reinterpreted through
the direct importer's heading or chunk-count restrictions. Oversized publication
chunks remain readable as bounded, explicitly ineligible context; their stored
content is not truncated or rewritten to satisfy the display bound.

Readers may append useful, incorrect or outdated feedback. Feedback does not
promote an item, change its review state or establish shared eligibility.
`human_validated`, `human_confirmed` and `ai_generated` are declarations; the
stored actor is the authenticated subject, not proof of external human review.

## History and source identity

`GET /v1/knowledge/{id}/history` authorizes the item before reading history.
It returns at most 32 review records and 32 feedback records, with separate
truncation flags. Stored feedback tenant and cluster scope are filtered before
the limit. Typed feedback tasks are excluded from the review stream because
that task table has no independent feedback scope; the scoped feedback stream
is authoritative for their display. No task is deleted by this projection.

New direct import and review records carry typed actor, status, hash and
declaration metadata. Older nullable metadata stays unknown; the reader never
extracts identities or scope from a reason prefix or treats owner as reviewer.
Legacy opaque tasks cannot establish review provenance. Actual Postmortem
lineage requires the stored published-item relation, matching tenant/cluster
and the current immutable human-confirmed revision; a declared URI alone is
insufficient.

New automatic conflicts compare exact tenant, optional cluster, source URI and
source version with a different content hash. Different legitimate source URIs
are independent sources. Existing conflict flags are preserved and are not
bulk-cleared. Exact duplicate imports reuse the existing item, status and
stored chunk count; new declarations do not silently promote it.

## Migration and deployment

[Migration 0065](../migrations/0065_reviewed_knowledge.sql) preserves existing
IDs, content, hashes, references and conflict flags while adding nullable typed
history metadata. It replaces tenant-only source uniqueness with two partial
unique indexes: one for a specific cluster and one for tenant-wide material.
This does not require PostgreSQL 15 `NULLS NOT DISTINCT` syntax or a sentinel
UUID. Knowledge import and Postmortem publication use the same bounded
transaction advisory lock for exact source identity; the digest is only a lock
key, never authority. Publication refuses a colliding existing source instead
of adopting an arbitrary imported item.

Deploy the migration and compatible writers together. Old binaries deduplicate
across clusters and are not safe writers after the new scope contract is in
use. Once legitimate cross-scope rows coexist, restoring the old unique
constraint may fail. A controlled rollback must reconcile that incompatibility
without silently deleting, rekeying or rewriting history. Do not alter old
migration checksums or strip typed provenance to make an old binary work.

Focused regressions live under
[`knowledge/retrieval_tests`](../crates/rocketmq-sre-control-plane/src/knowledge/retrieval_tests.rs).
Their PostgreSQL corpus and relevance expectations are explicit synthetic
engineering fixtures, not production qualification or general retrieval-quality
certification.
