# RocketMQ Dashboard (GPUI)

Native RocketMQ desktop dashboard built with GPUI and gpui-component. This is a
**standalone Cargo workspace**, using Rust 2024 and its checked-in toolchain.
It is not a member of the repository's root Cargo workspace.

## Run and build

Run these commands from the repository root, then remain in the GPUI directory:

```bash
cd rocketmq-dashboard/rocketmq-dashboard-gpui
cargo run
# Optimized executable
cargo build --release
```

A graphical desktop is required to run the application. Install the native build
prerequisites for the target platform: Xcode command line tools on macOS, the
MSVC C++ toolchain and Windows SDK on Windows, or the Linux GUI development
libraries described in [AGENTS.md](AGENTS.md). Build timings and memory usage
depend on the platform and workload.

The application starts in native fullscreen. Use the **Exit fullscreen** / **Enter fullscreen**
button to switch between fullscreen and the normal window. The button is keyboard accessible;
**Escape** continues to close dialogs and navigation drawers.

The Windows executable embeds its application icon from [assets/app-icon.ico](assets/app-icon.ico).
The original artwork and generation prompts are kept in [assets/](assets/app-icon.md).

Logging uses `RUST_LOG`. For example, in PowerShell from this directory:

```powershell
$env:RUST_LOG = "info,rocketmq_dashboard_gpui=debug"
cargo run
```

## Topic fields and mutation support

Topic forms resolve cluster shortcuts to exact Brokers before submission. Editing
starts with one Broker and can select other Brokers from the same authoritative
configuration snapshot. Every selected Broker retains its reviewed version; only
changed fields are applied, preserving each Broker's other settings and attributes.

| Field | Create | Read | Update / restriction |
| --- | --- | --- | --- |
| Topic name | Yes | Yes | Immutable; editing cannot rename or create a missing Topic |
| Broker placement | Exact inventory targets | Per Broker | Select existing configuration targets; cluster shortcuts select their exact Brokers |
| Read / write queues | 1–128 each | Yes | Version-checked changed fields |
| Permission | Read, write, inheritance bits | Yes | Version-checked; at least read or write is required |
| Ordering | Ordered / unordered | Yes | Broker state uses CAS; NameServer order entries use a preflight comparison and can fail separately |
| Message type | Normal, FIFO, Delay, Transaction | Yes | The same types and Unspecified use supervised state CAS; unknown and internal contracts are unavailable |
| Send Trace | Defaults to off | Active Send form | The actual checkbox value is sent with the message |

Full-field editing requires Brokers implementing the supervised Topic state
protocol. Older Brokers reject it; the desktop does not fall back to an unchecked
upsert. Broker and NameServer outcomes remain visible individually after partial
success. A version conflict preserves changed draft fields and requires an explicit
choice to keep or adopt authoritative values. Only conflicted Brokers remain
selected after rebasing, so an already applied or unknown write is not replayed.
Message bodies are omitted from summaries and cleared when the Send dialog closes.

## Monitor rule configuration

The Monitor rules page lists and searches consumer-group threshold configurations
for the selected environment. Min Count is a nonnegative consumer count (up to
2,147,483,647); Max Diff Total is a nonnegative backlog message count (up to
9,223,372,036,854,775,807). This page configures rules only: it does not evaluate
metrics, produce alerts, or send notifications.

Rule identity is Environment ID plus Consumer Group. Creating requires revision
zero; editing and deleting require the observed positive rule revision. Conflicts
show the authoritative state, preserve the open draft, and require an explicit
revision reconfirmation followed by another submission. A deleted rule cannot be
silently removed again, and recreated rules receive fresh revisions. Rule write
times are displayed in UTC.

Legacy entries retain their original ID, Group, and enabled setting. They remain
marked as needing thresholds, or environment assignment and thresholds, until
the user explicitly completes them. Empty legacy thresholds and unknown write
times are never replaced by valid-looking defaults. An unassigned entry can be
assigned explicitly to the selected environment from its completion dialog.

Accepted changes remain application-owned when a dialog closes. Rule state,
its local operation record, and its terminal P11 audit event share one canonical
atomic file replacement; configuration revision checks also prevent writes after
the connection or environment changes.

## Consumer and Producer workspaces

Consumer creation uses an editable form and explicit Broker checkboxes. Cluster
shortcuts select their displayed members; the confirmed subset is the write target.
The form requires complete NameServer Broker target discovery, independently of
unrelated Groups' Clients and Progress read quality. Every selected target still
requires an exact Group absence preflight; unknown Group values remain unknown.
Group membership discovery is tracked before per-Group diagnostic reads. Applied
mutations can therefore reload an authoritative Group catalog while retaining
Partial diagnostic quality and unknown client or lag values. Deletion still checks
the exact current membership and live connections; only the Broker's exact
ConsumerNotOnline response establishes an empty active-client set.
Mutation summaries count Broker/group targets separately from internal-topic cleanup
steps. Cleanup failures and uncertainty still make the workflow Partial. New audit
events store cleanup step counts separately; historical terminal facts are retained.
The five numeric controls cover retry queues, maximum retries, consume timeout,
Broker ID and slow-consumer Broker ID. Four flags control enabled, minimum-offset,
broadcast and orderly consumption. Maximum retries accepts -1 or greater, retry
queues accepts zero, and timeout must be positive.

Consumer editing starts from one Broker's versioned snapshot and submits only
changed fields under the existing state CAS. It preserves the Broker's notifications,
system flag, retry policy, subscriptions and additional attributes. Brokers without
that CAS capability return unavailable. A conflict retains the reviewed draft.
Persistence problems retain confirmed
in-memory effects and never count as durable success. An applied write with a
persistence problem is Partial; a rejected write with failed persistence is Failed.
Uncertainty without confirmed application is Unknown. The same form cannot replay
these outcomes. Local audit records retain field names and typed
outcome facts without configuration values or diagnostic text.

Configuration details show consumption, routing, retry policy, subscription Topics,
redacted attributes and cross-Broker differences. Failed targets stay separate from
observed configurations. Clients include addresses and subscriptions; progress keeps
signed lag and queue timestamps. Running Info limits each structured section to 256
rows and 64 KiB combined metadata. JStack stays only in the active detail and is
cleared on tab, scope, environment, session or detail changes.
Long Group and Client identities have full-value copy controls. Client fields and
subscriptions wrap within the sheet; raw diagnostic lines have a bounded scroll
area, and the inventory table scrolls horizontally at narrow window widths.

The Consumer query-scope control affects inventory, clients and progress reads.
Configuration and diagnostics use real Broker Direct targets discovered through the
selected NameServer, including when Consumer reads are configured for Proxy.
This Direct read evidence does not enable unsupported Proxy mutations, and a
missing selected Proxy never falls back to a default endpoint.

Producer inspection discovers real Topic candidates, accepts group row selection,
and freezes Topic and Group together when Apply is pressed. Discovery failures keep
a validated manual query available. Client details provide copy controls for client
ID, address, language and version. Late results from a previous query or environment
cannot replace the current applied pair.

## Architecture and configuration

[src/main.rs](src/main.rs) initializes the GPUI component library and application
root, installs logging, and composes the desktop services. The layers are:

```text
GPUI views
  -> application services
  -> GpuiAdminProvider / configuration, history and monitor stores
  -> rocketmq-admin-core client adapter
  -> application-owned ClientRuntime
```

The shared dashboard models and admin facade come from
[rocketmq-dashboard-common](../rocketmq-dashboard-common). The desktop provider
implements live cluster queries and administration; configuration, history and
monitor persistence remain local to this application. See
[src/services](src/services) and [src/infrastructure](src/infrastructure) for
the implemented operations and their lifecycle owners.

The private [DashboardBrokerOperations](src/services/dashboard_broker.rs) boundary forwards
Dashboard overview/current/history and Broker inventory/runtime/config reads, plus guarded
Broker config patches. Its [Admin provider](src/infrastructure/admin_provider/dashboard_broker.rs)
keeps raw Admin responses inside infrastructure and returns allowlisted DTOs to the services.

Configuration defaults to `rocketmq-dashboard/gpui/config.json` beneath the OS
user configuration directory. Set `ROCKETMQ_DASHBOARD_GPUI_CONFIG_PATH` to
override the complete file path. NameServer selection and connection settings
are stored in this document.

OPS → Connection manages named environment profiles. Creating or selecting a
profile explicitly changes the environment; editing its NameServer, Proxy,
TLS, or VIP settings preserves its persistent identity. History and Monitor
rules belong to that identity, while the configuration revision identifies a
particular connection generation.

The version 4 configuration document holds profiles, global settings, Monitor
rules, local operation records, and the desktop audit journal in one atomic replacement. Metric samples
remain in the separately versioned `history.json`. Version 1 configuration and
history files, version 2/3 profile documents, and legacy Monitor arrays are migrated on startup. The nested
Monitor document is version 2. Migration uses the same atomic candidate replacement;
newer unsupported versions and failed replacements preserve their source. Legacy points
and rules without environment evidence remain unassigned and are excluded from
the selected environment. Legacy rules also require thresholds before they can
be evaluated; the Monitor lifecycle still performs no evaluation. The original
`monitors.json` is preserved after its rules are imported into the configuration
document. Unsupported schemas or failed migration writes preserve their source
files and report a recoverable startup error.

One application instance owns the local data directory for its lifetime through
an OS file lock. A second instance using that directory must wait until the first
closes. An interrupted write leaves either the previous complete document or the
new complete document; unfinished candidate files are not treated as committed
state. Configuration and Monitor changes carry their operation record in the
same commit as their terminal audit event.

Accepted Broker, Topic, Consumer, Monitor, and configuration changes belong to the application,
so closing a Sheet or signing out only drops the old view's result receiver. External
changes first save an accepted-operation record and then save one terminal outcome;
these two writes do not provide a transaction with RocketMQ. Shutdown drains owned
work for the existing five-second budget while local storage and the provider remain
available. Interrupted requests stay pending for authoritative verification after restart,
and are never automatically replayed. Local login and logout also record safe outcomes.

Audit events contain trusted local actor and environment metadata, controlled resource
identities, and bounded result facts. They exclude message bodies, diagnostics,
credentials, and configuration values. Topic field edits retain only the requested
field names with their accepted operation, including pending and failed requests.
Partial effects, unconfirmed writes, and authoritative refresh failures remain
separate result facts. A failed terminal audit write preserves the
business result and displays an application warning; **Retry audit write** only retries
persistence. The journal retains at most 10,000 terminal events within 31 days and
128 pending requests. Service queries support bounded filtering and stable cursors;
the Audit query page and future DLQ and ACL editor hooks are later
deliveries. These records describe this desktop's operations and are not a tamper-proof
or organization-wide audit log.

Login events preserve their initiating actor and include the verified operator as a
separate terminal fact after successful authentication. Failed form usernames are not
retained. Exhausting the terminal-write recovery queue still permits local sign out;
one reserved logout fact remains visible and retryable without retaining a command.
Cursor continuation expires after journal retention evicts rows, including partial
eviction of unread results.

Authentication has two separate settings in the configuration:

- When local login is enabled, `ROCKETMQ_DASHBOARD_USERNAME` and
  `ROCKETMQ_DASHBOARD_PASSWORD` must be present. Only the local session marker
  is retained after authentication.
- When the Admin credential source is `environment`,
  `ROCKETMQ_ADMIN_ACCESS_KEY` and `ROCKETMQ_ADMIN_SECRET_KEY` are required;
  `ROCKETMQ_ADMIN_SECURITY_TOKEN` is optional. These credentials authenticate
  outbound RocketMQ requests.

The [auth owner](src/infrastructure/auth_state.rs) resolves secrets for immediate
use; the configuration file stores the credential-source choice. Background
admin and persistence work uses injected child scopes from the desktop runtime.
The application closes this runtime when the GPUI event loop exits.

## Development

### Dashboard metrics and local history

Topic Current keeps fractional rates and distinguishes observed zeroes from unavailable
offsets or rates. It excludes system, retry and DLQ Topics. Each refresh reads rates for
up to 200 application Topics in name order, with a 2,000 RPC budget; it selects at most
20 by 24-hour production, then current production rate, then name. Those selected
Topics receive offset-statistics queries and are ranked by stored message count.
Incomplete or omitted coverage is shown as a partial ranking; this bounded window
does not claim to find the busiest Topic outside its coverage. One Topic failure keeps
the other observations available.

Backlog requires complete Consumer-group discovery and a successful consumption
observation from every discovered master target. Missing targets remain unknown;
negative lag keeps its observed meaning.

History uses an explicitly selected UTC calendar day. All topics shows registered
Topic count, while a specific Topic shows its stored messages. Broker history offers
registered Broker count and per-Broker production or consumption rates. Each query
returns at most the latest 1,440 real observations, with UTC time labels, units and
paged exact values. Refresh errors retain the previous successful page and its time.

The collector is disabled by default. Set `settings.foundations.historyEnabled` to `true` in
`config.json`, with a positive `settings.foundations.historyIntervalSeconds`, then reload configuration.
The page shows disabled, warming, normal, partial or stale collection and the latest
successful collection time for the active environment in this process. A sample is
stale after three collection intervals. Existing retention limits and environment
attribution apply to both count and resource series; missing samples are not filled.

From this directory, select the checks relevant to a change:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo test --locked <test_name>
```

See [AGENTS.md](AGENTS.md) for platform setup and graphical smoke checks.

## Message workspace

Messages supports Topic/time-range, Topic + Key and Topic + Message ID searches in
NameServer Direct scope. UTC input uses `YYYY-MM-DD HH:MM:SS`. Search applies the
current draft; Refresh replays the applied query. Changing filters, page or connection
clears selection and cancels obsolete reads. Range pages scan Broker queues and sort
only the returned page by stored time. The SDK does not provide an exact grand total;
a full page permits trying the next page, which may be empty. Key searches return at
most 64 matches.

A searchable Topic picker loads real inventory and preserves validated manual entry
when discovery fails or is partial. Lists retain bounded Tags, Keys and host metadata only;
Overview includes readable UTC times and a Copy Message ID control. Properties and
Body load when their inspector tab opens;
one inspector retains at most 256 KiB of body and 128 properties / 64 KiB. JSON and
plain-text previews show truncation. Closing the inspector, navigating away, changing
connection or signing out clears its payload. Trace remains unavailable until P07.
Physical offsets are read through the selected Topic's advertised Broker addresses,
including hostname aliases. Returned Topic and physical ID are verified before details
or direct consumption; the embedded address never supplies a transport target.

Direct consume requires confirmation of the exact Message ID, Topic, environment,
Consumer group and optional Client ID. An admitted attempt stays owned by the
application and writes its terminal P11 audit fact even if the inspector closes.
The verified Broker validates the selected client and forwards consumption over its
registered Consumer channel; the Consumer's reported socket is never an RPC target.
Only `CR_SUCCESS` confirms success; other results and unconfirmed calls remain
distinct. Consumer remarks are represented by presence, without displaying arbitrary
private content. No new Message ID is promised.

## License

Apache License, Version 2.0.
