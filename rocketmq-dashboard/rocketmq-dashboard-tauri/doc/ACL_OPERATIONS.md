# Broker access control

The Access control page manages RocketMQ-Rust Broker identities and resource
policies. Dashboard login accounts are a separate identity system.

## Broker selection and reads

Select a Cluster and an explicit master Broker. Discovery identifies the target
by Cluster name, Broker name and address. Switching that target clears the old
selection and details. Each mounted scope owns its user and policy reads; a
late response cannot populate a replacement scope or connection revision.

Users and policies load independently. A denied or failed query remains visible
with its reason. The page may retain its last successful observation, but disables
mutation controls until the corresponding directory refresh succeeds. A failed
read is not presented as an empty directory.

The environment toolbar refreshes the selected Broker's users and policies.
Before scope selection it refreshes Broker discovery. The separate Refresh Brokers
action updates available scopes. Returning through navigation restores the selected
Cluster and Broker, then reads fresh directories. After a write, outstanding older
reads are invalidated before the post-write refresh.

The Users section combines a directory, selected-user details and resource
policies for the exact `User:<username>` subject. Policies can also be inspected
and edited independently in the Policies section. The page does not infer or
display account creation times, update times or saved passwords.

## User changes

Create, edit, password change and deletion retain the selected Broker scope.
Creation enables the new user. An update cannot rename the existing username.
Unrecognized type or status values require an explicit selection in Edit; they
are not replaced with permissive defaults.

The current Broker API requires a new password for every user update. Password
change preserves the recognized type and status. Passwords start empty and are
never loaded from a user record. Confirmation metadata and retained receipts
exclude password values and raw request or response objects. The editable form
is disposed after submission or dialog closure.

## Resource policy changes

Policy identity consists of subject, policy type and resource. Editing preserves
all original resource identities and exposes actions, source IPs and the Allow or
Deny decision. Removing an existing resource uses its separate deletion action.
Duplicate resource identities, missing resources/actions and malformed subjects
are rejected before dispatch. A deletion never substitutes the current row index
for the resource identity.

## Confirmation and outcomes

Confirmation fixes the original environment, connection revision, Broker and
requested identities. A connection change prevents a new submission from that
dialog. Once accepted, a write remains owned by the session until it settles,
even when navigation or connection settings change. Pending writes block dialog
dismissal; each confirmation submits at most once and has no automatic retry.

A receipt confirms success only when the returned operation, Broker scope and
username or subject match and the acknowledgement explicitly succeeds. A
transport failure or mismatched result is unconfirmed. A successful write whose
follow-up directory read fails remains acknowledged, with a separate read-back
warning. The current directory can be refreshed without resubmitting the write.

The latest receipt survives route and connection changes and displays its original
scope. A new authenticated session disposes that receipt. A later write replaces
it only when the later operation settles.

## Validation

Frontend validation runs from the Tauri app root:

```text
npm run build
npm test -- --run
```

Focused model coverage exercises immutable usernames and policy resources,
unknown user enums, password exclusion, fixed Broker scopes and the distinction
between unconfirmed writes and acknowledged writes with failed read-back.
Shared read and operation controllers cover cancellation and accepted-write
ownership.

Visual acceptance uses the selected ACL reference, including narrow-window and
keyboard checks. Live ACL acceptance uses the isolated
[RocketMQ-Rust ACL fixture](../deploy/dev/acl/README.md); frontend fixture data
does not establish real Broker authorization coverage.
