# Broker ACL users

Select and confirm a discovered master Broker before loading ACL users. Scope identity includes cluster name, Broker name, and address; the backend verifies current discovery before each operation. Changing scope or environment disposes its users and open forms. Local Dashboard login accounts use the separate Account page.

The authenticated commands are `list_acl_users`, `create_acl_user`, `update_acl_user`, and `delete_acl_user`. Their manager uses the common credential builder and participates in owned application shutdown. Writes hold the connection revision lease and audit user identity, target, and outcome without passwords or request bodies.

Password fields are input-only, masked, and kept in component memory. Neither responses nor persistence contain the submitted password. The current Web/core API sends a password on update, so both create and update reject blank passwords. An empty password is never interpreted as "keep existing." New users are enabled by the core create API; update requires a type and explicit enable/disable status. The form explains these semantics before submission.

Write failures retain form inputs and error information. Deletion confirms the username and Broker. An acknowledged write is followed by a user-list query; a failed read-back returns an independent warning while preserving the write receipt. Successful submissions clear the password and display the real returned list. Read failures require refreshing the list before another form can be opened.

## Validation

Focused fake-admin tests cover successful writes, rejected writes, failed read-back, blank passwords, changed target identities, and redaction. The ignored `local_acl_user_create_update_delete_round_trip` test uses the isolated [ACL Docker fixture](../deploy/dev/acl/README.md), creates a unique normal user, disables it, reads back its status, deletes it, and verifies absence. The development fixture test cleans up its generated identity.
