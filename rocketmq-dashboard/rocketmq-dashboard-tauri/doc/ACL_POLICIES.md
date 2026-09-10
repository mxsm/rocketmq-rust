# ACL policy management

The ACL page provides separate Users and Policies views after confirmation of a discovered Broker scope. Policy forms edit subject, policy type, resources, actions, source IPs, and decision. Multi-resource entries remain separate resources on the wire. Existing resource identities remain fixed while editing; resource deletion uses its own confirmation dialog.

The commands `list_acl_policies`, `create_acl_policy`, `update_acl_policy`, and `delete_acl_policy` share the ACL manager's lifecycle, common credentials, connection revision lease, audit boundary, and independent read-back results. Scope changes dispose lists and forms, and completed requests from a disposed view cannot update the next scope.

Deletion requires the original subject, policy type, and nonempty resource. The backend confirms that exact identity in a fresh policy listing before sending the write. It does not interpret an empty resource as deletion of the entire subject. The client/admin-core typed entry-delete extension carries `policyType` in the existing `DeleteAclRequestHeader`; legacy deletion calls keep their original behavior. This distinguishes Default and Custom policies containing the same resource.

A rejected write leaves the form open with its inputs. An acknowledged write updates the list from a separate read-back; if that read fails, the result retains the acknowledgement and asks for a refresh. Audit records policy identity, Broker target, and outcome without credentials.

## Validation

The focused tests exercise multi-resource conversion, duplicate identity rejection, original-identity deletion, and preservation of the policy-type wire header. The ignored `local_acl_policy_typed_delete_preserves_other_policy_and_resource` test uses the isolated ACL fixture to create a unique user, create Custom and Default policies, update one resource, delete the Default entry, and verify that Custom resources survive. It then removes its generated ACL and user.
