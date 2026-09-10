import { invokeAuthenticatedCommand } from './invoke';
import type { AclScope, AclUser, AclUserChange, AclUserResult } from '../features/acl/types';
export class AclService {
    static listUsers(scope: AclScope): Promise<AclUser[]> { return invokeAuthenticatedCommand('list_acl_users', { scope }); }
    static createUser(request: AclUserChange): Promise<AclUserResult> { return invokeAuthenticatedCommand('create_acl_user', { request }); }
    static updateUser(request: AclUserChange): Promise<AclUserResult> { return invokeAuthenticatedCommand('update_acl_user', { request }); }
    static deleteUser(scope: AclScope, username: string): Promise<AclUserResult> { return invokeAuthenticatedCommand('delete_acl_user', { request: { scope, username } }); }
}
