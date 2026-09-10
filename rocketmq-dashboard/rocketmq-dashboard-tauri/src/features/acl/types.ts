export interface AclScope { clusterName: string; brokerName: string; brokerAddr: string }
export interface AclUser { username: string; userType: string | null; userStatus: string | null }
export interface AclUserChange { scope: AclScope; username: string; password: string; userType: 'normal' | 'super'; userStatus?: 'enable' | 'disable' }
export interface AclUserResult { scope: AclScope; username: string; operation: 'create' | 'update' | 'delete'; success: boolean; users: AclUser[] | null; readBackError: string | null }
export const aclScopeKey = (scope: AclScope) => JSON.stringify([scope.clusterName, scope.brokerName, scope.brokerAddr]);
