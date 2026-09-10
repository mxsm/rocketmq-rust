export interface AclScope { clusterName: string; brokerName: string; brokerAddr: string }
export interface AclUser { username: string; userType: string | null; userStatus: string | null }
export interface AclUserChange { scope: AclScope; username: string; password: string; userType: 'normal' | 'super'; userStatus?: 'enable' | 'disable' }
export interface AclUserResult { scope: AclScope; username: string; operation: 'create' | 'update' | 'delete'; success: boolean; users: AclUser[] | null; readBackError: string | null }
export const aclScopeKey = (scope: AclScope) => JSON.stringify([scope.clusterName, scope.brokerName, scope.brokerAddr]);

export type AclPolicyType = 'Custom' | 'Default';
export interface AclPolicyEntry { resource: string | null; actions: string[]; sourceIps: string[]; decision: string | null }
export interface AclPolicy { subject: string | null; policyType: string | null; entries: AclPolicyEntry[] }
export interface AclPolicyDraftEntry { resources: string[]; actions: string[]; sourceIps: string[]; decision: 'Allow' | 'Deny' }
export interface AclPolicyDraft { policyType: AclPolicyType; entries: AclPolicyDraftEntry[] }
export interface AclPolicyChange { scope: AclScope; subject: string; policies: AclPolicyDraft[] }
export interface AclPolicyDelete { scope: AclScope; subject: string; policyType: AclPolicyType; resource: string }
export interface AclPolicyResult { scope: AclScope; subject: string; operation: 'create' | 'update' | 'delete'; success: boolean; policies: AclPolicy[] | null; readBackError: string | null }
