import type { AclPolicy, AclPolicyChange, AclPolicyDelete, AclPolicyDraftEntry, AclPolicyResult, AclScope, AclUser, AclUserChange, AclUserResult } from './types';
import { aclScopeKey } from './types';

export interface AclContext { revision: number; environmentId: string | null }
export type AclAction =
    | { kind: 'user_create' }
    | { kind: 'user_update' | 'user_password' | 'user_delete'; user: AclUser }
    | { kind: 'policy_create'; subject?: string }
    | { kind: 'policy_update'; policy: AclPolicy }
    | { kind: 'policy_delete'; request: AclPolicyDelete };
export type AclWrite =
    | { kind: 'user_create' | 'user_update'; request: AclUserChange }
    | { kind: 'user_delete'; request: { scope: AclScope; username: string } }
    | { kind: 'policy_create' | 'policy_update'; request: AclPolicyChange }
    | { kind: 'policy_delete'; request: AclPolicyDelete };
export interface AclTarget {
    kind: AclWrite['kind']; scope: AclScope; identity: string; policyType?: string; resources?: string[];
    userType?: string; userStatus?: string; entries?: AclPolicyDraftEntry[];
}
export interface AclReceipt {
    target: AclTarget; context: AclContext; finishedAt: number;
    acknowledged: boolean; readBackAvailable: boolean; message: string;
}

export const aclContextMatches = (expected: AclContext, current: AclContext | null) => Boolean(current && expected.revision === current.revision && expected.environmentId === current.environmentId);
export function captureAclAction(action: AclAction, scope: AclScope, expected: AclContext, current: AclContext | null) {
    if (!aclContextMatches(expected, current)) throw new Error('Connection settings changed. Refresh the Broker scope before opening an action.');
    if (!scope.clusterName.trim() || !scope.brokerName.trim() || !scope.brokerAddr.trim()) throw new Error('Choose an explicit master Broker.');
    if ('user' in action && (!action.user.username.trim() || action.user.username !== action.user.username.trim() || /[\u0000-\u001f\u007f-\u009f]/.test(action.user.username))) throw new Error('The selected Broker username is incomplete or malformed. Refresh the directory.');
    if (action.kind === 'policy_delete' && aclScopeKey(action.request.scope) !== aclScopeKey(scope)) throw new Error('The resource belongs to a different Broker scope.');
    return { action: structuredClone(action), scope: { ...scope }, context: { ...expected } };
}

/** Receipts and confirmation metadata deliberately exclude the user password and the full write request. */
export function aclWriteTarget(write: AclWrite): AclTarget {
    const scope = { ...write.request.scope };
    switch (write.kind) {
        case 'user_create': case 'user_update': return { kind: write.kind, scope, identity: write.request.username, userType: write.request.userType, userStatus: write.request.userStatus };
        case 'user_delete': return { kind: write.kind, scope, identity: write.request.username };
        case 'policy_create': case 'policy_update': return { kind: write.kind, scope, identity: write.request.subject, policyType: write.request.policies.map(policy => policy.policyType).join(', '), resources: write.request.policies.flatMap(policy => policy.entries.flatMap(entry => entry.resources)), entries: write.request.policies.flatMap(policy => policy.entries.map(entry => ({ resources: [...entry.resources], actions: [...entry.actions], sourceIps: [...entry.sourceIps], decision: entry.decision }))) };
        case 'policy_delete': return { kind: write.kind, scope, identity: write.request.subject, policyType: write.request.policyType, resources: [write.request.resource] };
    }
}

export function aclWriteReceipt(target: AclTarget, context: AclContext, result: AclUserResult | AclPolicyResult | null, finishedAt = Date.now()): AclReceipt {
    const operation = target.kind.split('_')[1];
    const user = target.kind.startsWith('user_');
    const matches = Boolean(result?.scope && aclScopeKey(result.scope) === aclScopeKey(target.scope) && result.operation === operation &&
        (user ? 'username' in result && result.username === target.identity : 'subject' in result && result.subject === target.identity));
    const acknowledged = matches && result?.success === true;
    const readBackAvailable = Boolean(acknowledged && result && !result.readBackError && (user ? 'users' in result && Array.isArray(result.users) : 'policies' in result && Array.isArray(result.policies)));
    return { target: structuredClone(target), context: { ...context }, finishedAt, acknowledged, readBackAvailable,
        message: acknowledged ? readBackAvailable ? 'The Broker acknowledged this change and returned a refreshed directory.' : 'The Broker acknowledged this change, but its directory could not be read back. Refresh before another operation.' : 'The write outcome is unconfirmed. Inspect the original Broker before retrying; this operation will not submit again.' };
}
