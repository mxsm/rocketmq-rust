import type { AclPolicy, AclPolicyDraft, AclPolicyDelete, AclScope, AclPolicyEntry, AclPolicyType } from './types';
export const policyType = (value: string | null): AclPolicyType | null => value?.toLowerCase() === 'custom' ? 'Custom' : value?.toLowerCase() === 'default' ? 'Default' : null;
export const policyIdentity = (policy: AclPolicy, entry: AclPolicyEntry) => JSON.stringify([policy.subject, policy.policyType, entry.resource]);
export function policyDeleteRequest(scope: AclScope, policy: AclPolicy, entry: AclPolicyEntry): AclPolicyDelete {
    const kind = policyType(policy.policyType);
    if (!policy.subject?.trim() || !entry.resource?.trim() || !kind) throw new Error('An exact subject, policy type and resource are required.');
    return { scope: { ...scope }, subject: policy.subject, policyType: kind, resource: entry.resource };
}
export function policyDraft(policy: AclPolicy): AclPolicyDraft {
    const kind = policyType(policy.policyType);
    if (!policy.subject?.trim() || !kind || policy.entries.length === 0) throw new Error('Policy identity is incomplete.');
    return { policyType: kind, entries: policy.entries.map(entry => {
        if (!entry.resource || !['allow', 'deny'].includes(entry.decision?.toLowerCase() ?? '')) throw new Error('Policy entry cannot be safely edited.');
        return { resources: [entry.resource], actions: [...entry.actions], sourceIps: [...entry.sourceIps], decision: entry.decision?.toLowerCase() === 'allow' ? 'Allow' : 'Deny' };
    }) };
}
