import { policyDraft } from './policies';
import type { AclPolicy, AclPolicyChange, AclPolicyDraft, AclPolicyDraftEntry, AclScope } from './types';

export const policyTokens = (text: string) => text.split(/[,\n]/).map(value => value.trim()).filter(Boolean);
export const emptyPolicyEntry = (): AclPolicyDraftEntry => ({ resources: [], actions: ['Pub', 'Sub'], sourceIps: [], decision: 'Allow' });

/** Updates retain every resource identity; deletion is a separate, explicitly targeted operation. */
export function buildAclPolicyChange(scope: AclScope, subject: string, draft: AclPolicyDraft, existing: AclPolicy | null): AclPolicyChange {
    if (!scope.clusterName.trim() || !scope.brokerName.trim() || !scope.brokerAddr.trim()) throw new Error('Select an explicit master Broker scope.');
    if (!subject.trim() || subject !== subject.trim() || /[\u0000-\u001f\u007f-\u009f]/.test(subject)) throw new Error('Enter an exact subject without surrounding whitespace or control characters.');
    if (!['Custom', 'Default'].includes(draft.policyType) || !draft.entries.length) throw new Error('Choose a policy type and provide at least one entry.');
    const resources = new Set<string>();
    for (const entry of draft.entries) {
        if (!entry.resources.length || !entry.actions.length || [...entry.resources, ...entry.actions, ...entry.sourceIps].some(value => !value.trim() || value !== value.trim() || /[\u0000-\u001f\u007f-\u009f]/.test(value))) {
            throw new Error('Every entry requires explicit resources and actions. Remove empty or malformed values.');
        }
        if (!['Allow', 'Deny'].includes(entry.decision)) throw new Error('Choose Allow or Deny for every entry.');
        for (const resource of entry.resources) {
            if (resources.has(resource)) throw new Error('A resource can appear only once within a policy type.');
            resources.add(resource);
        }
    }
    if (existing) {
        const original = policyDraft(existing);
        const before = original.entries.flatMap(entry => entry.resources).sort();
        if (subject !== existing.subject || draft.policyType !== original.policyType || JSON.stringify(before) !== JSON.stringify([...resources].sort())) {
            throw new Error('Retain the selected subject, policy type and resources. Use resource deletion to remove an entry.');
        }
    }
    return { scope: { ...scope }, subject, policies: [structuredClone(draft)] };
}
