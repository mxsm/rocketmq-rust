import { describe, expect, it } from 'vitest';
import { policyDeleteRequest, policyDraft, policyIdentity } from './policies';
import type { AclPolicy } from './types';
const scope = { clusterName: 'cluster', brokerName: 'broker-a', brokerAddr: 'localhost:10911' };
const policy: AclPolicy = { subject: 'User:alice', policyType: 'Default', entries: [
    { resource: 'Topic:a', actions: ['Pub'], sourceIps: ['127.0.0.1'], decision: 'Allow' },
    { resource: 'Topic:b', actions: ['Sub'], sourceIps: [], decision: 'Deny' },
] };
describe('ACL policy identities', () => {
    it('retains all resources, actions, and source filters during editing', () => {
        const draft = policyDraft(policy);
        expect(draft.policyType).toBe('Default');
        expect(draft.entries.map(entry => entry.resources)).toEqual([['Topic:a'], ['Topic:b']]);
        expect(draft.entries[0].sourceIps).toEqual(['127.0.0.1']);
        expect(draft.entries[1].decision).toBe('Deny');
    });
    it('deletes by original subject, policy type, resource and scope', () => {
        expect(policyDeleteRequest(scope, policy, policy.entries[1])).toEqual({ scope, subject: 'User:alice', policyType: 'Default', resource: 'Topic:b' });
        expect(policyIdentity(policy, policy.entries[0])).not.toBe(policyIdentity({ ...policy, policyType: 'Custom' }, policy.entries[0]));
        expect(() => policyDeleteRequest(scope, policy, { ...policy.entries[0], resource: '' })).toThrow();
        expect(() => policyDraft({ ...policy, policyType: null })).toThrow();
    });
});
