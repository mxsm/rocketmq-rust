import { expect, it } from 'vitest';
import { buildAclPolicyChange, policyTokens } from './policyDraft';
import { policyDraft } from './policies';
import type { AclPolicy } from './types';

const scope = { clusterName: 'cluster', brokerName: 'broker-a', brokerAddr: '127.0.0.1:10911' };
const policy: AclPolicy = { subject: 'User:reader', policyType: 'Custom', entries: [
    { resource: 'Topic:orders', actions: ['Sub'], sourceIps: ['10.1.0.0/16'], decision: 'Allow' },
    { resource: 'Topic:payments', actions: ['Pub'], sourceIps: [], decision: 'Deny' },
] };

it('preserves multi-resource updates and copies the reviewed payload', () => {
    const draft = policyDraft(policy);
    draft.entries[0].decision = 'Deny';
    const request = buildAclPolicyChange(scope, policy.subject!, draft, policy);
    draft.entries[0].sourceIps.push('127.0.0.1');
    expect(request.policies[0].entries[0]).toEqual({ resources: ['Topic:orders'], actions: ['Sub'], sourceIps: ['10.1.0.0/16'], decision: 'Deny' });
    expect(request.policies[0].entries[1].resources).toEqual(['Topic:payments']);
});

it('rejects renaming, dropping or adding update resources, subject and type changes', () => {
    const draft = policyDraft(policy);
    expect(() => buildAclPolicyChange(scope, 'User:other', draft, policy)).toThrow('Retain');
    expect(() => buildAclPolicyChange(scope, policy.subject!, { ...draft, policyType: 'Default' }, policy)).toThrow('Retain');
    expect(() => buildAclPolicyChange(scope, policy.subject!, { ...draft, entries: draft.entries.slice(1) }, policy)).toThrow('Retain');
    draft.entries[0].resources = ['Topic:replacement'];
    expect(() => buildAclPolicyChange(scope, policy.subject!, draft, policy)).toThrow('Retain');
});

it('rejects duplicate resources, empty actions and malformed subjects before dispatch', () => {
    const draft = policyDraft(policy);
    expect(() => buildAclPolicyChange(scope, 'User:reader\n', draft, null)).toThrow('exact subject');
    draft.entries[1].resources = ['Topic:orders'];
    expect(() => buildAclPolicyChange(scope, policy.subject!, draft, null)).toThrow('only once');
    draft.entries.pop(); draft.entries[0].actions = [];
    expect(() => buildAclPolicyChange(scope, policy.subject!, draft, null)).toThrow('explicit resources and actions');
});

it('parses editing separators without changing resource and network identifiers', () => {
    expect(policyTokens(' Topic:orders,\nTopic:payments\n ')).toEqual(['Topic:orders', 'Topic:payments']);
    expect(policyTokens('10.1.0.0/16, ::1')).toEqual(['10.1.0.0/16', '::1']);
});
