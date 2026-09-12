import { expect, it } from 'vitest';
import { aclWriteReceipt, aclWriteTarget, captureAclAction, type AclWrite } from './aclActions';
import type { AclUserResult } from './types';

const scope = { clusterName: 'cluster', brokerName: 'broker-a', brokerAddr: '127.0.0.1:10911' };
const context = { environmentId: 'local', revision: 12 };
const user = { username: 'reader', userType: 'Normal', userStatus: 'Enable' };
const write: AclWrite = { kind: 'user_update', request: { scope, username: 'reader', password: 'fixture-private-value', userType: 'normal', userStatus: 'disable' } };
const result: AclUserResult = { scope, username: 'reader', operation: 'update', success: true, users: [user], readBackError: null };

it('refuses stale actions and freezes the selected Broker identity', () => {
    expect(() => captureAclAction({ kind: 'user_delete', user }, scope, context, { ...context, revision: 13 })).toThrow('Connection settings');
    expect(() => captureAclAction({ kind: 'user_delete', user }, scope, context, { ...context, environmentId: 'other' })).toThrow('Connection settings');
    const input = { ...user }, broker = { ...scope };
    const captured = captureAclAction({ kind: 'user_delete', user: input }, broker, context, context);
    input.username = 'other'; broker.brokerAddr = 'other:10911';
    expect(captured.action).toEqual({ kind: 'user_delete', user });
    expect(captured.scope).toEqual(scope);
});

it('does not transplant a resource deletion to another Broker', () => {
    expect(() => captureAclAction({ kind: 'policy_delete', request: { scope: { ...scope, brokerName: 'broker-b' }, subject: 'User:reader', policyType: 'Default', resource: 'Topic:orders' } }, scope, context, context)).toThrow('different Broker');
});

it('stores only explicit confirmation metadata, never the password or a raw response', () => {
    const target = aclWriteTarget(write);
    const receipt = aclWriteReceipt(target, context, { ...result, password: 'unexpected-secret' } as AclUserResult, 500);
    expect(receipt).toMatchObject({ acknowledged: true, readBackAvailable: true, finishedAt: 500, context });
    expect(JSON.stringify({ target, receipt })).not.toMatch(/fixture-private-value|unexpected-secret|password/i);
});

it('retains acknowledged writes when the follow-up read fails', () => {
    const receipt = aclWriteReceipt(aclWriteTarget(write), context, { ...result, users: null, readBackError: 'private diagnostic' });
    expect(receipt.acknowledged).toBe(true);
    expect(receipt.readBackAvailable).toBe(false);
    expect(receipt.message).toContain('could not be read back');
    expect(JSON.stringify(receipt)).not.toContain('private diagnostic');
});

it('does not turn a transport failure, failed acknowledgement or mismatched identity into success', () => {
    const target = aclWriteTarget(write);
    for (const response of [null, { ...result, success: false }, { ...result, username: 'another-user' }, { ...result, operation: 'delete' as const }, { ...result, scope: { ...scope, brokerAddr: 'other:10911' } }]) {
        expect(aclWriteReceipt(target, context, response)).toMatchObject({ acknowledged: false, readBackAvailable: false });
    }
    expect(aclWriteReceipt(target, context, {} as AclUserResult).acknowledged).toBe(false);
    expect(aclWriteReceipt(target, context, { ...result, users: undefined }).readBackAvailable).toBe(false);
});

it('preserves the policy type, decisions and source restrictions in confirmation metadata', () => {
    const target = aclWriteTarget({ kind: 'policy_update', request: { scope, subject: 'User:reader', policies: [{ policyType: 'Default', entries: [{ resources: ['Topic:a', 'Topic:b'], actions: ['Sub'], sourceIps: ['10.0.0.0/8'], decision: 'Deny' }] }] } });
    expect(target).toMatchObject({ policyType: 'Default', resources: ['Topic:a', 'Topic:b'], entries: [{ resources: ['Topic:a', 'Topic:b'], actions: ['Sub'], sourceIps: ['10.0.0.0/8'], decision: 'Deny' }] });
    expect(aclWriteReceipt(target, context, { scope, subject: 'User:reader', operation: 'update', success: true, policies: [], readBackError: null }).acknowledged).toBe(true);
});
