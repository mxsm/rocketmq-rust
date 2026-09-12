import { expect, it } from 'vitest';
import { buildAclUserChange, newAclUserDraft } from './userDraft';

const scope = { clusterName: 'cluster', brokerName: 'broker', brokerAddr: '127.0.0.1:10911' };
const existing = { username: 'reader', userType: 'Normal', userStatus: 'Disable' };

it('starts an existing-user edit without a password and preserves recognized Broker state', () => {
    expect(newAclUserDraft(existing)).toEqual({ username: 'reader', password: '', userType: 'normal', userStatus: 'disable' });
    expect(() => buildAclUserChange(scope, newAclUserDraft(existing), existing)).toThrow('new password');
});

it('does not convert unknown type or status into broader defaults', () => {
    const user = { ...existing, userType: 'FutureType', userStatus: null };
    const draft = { ...newAclUserDraft(user), password: 'test-only-value' };
    expect(draft.userType).toBe(''); expect(draft.userStatus).toBe('');
    expect(() => buildAclUserChange(scope, draft, user)).toThrow('explicit user type');
    expect(() => buildAclUserChange(scope, { ...draft, userType: 'normal' }, user)).toThrow('explicit user status');
});

it('preserves the new password exactly, without trimming or storing a copy in the user model', () => {
    const draft = { ...newAclUserDraft(existing), password: ' value with spaces ' };
    expect(buildAclUserChange(scope, draft, existing).password).toBe(' value with spaces ');
    expect(existing).not.toHaveProperty('password');
});

it('rejects username changes, control characters and disabled creation', () => {
    const draft = { ...newAclUserDraft(existing), password: 'test-only-value' };
    expect(() => buildAclUserChange(scope, { ...draft, username: 'other' }, existing)).toThrow('selected username');
    expect(() => buildAclUserChange(scope, { ...draft, username: ' reader' }, null)).toThrow('whitespace');
    expect(() => buildAclUserChange(scope, { ...draft, username: 'read\ner' }, null)).toThrow('control');
    expect(() => buildAclUserChange(scope, draft, null)).toThrow('enabled');
});
