import type { AclScope, AclUser, AclUserChange } from './types';

export interface AclUserDraft {
    username: string;
    password: string;
    userType: AclUserChange['userType'] | '';
    userStatus: NonNullable<AclUserChange['userStatus']> | '';
}

export function newAclUserDraft(user: AclUser | null): AclUserDraft {
    if (!user) return { username: '', password: '', userType: 'normal', userStatus: 'enable' };
    const type = user.userType?.toLowerCase();
    const status = user.userStatus?.toLowerCase();
    return { username: user.username, password: '', userType: type === 'normal' || type === 'super' ? type : '',
        userStatus: status === 'enable' || status === 'disable' ? status : '' };
}

/** A missing Broker enum is not permission to silently replace it with a default. */
export function buildAclUserChange(scope: AclScope, draft: AclUserDraft, existing: AclUser | null): AclUserChange {
    if (!scope.clusterName.trim() || !scope.brokerName.trim() || !scope.brokerAddr.trim()) throw new Error('Select an explicit master Broker scope.');
    if (!draft.username.trim() || draft.username !== draft.username.trim() || /[\u0000-\u001f\u007f-\u009f]/.test(draft.username)) {
        throw new Error('Enter a username without surrounding whitespace or control characters.');
    }
    if (existing && existing.username !== draft.username) throw new Error('The update must retain the selected username.');
    if (!draft.password.trim()) throw new Error('Enter a new password. The Broker API requires it for this change.');
    if (!draft.userType || !['normal', 'super'].includes(draft.userType)) throw new Error('Choose an explicit user type; the current type was not recognized.');
    if (!draft.userStatus || !['enable', 'disable'].includes(draft.userStatus)) throw new Error('Choose an explicit user status; the current status was not recognized.');
    if (!existing && draft.userStatus !== 'enable') throw new Error('New ACL users are enabled. Change status after creation.');
    return { scope: { ...scope }, username: draft.username, password: draft.password, userType: draft.userType, userStatus: draft.userStatus };
}
