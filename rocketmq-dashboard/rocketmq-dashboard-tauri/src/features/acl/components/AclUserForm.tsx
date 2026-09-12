import { useState } from 'react';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { PageState } from '../../../components/layout/PageState';
import { buildAclUserChange, newAclUserDraft, type AclUserDraft } from '../userDraft';
import type { AclWrite } from '../aclActions';
import type { AclScope, AclUser } from '../types';

export function AclUserForm({ scope, user, passwordOnly, disabled, onReview }: {
    scope: AclScope; user: AclUser | null; passwordOnly: boolean; disabled: boolean; onReview: (write: AclWrite) => void;
}) {
    const [draft, setDraft] = useState(() => newAclUserDraft(user));
    const [error, setError] = useState('');
    return <form className="ops-acl-form" onSubmit={event => {
        event.preventDefault();
        if (disabled) return;
        try { onReview({ kind: user ? 'user_update' : 'user_create', request: buildAclUserChange(scope, draft, user) }); }
        catch (error) { setError(error instanceof Error ? error.message : 'Check the user fields.'); }
    }}>
        {error && <PageState kind="error" title="Check the user fields" description={error} />}
        <fieldset disabled={disabled} className="ops-acl-fields">
            <Input label="Username" autoComplete="off" readOnly={Boolean(user)} value={draft.username} onChange={event => setDraft({ ...draft, username: event.target.value })} />
            <Input label="New password" type="password" autoComplete="new-password" value={draft.password} onChange={event => setDraft({ ...draft, password: event.target.value })} />
            <p className="ops-acl-note">Saved passwords are never loaded. {user ? 'This Broker API requires a new password with every user update.' : 'New Broker ACL users are enabled.'}</p>
            <div className="ops-acl-form-grid">
                <label className="ops-acl-select">User type<select disabled={passwordOnly} value={draft.userType} onChange={event => setDraft({ ...draft, userType: event.target.value as AclUserDraft['userType'] })}>
                    <option value="" disabled>Choose a user type</option><option value="normal">Normal</option><option value="super">Super</option>
                </select></label>
                {user && <label className="ops-acl-select">Status<select disabled={passwordOnly} value={draft.userStatus} onChange={event => setDraft({ ...draft, userStatus: event.target.value as AclUserDraft['userStatus'] })}>
                    <option value="" disabled>Choose a user status</option><option value="enable">Enabled</option><option value="disable">Disabled</option>
                </select></label>}
            </div>
            {passwordOnly && <p className="ops-acl-note">The selected user type and status are preserved. If either value is unknown, refresh the user or use Edit to choose explicit values.</p>}
        </fieldset>
        <footer className="ops-acl-form-actions"><Button type="submit" disabled={disabled}>Review user change</Button></footer>
    </form>;
}
