import { Pencil, Trash2 } from 'lucide-react';
import { Button } from '../../../components/ui/LegacyButton';
import { PageState } from '../../../components/layout/PageState';
import { StatusBadge } from '../../../components/layout/StatusBadge';
import { useAclActions } from '../aclActionContext';
import type { AclContext } from '../aclActions';
import type { AclScope, AclUser } from '../types';
import { AclValue } from './AclReceiptPanel';

export function AclUserStatus({ status }: { status: string | null }) {
    const value = status?.toLowerCase();
    return <StatusBadge tone={value === 'enable' ? 'success' : value === 'disable' ? 'warning' : 'neutral'}>{value === 'enable' ? 'Enabled' : value === 'disable' ? 'Disabled' : status || 'Unknown'}</StatusBadge>;
}

export function AclUsers({ scope, context, users, selected, onSelect, disabled, showEmpty }: {
    scope: AclScope; context: AclContext; users: AclUser[]; selected: AclUser | null; onSelect: (user: AclUser) => void; disabled: boolean; showEmpty: boolean;
}) {
    const { open } = useAclActions();
    return <div className="ops-acl-split">
        <div className="ops-acl-directory"><div className="ops-acl-scroll" role="region" aria-label="Broker ACL users" tabIndex={0}>
            <table><thead><tr><th scope="col">User</th><th scope="col">Type</th><th scope="col">Status</th><th scope="col">Actions</th></tr></thead>
                <tbody>{users.map(user => <tr key={user.username} data-selected={user.username === selected?.username}>
                    <th scope="row"><button className="ops-acl-choice" aria-pressed={user.username === selected?.username} onClick={() => onSelect(user)}><span>{user.username}</span></button></th>
                    <td><AclValue value={user.userType || 'Unknown'} /></td><td><AclUserStatus status={user.userStatus} /></td>
                    <td><Button variant="ghost" className="ops-button-icon-only" icon={Trash2} aria-label={`Delete ACL user ${user.username}`} disabled={disabled} onClick={() => open({ kind: 'user_delete', user }, scope, context)} /></td>
                </tr>)}</tbody>
            </table>
            {showEmpty && !users.length && <PageState kind="empty" title="No matching users" description="Refresh the directory or change the username filter." />}
        </div></div>
        <section className="ops-acl-detail" aria-label="User details"><header className="ops-section-header"><h2>User details</h2>
            {selected && <Button icon={Pencil} disabled={disabled} onClick={() => open({ kind: 'user_update', user: selected }, scope, context)}>Edit</Button>}
        </header>
            {selected ? <><dl className="ops-acl-properties">
                <div><dt>User</dt><dd><AclValue value={selected.username} /></dd></div><div><dt>Type</dt><dd><AclValue value={selected.userType || 'Unknown'} /></dd></div>
                <div><dt>Status</dt><dd><AclUserStatus status={selected.userStatus} /></dd></div>
            </dl><div className="ops-acl-password"><div><strong>Password</strong><p className="ops-acl-note">Saved passwords are never returned.</p></div>
                <Button variant="secondary" disabled={disabled} onClick={() => open({ kind: 'user_password', user: selected }, scope, context)}>Change password</Button>
            </div><p className="ops-acl-note">Broker ACL identity · {scope.brokerName}</p></> : <PageState kind="empty" title="Select a user" description="Inspect its type, status and related resource policies." />}
        </section>
    </div>;
}
