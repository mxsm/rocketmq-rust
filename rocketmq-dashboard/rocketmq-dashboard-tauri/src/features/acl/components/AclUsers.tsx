import { useEffect, useRef, useState } from 'react';
import { AclService } from '../../../services/acl.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import type { AclScope, AclUser, AclUserChange, AclUserResult } from '../types';

const field = 'w-full rounded border bg-transparent px-3 py-2 text-sm';
export function AclUsers({ scope }: { scope: AclScope }) {
    const [users, setUsers] = useState<AclUser[] | null>(null);
    const [error, setError] = useState('');
    const [loading, setLoading] = useState(false);
    const [query, setQuery] = useState('');
    const [editor, setEditor] = useState<{ user: AclUser | null } | null>(null);
    const [deleting, setDeleting] = useState<AclUser | null>(null);
    const [receipt, setReceipt] = useState<AclUserResult | null>(null);
    const generation = useRef(0);
    const load = async () => {
        const current = ++generation.current;
        setLoading(true); setError(''); setUsers(null);
        try { const users = await AclService.listUsers(scope); if (current === generation.current) setUsers(users); }
        catch (error) { if (current === generation.current) setError(dashboardErrorMessage(error, 'Unable to read ACL users.')); }
        finally { if (current === generation.current) setLoading(false); }
    };
    useEffect(() => { void load(); return () => { generation.current++; }; }, []);
    const applied = (result: AclUserResult) => {
        generation.current++;
        setReceipt(result); setUsers(result.users); setError(result.readBackError ?? '');
        setLoading(false); setEditor(null); setDeleting(null);
    };
    const filtered = (users ?? []).filter(user => user.username.toLowerCase().includes(query.trim().toLowerCase()));
    return <section className="space-y-4">
        <header className="flex flex-wrap items-center justify-between gap-3"><h2 className="font-semibold">Broker ACL users</h2>
            <input className={field + ' max-w-xs'} aria-label="Search ACL users" placeholder="Search username" value={query} onChange={event => setQuery(event.target.value)} />
            <button disabled={loading} onClick={() => void load()} className="rounded border px-3 py-2 text-sm">Refresh users</button>
            <button disabled={loading || users === null} onClick={() => { setReceipt(null); setEditor({ user: null }); }} className="rounded bg-blue-600 px-3 py-2 text-sm text-white">Add user</button>
        </header>
        {receipt && <p role="status">Broker acknowledged ACL {receipt.operation} for {receipt.username} at {receipt.scope.brokerAddr}.</p>}
        {error && <p role="alert" className="text-red-600 dark:text-red-400">{error}</p>}
        {loading && <p>Loading ACL users…</p>}
        {!loading && users?.length === 0 && <p>No ACL users were returned for this Broker.</p>}
        <div className="overflow-auto"><table className="w-full text-left text-sm"><thead><tr><th>Username</th><th>Type</th><th>Status</th><th>Actions</th></tr></thead>
            <tbody>{filtered.map(user => <tr key={user.username} className="border-t"><td className="p-3 font-mono">{user.username}</td><td>{user.userType ?? 'Unknown'}</td><td>{user.userStatus ?? 'Unknown'}</td>
                <td className="space-x-3"><button onClick={() => { setReceipt(null); setEditor({ user }); }}>Edit</button><button className="text-red-600" onClick={() => { setReceipt(null); setDeleting(user); }}>Delete</button></td></tr>)}</tbody>
        </table></div>
        {editor && <AclUserEditor scope={scope} user={editor.user} onClose={() => setEditor(null)} onApplied={applied} />}
        {deleting && <AclUserDeleteDialog scope={scope} user={deleting} onClose={() => setDeleting(null)} onApplied={applied} />}
    </section>;
}
function AclUserEditor({ scope, user, onClose, onApplied }: { scope: AclScope; user: AclUser | null; onClose: () => void; onApplied: (result: AclUserResult) => void }) {
    const [username, setUsername] = useState(user?.username ?? '');
    const [password, setPassword] = useState('');
    const [userType, setUserType] = useState<'normal' | 'super'>(user?.userType?.toLowerCase() === 'super' ? 'super' : 'normal');
    const [userStatus, setUserStatus] = useState<'enable' | 'disable'>(user?.userStatus?.toLowerCase() === 'disable' ? 'disable' : 'enable');
    const [error, setError] = useState('');
    const [busy, setBusy] = useState(false);
    const active = useRef(true);
    useEffect(() => { active.current = true; return () => { active.current = false; }; }, []);
    const save = async () => {
        if (busy) return;
        if (!username.trim() || username !== username.trim() || !password.trim()) { setError('Username and password are required.'); return; }
        setBusy(true); setError('');
        const request: AclUserChange = { scope, username, password, userType, userStatus: user ? userStatus : 'enable' };
        try { const result = await (user ? AclService.updateUser(request) : AclService.createUser(request)); if (active.current) { setPassword(''); onApplied(result); } }
        catch (error) { if (active.current) setError(dashboardErrorMessage(error, 'ACL user write was not confirmed.')); }
        finally { if (active.current) setBusy(false); }
    };
    return <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-5"><section role="dialog" aria-modal="true" aria-label={user ? 'Edit ACL user' : 'Create ACL user'} className="w-full max-w-xl space-y-4 rounded-xl bg-white p-6 shadow-xl dark:bg-gray-900">
        <h2 className="text-lg font-semibold">{user ? 'Edit' : 'Create'} Broker ACL user</h2><p className="break-all font-mono text-sm">{scope.clusterName} / {scope.brokerName} · {scope.brokerAddr}</p>
        {error && <p role="alert" className="text-red-600">{error}</p>}
        <label className="block text-sm">Username<input className={field} autoComplete="off" disabled={Boolean(user) || busy} value={username} onChange={event => setUsername(event.target.value)} /></label>
        <label className="block text-sm">Password<input className={field} type="password" autoComplete="new-password" disabled={busy} value={password} onChange={event => setPassword(event.target.value)} /></label>
        <p className="text-sm text-gray-500">{user ? 'Enter the password to apply with this update. The current API requires a value; leaving it blank is not supported.' : 'New ACL users are enabled. Their status can be changed after creation.'} Saved passwords are never loaded into this form.</p>
        <label className="block text-sm">User type<select className={field} disabled={busy} value={userType} onChange={event => setUserType(event.target.value as 'normal' | 'super')}><option value="normal">Normal</option><option value="super">Super</option></select></label>
        {user && <label className="block text-sm">Status<select className={field} disabled={busy} value={userStatus} onChange={event => setUserStatus(event.target.value as 'enable' | 'disable')}><option value="enable">Enable</option><option value="disable">Disable</option></select></label>}
        <footer className="flex justify-end gap-3"><button onClick={onClose}>Cancel</button><button disabled={busy} onClick={() => void save()} className="rounded bg-blue-600 px-4 py-2 text-white">{busy ? 'Applying…' : 'Apply user change'}</button></footer>
    </section></div>;
}
function AclUserDeleteDialog({ scope, user, onClose, onApplied }: { scope: AclScope; user: AclUser; onClose: () => void; onApplied: (result: AclUserResult) => void }) {
    const [busy, setBusy] = useState(false); const [error, setError] = useState('');
    const active = useRef(true);
    useEffect(() => { active.current = true; return () => { active.current = false; }; }, []);
    const remove = async () => {
        if (busy) return; setBusy(true); setError('');
        try { const result = await AclService.deleteUser(scope, user.username); if (active.current) onApplied(result); }
        catch (error) { if (active.current) setError(dashboardErrorMessage(error, 'ACL user deletion was not confirmed.')); }
        finally { if (active.current) setBusy(false); }
    };
    return <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-5"><section role="dialog" aria-modal="true" aria-label="Delete ACL user" className="w-full max-w-lg space-y-4 rounded-xl bg-white p-6 dark:bg-gray-900">
        <h2 className="font-semibold">Delete Broker ACL user</h2><p>Delete <strong>{user.username}</strong> from {scope.clusterName} / {scope.brokerName} at {scope.brokerAddr}?</p>
        {error && <p role="alert" className="text-red-600">{error}</p>}
        <footer className="flex justify-end gap-3"><button onClick={onClose}>Cancel</button><button disabled={busy} onClick={() => void remove()} className="rounded bg-red-600 px-4 py-2 text-white">{busy ? 'Deleting…' : 'Confirm deletion'}</button></footer>
    </section></div>;
}
