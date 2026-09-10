import { useEffect, useRef, useState } from 'react';
import { AclService } from '../../../services/acl.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import { policyDeleteRequest, policyDraft, policyIdentity } from '../policies';
import type { AclScope, AclPolicy, AclPolicyDraft, AclPolicyDraftEntry, AclPolicyDelete, AclPolicyResult } from '../types';
const field = 'w-full rounded border bg-transparent px-3 py-2 text-sm';
const tokens = (text: string) => text.split(/[,\n]/).map(value => value.trim()).filter(Boolean);
const emptyEntry = (): AclPolicyDraftEntry => ({ resources: [], actions: ['Pub', 'Sub'], sourceIps: [], decision: 'Allow' });

export function AclPolicies({ scope }: { scope: AclScope }) {
    const [policies, setPolicies] = useState<AclPolicy[] | null>(null);
    const [loading, setLoading] = useState(false); const [error, setError] = useState(''); const [query, setQuery] = useState('');
    const [editor, setEditor] = useState<{ subject: string; draft: AclPolicyDraft; update: boolean } | null>(null);
    const [deleting, setDeleting] = useState<AclPolicyDelete | null>(null);
    const [receipt, setReceipt] = useState<AclPolicyResult | null>(null);
    const generation = useRef(0);
    const load = async () => {
        const current = ++generation.current; setLoading(true); setError(''); setPolicies(null);
        try { const policies = await AclService.listPolicies(scope); if (current === generation.current) setPolicies(policies); }
        catch (error) { if (current === generation.current) setError(dashboardErrorMessage(error, 'Unable to read ACL policies.')); }
        finally { if (current === generation.current) setLoading(false); }
    };
    useEffect(() => { void load(); return () => { generation.current++; }; }, []);
    const applied = (result: AclPolicyResult) => { generation.current++; setLoading(false); setReceipt(result); setPolicies(result.policies); setError(result.readBackError ?? ''); setEditor(null); setDeleting(null); };
    const edit = (policy: AclPolicy) => {
        try { setEditor({ subject: policy.subject ?? '', draft: policyDraft(policy), update: true }); setReceipt(null); }
        catch (error) { setError(error instanceof Error ? error.message : 'Policy cannot be edited.'); }
    };
    const filtered = (policies ?? []).filter(policy => [policy.subject, ...policy.entries.map(entry => entry.resource)].some(value => value?.toLowerCase().includes(query.trim().toLowerCase())));
    return <section className="space-y-4"><header className="flex flex-wrap items-center justify-between gap-3"><h2 className="font-semibold">Broker ACL policies</h2>
        <input className={field + ' max-w-xs'} aria-label="Search ACL policies" placeholder="Search subject or resource" value={query} onChange={event => setQuery(event.target.value)} />
        <button disabled={loading} onClick={() => void load()} className="rounded border px-3 py-2 text-sm">Refresh policies</button>
        <button disabled={loading || policies === null} onClick={() => { setReceipt(null); setEditor({ subject: '', draft: { policyType: 'Custom', entries: [emptyEntry()] }, update: false }); }} className="rounded bg-blue-600 px-3 py-2 text-sm text-white">Add policy</button>
    </header>
    {receipt && <p role="status">Broker acknowledged ACL {receipt.operation} for {receipt.subject} at {receipt.scope.brokerAddr}.</p>}
    {error && <p role="alert" className="text-red-600">{error}</p>}{loading && <p>Loading ACL policies…</p>}
    {!loading && policies?.length === 0 && <p>No ACL policies were returned for this Broker.</p>}
    {filtered.map((policy, index) => <article key={JSON.stringify([policy.subject, policy.policyType, index])} className="space-y-3 rounded border p-4">
        <header className="flex justify-between gap-3"><strong>{policy.subject ?? 'Unknown subject'} · {policy.policyType ?? 'Unknown policy type'}</strong><button onClick={() => edit(policy)}>Edit entries</button></header>
        <div className="overflow-auto"><table className="w-full text-left text-sm"><thead><tr><th>Resource</th><th>Actions</th><th>Source IPs</th><th>Decision</th><th>Action</th></tr></thead><tbody>
            {policy.entries.map((entry, index) => <tr key={policyIdentity(policy, entry) + index} className="border-t"><td className="p-2 font-mono">{entry.resource ?? 'Unknown resource'}</td><td>{entry.actions.join(', ')}</td><td>{entry.sourceIps.join(', ') || 'Any source'}</td><td>{entry.decision ?? 'Unknown'}</td>
                <td><button className="text-red-600" onClick={() => { try { setDeleting(policyDeleteRequest(scope, policy, entry)); setReceipt(null); } catch (error) { setError(error instanceof Error ? error.message : 'Policy identity is invalid.'); } }}>Delete resource</button></td></tr>)}
        </tbody></table></div>
    </article>)}
    {editor && <PolicyEditor scope={scope} initial={editor} onClose={() => setEditor(null)} onApplied={applied} />}
    {deleting && <PolicyDeleteDialog request={deleting} onClose={() => setDeleting(null)} onApplied={applied} />}
    </section>;
}
function PolicyEditor({ scope, initial, onClose, onApplied }: { scope: AclScope; initial: { subject: string; draft: AclPolicyDraft; update: boolean }; onClose: () => void; onApplied: (result: AclPolicyResult) => void }) {
    const [subject, setSubject] = useState(initial.subject); const [draft, setDraft] = useState(initial.draft);
    const entryIds = useRef(initial.draft.entries.map((_, index) => index));
    const nextEntryId = useRef(initial.draft.entries.length);
    const [busy, setBusy] = useState(false); const [error, setError] = useState(''); const active = useRef(true);
    useEffect(() => { active.current = true; return () => { active.current = false; }; }, []);
    const changeEntry = (index: number, patch: Partial<AclPolicyDraftEntry>) => setDraft(current => ({ ...current, entries: current.entries.map((entry, position) => position === index ? { ...entry, ...patch } : entry) }));
    const save = async () => {
        if (busy) return;
        if (!subject.trim() || !draft.entries.length || draft.entries.some(entry => !entry.resources.length || !entry.actions.length)) { setError('Subject, resources and actions are required.'); return; }
        setBusy(true); setError('');
        try { const request = { scope, subject, policies: [draft] }; const result = await (initial.update ? AclService.updatePolicy(request) : AclService.createPolicy(request)); if (active.current) onApplied(result); }
        catch (error) { if (active.current) setError(dashboardErrorMessage(error, 'ACL policy write was not confirmed.')); }
        finally { if (active.current) setBusy(false); }
    };
    return <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-5"><section role="dialog" aria-modal="true" aria-label="Edit ACL policy" className="max-h-[90vh] w-full max-w-3xl space-y-4 overflow-auto rounded-xl bg-white p-6 dark:bg-gray-900">
        <h2 className="text-lg font-semibold">{initial.update ? 'Update' : 'Create'} ACL policy</h2><p className="font-mono text-sm">{scope.clusterName} / {scope.brokerName} · {scope.brokerAddr}</p>
        {error && <p role="alert" className="text-red-600">{error}</p>}
        <label className="block text-sm">Subject<input className={field} placeholder="User:username" value={subject} disabled={initial.update || busy} onChange={event => setSubject(event.target.value)} /></label>
        <label className="block text-sm">Policy type<select className={field} disabled={initial.update || busy} value={draft.policyType} onChange={event => setDraft({ ...draft, policyType: event.target.value as 'Custom' | 'Default' })}><option>Custom</option><option>Default</option></select></label>
        <p className="text-sm text-gray-500">Updates modify the listed resources. Use the resource deletion action to remove an existing entry.</p>
        {draft.entries.map((entry, index) => <fieldset key={entryIds.current[index]} disabled={busy} className="space-y-3 rounded border p-4"><legend>Entry {index + 1}</legend>
            <PolicyEntryForm entry={entry} readOnlyResources={initial.update} onChange={patch => changeEntry(index, patch)} />
            {!initial.update && <button type="button" onClick={() => { entryIds.current.splice(index, 1); setDraft({ ...draft, entries: draft.entries.filter((_, position) => position !== index) }); }}>Remove draft entry</button>}
        </fieldset>)}
        {!initial.update && <button disabled={busy} onClick={() => { entryIds.current.push(nextEntryId.current++); setDraft({ ...draft, entries: [...draft.entries, emptyEntry()] }); }}>Add another entry</button>}
        <footer className="flex justify-end gap-3"><button onClick={onClose}>Cancel</button><button disabled={busy} onClick={() => void save()} className="rounded bg-blue-600 px-4 py-2 text-white">{busy ? 'Applying…' : 'Apply policy change'}</button></footer>
    </section></div>;
}
function PolicyEntryForm({ entry, readOnlyResources, onChange }: { entry: AclPolicyDraftEntry; readOnlyResources: boolean; onChange: (patch: Partial<AclPolicyDraftEntry>) => void }) {
    const [resources, setResources] = useState(entry.resources.join('\n'));
    const [actions, setActions] = useState(entry.actions.join(', '));
    const [sourceIps, setSourceIps] = useState(entry.sourceIps.join(', '));
    return <>
        <label className="block text-sm">Resources · one per line<textarea className={field} placeholder="Topic:orders" value={resources} readOnly={readOnlyResources}
            onChange={event => { setResources(event.target.value); onChange({ resources: tokens(event.target.value) }); }} /></label>
        <label className="block text-sm">Actions · comma separated<input className={field} value={actions}
            onChange={event => { setActions(event.target.value); onChange({ actions: tokens(event.target.value) }); }} /></label>
        <label className="block text-sm">Source IPs / CIDRs · blank means any source<input className={field} value={sourceIps}
            onChange={event => { setSourceIps(event.target.value); onChange({ sourceIps: tokens(event.target.value) }); }} /></label>
        <label className="block text-sm">Decision<select className={field} value={entry.decision} onChange={event => onChange({ decision: event.target.value as 'Allow' | 'Deny' })}><option>Allow</option><option>Deny</option></select></label>
    </>;
}
function PolicyDeleteDialog({ request, onClose, onApplied }: { request: AclPolicyDelete; onClose: () => void; onApplied: (result: AclPolicyResult) => void }) {
    const [busy, setBusy] = useState(false); const [error, setError] = useState(''); const active = useRef(true);
    useEffect(() => { active.current = true; return () => { active.current = false; }; }, []);
    const remove = async () => {
        if (busy) return; setBusy(true); setError('');
        try { const result = await AclService.deletePolicy(request); if (active.current) onApplied(result); }
        catch (error) { if (active.current) setError(dashboardErrorMessage(error, 'ACL resource deletion was not confirmed.')); }
        finally { if (active.current) setBusy(false); }
    };
    return <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-5"><section role="dialog" aria-modal="true" aria-label="Delete ACL resource" className="w-full max-w-xl space-y-4 rounded-xl bg-white p-6 dark:bg-gray-900">
        <h2 className="font-semibold">Delete ACL resource</h2><p>{request.subject} / {request.policyType} / <strong>{request.resource}</strong></p><p className="text-sm">Broker: {request.scope.clusterName} / {request.scope.brokerName} · {request.scope.brokerAddr}</p>
        {error && <p role="alert" className="text-red-600">{error}</p>}<footer className="flex justify-end gap-3"><button onClick={onClose}>Cancel</button><button disabled={busy} onClick={() => void remove()} className="rounded bg-red-600 px-4 py-2 text-white">{busy ? 'Deleting…' : 'Confirm resource deletion'}</button></footer>
    </section></div>;
}
