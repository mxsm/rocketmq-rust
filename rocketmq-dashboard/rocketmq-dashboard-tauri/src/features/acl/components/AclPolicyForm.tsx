import { useRef, useState } from 'react';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { PageState } from '../../../components/layout/PageState';
import { policyDraft } from '../policies';
import { buildAclPolicyChange, emptyPolicyEntry, policyTokens } from '../policyDraft';
import type { AclWrite } from '../aclActions';
import type { AclPolicy, AclPolicyDraftEntry, AclScope } from '../types';

export function AclPolicyForm({ scope, policy, initialSubject, disabled, onReview }: {
    scope: AclScope; policy: AclPolicy | null; initialSubject: string; disabled: boolean; onReview: (write: AclWrite) => void;
}) {
    const [subject, setSubject] = useState(policy?.subject ?? initialSubject);
    const [draft, setDraft] = useState(() => policy ? policyDraft(policy) : { policyType: 'Custom' as const, entries: [emptyPolicyEntry()] });
    const [error, setError] = useState('');
    const entryIds = useRef(draft.entries.map((_, index) => index));
    const nextId = useRef(draft.entries.length);
    return <form className="ops-acl-form" onSubmit={event => {
        event.preventDefault();
        if (disabled) return;
        try { onReview({ kind: policy ? 'policy_update' : 'policy_create', request: buildAclPolicyChange(scope, subject, draft, policy) }); }
        catch (error) { setError(error instanceof Error ? error.message : 'Check the policy fields.'); }
    }}>
        {error && <PageState kind="error" title="Check the policy fields" description={error} />}
        <fieldset disabled={disabled} className="ops-acl-fields">
            <div className="ops-acl-form-grid"><Input label="Subject" placeholder="User:username" readOnly={Boolean(policy)} value={subject} onChange={event => setSubject(event.target.value)} />
                <label className="ops-acl-select">Policy type<select disabled={Boolean(policy)} value={draft.policyType} onChange={event => setDraft({ ...draft, policyType: event.target.value as 'Custom' | 'Default' })}><option>Custom</option><option>Default</option></select></label></div>
            <p className="ops-acl-note">Changes apply to the listed resources. Delete an existing resource through its separate deletion action.</p>
            {draft.entries.map((entry, index) => <fieldset key={entryIds.current[index]} className="ops-acl-entry"><legend>Entry {index + 1}</legend>
                <PolicyEntryForm entry={entry} resourcesReadOnly={Boolean(policy)} onChange={patch => setDraft(current => ({ ...current, entries: current.entries.map((value, position) => position === index ? { ...value, ...patch } : value) }))} />
                {!policy && <Button variant="ghost" disabled={draft.entries.length === 1} onClick={() => { entryIds.current.splice(index, 1); setDraft({ ...draft, entries: draft.entries.filter((_, position) => position !== index) }); }}>Remove draft entry</Button>}
            </fieldset>)}
            {!policy && <Button variant="outline" onClick={() => { entryIds.current.push(nextId.current++); setDraft({ ...draft, entries: [...draft.entries, emptyPolicyEntry()] }); }}>Add another entry</Button>}
        </fieldset>
        <footer className="ops-acl-form-actions"><Button type="submit" disabled={disabled}>Review policy change</Button></footer>
    </form>;
}

function PolicyEntryForm({ entry, resourcesReadOnly, onChange }: { entry: AclPolicyDraftEntry; resourcesReadOnly: boolean; onChange: (patch: Partial<AclPolicyDraftEntry>) => void }) {
    const [resources, setResources] = useState(entry.resources.join('\n'));
    const [actions, setActions] = useState(entry.actions.join(', '));
    const [sourceIps, setSourceIps] = useState(entry.sourceIps.join(', '));
    return <div className="ops-acl-fields">
        <label className="ops-acl-select">Resources · one per line<textarea rows={3} readOnly={resourcesReadOnly} value={resources} onChange={event => { setResources(event.target.value); onChange({ resources: policyTokens(event.target.value) }); }} /></label>
        <Input label="Actions · comma separated" value={actions} onChange={event => { setActions(event.target.value); onChange({ actions: policyTokens(event.target.value) }); }} />
        <Input label="Source IPs / CIDRs · blank means any source" value={sourceIps} onChange={event => { setSourceIps(event.target.value); onChange({ sourceIps: policyTokens(event.target.value) }); }} />
        <label className="ops-acl-select">Decision<select value={entry.decision} onChange={event => onChange({ decision: event.target.value as 'Allow' | 'Deny' })}><option>Allow</option><option>Deny</option></select></label>
    </div>;
}
