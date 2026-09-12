import { useState } from 'react';
import { Pencil, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '../../../components/ui/LegacyButton';
import { PageState } from '../../../components/layout/PageState';
import { StatusBadge } from '../../../components/layout/StatusBadge';
import { useAclActions } from '../aclActionContext';
import { policyDeleteRequest, policyIdentity } from '../policies';
import type { AclContext } from '../aclActions';
import type { AclPolicy, AclScope } from '../types';
import { AclValue } from './AclReceiptPanel';

const identity = (policy: AclPolicy) => JSON.stringify([policy.subject, policy.policyType]);

export function AclPolicies({ scope, context, policies, disabled, showEmpty }: { scope: AclScope; context: AclContext; policies: AclPolicy[]; disabled: boolean; showEmpty: boolean }) {
    const [selectedKey, setSelectedKey] = useState('');
    const selected = policies.find(policy => identity(policy) === selectedKey) ?? null;
    const { open } = useAclActions();
    return <div className="ops-acl-split">
        <div className="ops-acl-directory"><div className="ops-acl-scroll" role="region" aria-label="Broker ACL policies" tabIndex={0}>
            <table><thead><tr><th scope="col">Subject</th><th scope="col">Policy type</th><th scope="col">Resources</th></tr></thead><tbody>
                {policies.map((policy, index) => <tr key={`${identity(policy)}:${index}`} data-selected={policy === selected}><th scope="row"><button className="ops-acl-choice" aria-pressed={policy === selected} onClick={() => setSelectedKey(identity(policy))}><span>{policy.subject || 'Unknown subject'}</span></button></th>
                    <td>{policy.policyType || 'Unknown'}</td><td>{policy.entries.length}</td></tr>)}
            </tbody></table>
            {showEmpty && !policies.length && <PageState kind="empty" title="No matching policies" description="Change the filter or create a resource policy." />}
        </div></div>
        <section className="ops-acl-detail" aria-label="Policy details"><header className="ops-section-header"><h2>Policy details</h2>
            {selected && <Button icon={Pencil} disabled={disabled} onClick={() => open({ kind: 'policy_update', policy: selected }, scope, context)}>Edit entries</Button>}
        </header>{selected ? <><dl className="ops-acl-properties"><div><dt>Subject</dt><dd><AclValue value={selected.subject || 'Unknown'} /></dd></div><div><dt>Policy type</dt><dd>{selected.policyType || 'Unknown'}</dd></div></dl>
            <AclPolicyResources scope={scope} context={context} policies={[selected]} disabled={disabled} />
        </> : <PageState kind="empty" title="Select a policy" description="Inspect resource permissions and source IP restrictions." />}</section>
    </div>;
}

export function AclPolicyResources({ scope, context, policies, disabled }: { scope: AclScope; context: AclContext; policies: AclPolicy[]; disabled: boolean }) {
    const { open } = useAclActions();
    return <div className="ops-acl-scroll ops-acl-resource-table" role="region" aria-label="Resource policy entries" tabIndex={0}>
        <table><thead><tr>{['Subject / type', 'Resource', 'Actions', 'Source IPs', 'Decision', 'Actions'].map((label, index) => <th key={index} scope="col">{label}</th>)}</tr></thead>
            <tbody>{policies.flatMap((policy, policyIndex) => policy.entries.map((entry, index) => <tr key={`${policyIdentity(policy, entry)}:${policyIndex}:${index}`}>
                <td><AclValue value={policy.subject || 'Unknown subject'} /><span className="ops-acl-note">{policy.policyType || 'Unknown type'}</span></td>
                <td><AclValue value={entry.resource || 'Unknown resource'} /></td><td><AclValue value={entry.actions.join(', ') || 'Not reported'} /></td><td><AclValue value={entry.sourceIps.join(', ') || 'Any source'} /></td>
                <td><StatusBadge tone={entry.decision?.toLowerCase() === 'allow' ? 'success' : entry.decision?.toLowerCase() === 'deny' ? 'danger' : 'neutral'}>{entry.decision || 'Unknown'}</StatusBadge></td>
                <td><div className="ops-acl-row-actions"><Button variant="ghost" icon={Pencil} className="ops-button-icon-only" aria-label={`Edit policy ${policy.subject ?? ''} ${policy.policyType ?? ''}`} disabled={disabled} onClick={() => open({ kind: 'policy_update', policy }, scope, context)} />
                    <Button variant="ghost" icon={Trash2} className="ops-button-icon-only" aria-label={`Delete resource ${entry.resource ?? ''} from ${policy.subject ?? ''} ${policy.policyType ?? ''}`} disabled={disabled} onClick={() => {
                        try { open({ kind: 'policy_delete', request: policyDeleteRequest(scope, policy, entry) }, scope, context); }
                        catch (error) { toast.error(error instanceof Error ? error.message : 'The resource identity is incomplete.'); }
                    }} /></div></td>
            </tr>))}</tbody>
        </table>
        {!policies.some(policy => policy.entries.length) && <PageState kind="empty" title="No resource entries returned" />}
    </div>;
}
