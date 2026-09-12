import { PageState } from '../../../components/layout/PageState';
import { StatusBadge } from '../../../components/layout/StatusBadge';
import type { AclReceipt, AclTarget } from '../aclActions';

export function AclTargetDetails({ target }: { target: AclTarget }) {
    return <><dl className="ops-acl-properties">{[
        ['Broker', `${target.scope.clusterName} / ${target.scope.brokerName} · ${target.scope.brokerAddr}`],
        [target.kind.startsWith('user_') ? 'Username' : 'Subject', target.identity],
        ...(target.userType ? [['User type', target.userType]] : []), ...(target.userStatus ? [['Status', target.userStatus]] : []),
        ...(target.policyType ? [['Policy type', target.policyType]] : []), ...(target.resources ? [['Resources', target.resources.join('\n')]] : []),
    ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd><AclValue value={value} /></dd></div>)}</dl>
        {target.entries?.map((entry, index) => <section className="ops-acl-entry" key={index} aria-label={`Policy change entry ${index + 1}`}><dl className="ops-acl-properties">{[
            ['Resources', entry.resources.join('\n')], ['Actions', entry.actions.join(', ')], ['Source IPs', entry.sourceIps.join(', ') || 'Any source'], ['Decision', entry.decision],
        ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd><AclValue value={value} /></dd></div>)}</dl></section>)}
    </>;
}

export function AclValue({ value }: { value: string }) {
    return <span className="ops-acl-value" tabIndex={value.length > 160 ? 0 : undefined}>{value}</span>;
}

export function AclReceiptPanel({ receipt }: { receipt: AclReceipt }) {
    return <section className="ops-acl-receipt" aria-label="Latest ACL operation">
        {receipt.acknowledged && receipt.readBackAvailable ? <div role="status"><StatusBadge tone="success">ACL change acknowledged</StatusBadge><p className="ops-acl-note">{receipt.message}</p></div>
            : <PageState kind="partial" title={receipt.acknowledged ? 'ACL change acknowledged · read-back unavailable' : 'ACL outcome unconfirmed'} description={receipt.message} />}
        <p className="ops-acl-note">{receipt.target.kind.replace('_', ' ')} · Environment {receipt.context.environmentId ?? 'Not configured'} · Revision {receipt.context.revision} · {new Date(receipt.finishedAt).toLocaleString()}</p>
        <AclTargetDetails target={receipt.target} />
    </section>;
}
