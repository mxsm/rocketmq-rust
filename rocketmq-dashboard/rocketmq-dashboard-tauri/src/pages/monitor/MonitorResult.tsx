import { StatusBadge } from '../../components/layout/StatusBadge';
import { monitorContextMatches, type MonitorContext, type MonitorReceipt, type MonitorTarget } from './monitorModel';

export function MonitorTargetDetails({ target }: { target: MonitorTarget }) {
    return <dl className="ops-monitor-target">
        <div><dt>Environment</dt><dd tabIndex={0}>{target.context.environmentId}</dd></div>
        <div><dt>Consumer group</dt><dd tabIndex={0}>{target.request.consumerGroup}</dd></div>
        <div><dt>Expected rule revision</dt><dd>{target.request.expectedRevision}</dd></div>
        <div><dt>Connection revision</dt><dd>{target.context.revision}</dd></div>
        {target.kind === 'save' && <><div><dt>Minimum online clients</dt><dd>{target.request.minCount}</dd></div><div><dt>Maximum lag</dt><dd>{target.request.maxDiffTotal}</dd></div></>}
    </dl>;
}
export function MonitorResult({ receipt, context }: { receipt: MonitorReceipt; context: MonitorContext }) {
    const matches = monitorContextMatches(receipt.target.context, context);
    return <section className="ops-monitor-result" aria-label="Latest monitor rule result" role="status">
        <div className="ops-monitor-result-heading"><h2>{receipt.target.kind === 'delete' ? 'Delete rule result' : 'Save rule result'}</h2>
            <StatusBadge tone={receipt.outcome === 'success' ? 'success' : 'warning'}>{receipt.outcome === 'success' ? 'Committed' : receipt.outcome === 'conflict' ? 'Version conflict' : 'Not confirmed'}</StatusBadge>
        </div>
        <p>{receipt.message}</p>
        {!matches && <p className="ops-monitor-note">This result belongs to the original connection shown below. It does not update the current environment’s rules.</p>}
        <MonitorTargetDetails target={receipt.target} />
        <p className="ops-monitor-note">Completed {new Date(receipt.completedAt).toLocaleString()}</p>
    </section>;
}
