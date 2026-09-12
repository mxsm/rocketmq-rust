import { Copy, Info } from 'lucide-react';
import { toast } from 'sonner';
import type { AuditEvent } from '../../services/audit.service';
import { Button } from '../../components/ui/LegacyButton';
import { StatusBadge } from '../../components/layout/StatusBadge';
import { auditDetail, auditOutcome, auditTimestamp } from './auditModel';

export function AuditDetails({ event }: { event: AuditEvent }) {
    const detail = auditDetail(event.detail);
    const outcome = auditOutcome(event.outcome, detail.resultUnknown);
    const copy = async () => {
        try { await navigator.clipboard.writeText(event.requestId); toast.success('Request ID copied'); }
        catch { toast.error('Request ID could not be copied'); }
    };
    return <section className="ops-audit-details" aria-labelledby="audit-details-heading">
        <h2 id="audit-details-heading">Audit record details</h2>
        <p className="ops-audit-note">Recorded metadata for the selected operation.</p>
        <div className="ops-audit-detail-columns">
            <dl><div><dt>Time</dt><dd>{auditTimestamp(event.createdAtMs)}</dd></div><div><dt>Actor</dt><dd tabIndex={0}>{event.actor ?? 'Not recorded'}</dd></div>
                <div><dt>Action</dt><dd tabIndex={0}>{event.action}</dd></div><div><dt>Resource type</dt><dd tabIndex={0}>{event.resourceType}</dd></div>
                <div><dt>Resource</dt><dd tabIndex={0}>{event.resourceName ?? 'Not recorded'}</dd></div><div><dt>Environment</dt><dd tabIndex={0}>{event.environmentId ?? 'Not recorded'}</dd></div>
                <div><dt>Request ID</dt><dd className="ops-audit-copy"><span tabIndex={0}>{event.requestId || 'Not recorded'}</span><Button variant="ghost" icon={Copy} aria-label="Copy request ID" disabled={!event.requestId} onClick={() => { void copy(); }} /></dd></div>
            </dl>
            <dl><div><dt>Outcome</dt><dd><StatusBadge tone={outcome.tone}>{outcome.label}</StatusBadge></dd></div>
                <div><dt>Success count</dt><dd>{detail.successCount}</dd></div><div><dt>Failure count</dt><dd>{detail.failureCount}</dd></div>
                <div><dt>Error code</dt><dd tabIndex={0}>{detail.errorCode}</dd></div>
            </dl>
        </div>
        <p className="ops-audit-notice"><Info aria-hidden="true" size={18} />{outcome.label === 'Unknown' ? 'The outcome could not be confirmed. Inspect the original resource before another operation.' : 'Unknown means the outcome could not be confirmed. Missing counts are not treated as zero.'}</p>
    </section>;
}
