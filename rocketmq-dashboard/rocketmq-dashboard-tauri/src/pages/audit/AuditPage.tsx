import { useCallback, useState } from 'react';
import { ArrowDown, ChevronRight } from 'lucide-react';
import { queryAuditEvents } from '../../services/audit.service';
import { useReadResource } from '../../hooks/useReadResource';
import { useNavigationState } from '../../stores/app.store';
import { usePageRefresh } from '../../app/layout/pageToolbar';
import { Button } from '../../components/ui/LegacyButton';
import { PageState } from '../../components/layout/PageState';
import { StatusBadge } from '../../components/layout/StatusBadge';
import { AuditFilters } from './AuditFilters';
import { AuditDetails } from './AuditDetails';
import { auditOutcome, auditQuery, auditTimestamp, emptyAuditFilters, initialAuditLocation, navigateAudit } from './auditModel';
import './audit.css';

const initialView = () => ({ draft: emptyAuditFilters(), applied: emptyAuditFilters(), location: initialAuditLocation(), selected: null as string | null });

export function AuditPage() {
    const [view, setView] = useNavigationState('auditView', initialView);
    const [validation, setValidation] = useState('');
    const cursor = view.location.cursors[view.location.cursors.length - 1];
    const load = useCallback(() => queryAuditEvents({ ...view.location.query, cursor }), [view.location.query, view.location.generation, cursor]);
    const page = useReadResource(load, 'Audit events could not be loaded.');
    const refresh = useCallback(() => setView(previous => ({ ...previous, location: navigateAudit(previous.location, { kind: 'refresh' }), selected: null })), [setView]);
    usePageRefresh({ refresh, pending: page.pending, refreshedAt: page.receivedAt });
    const apply = () => {
        try {
            const query = auditQuery(view.draft);
            setValidation('');
            setView(previous => ({ ...previous, applied: { ...previous.draft }, location: navigateAudit(previous.location, { kind: 'apply', query }), selected: null }));
        } catch (error) { setValidation(error instanceof Error ? error.message : 'Choose valid audit filters.'); }
    };
    const reset = () => {
        setValidation('');
        setView(previous => ({ ...initialView(), location: navigateAudit(previous.location, { kind: 'apply', query: { limit: 50 } }) }));
    };
    const selected = page.data?.items.find(event => event.eventId === view.selected) ?? null;
    const usable = Boolean(page.data) && !page.pending && !page.error;
    const next = page.data?.nextCursor ?? null;
    return <div className="ops-audit">
        <AuditFilters filters={view.draft} pending={page.pending} error={validation} changed={JSON.stringify(view.draft) !== JSON.stringify(view.applied)}
            onChange={draft => setView(previous => ({ ...previous, draft }))} apply={apply} reset={reset} />
        {page.pending && <PageState kind="loading" title="Loading audit events" />}
        {page.error && <PageState kind="error" title="Audit query failed" description={page.error} action={<Button variant="outline" onClick={() => { void page.read(); }}>Retry this page</Button>} />}
        {page.data && <div className="ops-audit-table-wrap" role="region" tabIndex={0} aria-label="Audit events" aria-busy={page.pending}>
            <table className="ops-audit-table"><caption className="sr-only">Filtered audit records, newest first</caption>
                <thead><tr><th scope="col">Time <ArrowDown aria-hidden="true" size={14} style={{ display: 'inline', verticalAlign: 'middle' }} /></th>
                    <th scope="col">Actor</th><th scope="col">Action</th><th scope="col">Resource</th><th scope="col">Outcome</th><th scope="col">Details</th></tr></thead>
                <tbody>{page.data.items.map(event => {
                    const outcome = auditOutcome(event.outcome, event.detail?.resultUnknown === true);
                    return <tr key={event.eventId} data-selected={event.eventId === view.selected}>
                        <td>{auditTimestamp(event.createdAtMs)}</td><td><span className="ops-audit-cell" tabIndex={0}>{event.actor ?? 'Not recorded'}</span></td>
                        <td><span className="ops-audit-cell" tabIndex={0}>{event.action}</span></td><td><span className="ops-audit-cell" tabIndex={0}>{event.resourceName ?? 'Not recorded'}</span></td>
                        <td><StatusBadge tone={outcome.tone}>{outcome.label}</StatusBadge></td>
                        <td><Button variant="ghost" icon={ChevronRight} disabled={!usable} aria-label={`View ${event.action} audit record ${event.eventId}`} aria-expanded={event.eventId === view.selected}
                            onClick={() => setView(previous => ({ ...previous, selected: event.eventId }))}>View</Button></td>
                    </tr>;
                })}</tbody>
            </table>
        </div>}
        {usable && page.data?.items.length === 0 && <PageState kind="empty" title="No matching audit events" description="Change the exact filters or time range to inspect other records." />}
        {selected && <AuditDetails event={selected} />}
        {(page.data || view.location.cursors.length > 1) && <nav className="ops-audit-pagination" aria-label="Audit pagination">
            <Button variant="outline" disabled={page.pending || view.location.cursors.length === 1} onClick={() => setView(previous => ({ ...previous, location: navigateAudit(previous.location, { kind: 'previous' }), selected: null }))}>Previous</Button>
            <span>Page {view.location.cursors.length}</span>
            <Button variant="outline" disabled={!usable || !next || view.location.cursors.includes(next)} onClick={() => setView(previous => ({ ...previous, location: navigateAudit(previous.location, { kind: 'next', cursor: next }), selected: null }))}>Next</Button>
        </nav>}
    </div>;
}
