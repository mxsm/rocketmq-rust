import { useId } from 'react';
import { Button } from '../../components/ui/LegacyButton';
import { Input } from '../../components/ui/LegacyInput';
import { PageState } from '../../components/layout/PageState';
import { auditOutcomes, type AuditFilters as Filters } from './auditModel';

export function AuditFilters({ filters, pending, error, changed, onChange, apply, reset }: {
    filters: Filters; pending: boolean; error: string; changed: boolean;
    onChange: (filters: Filters) => void; apply: () => void; reset: () => void;
}) {
    const outcomeId = useId();
    return <form className="ops-audit-filters" noValidate onSubmit={event => { event.preventDefault(); apply(); }} aria-label="Audit filters">
        <div className="ops-audit-filter-fields">
            <Input label="Actor" placeholder="Exact username" value={filters.actor} onChange={event => onChange({ ...filters, actor: event.target.value })} />
            <Input label="Action" placeholder="Exact action, e.g. topic.delete" value={filters.action} onChange={event => onChange({ ...filters, action: event.target.value })} />
            <div className="ops-field"><label className="ops-field-label" htmlFor={outcomeId}>Outcome</label><select id={outcomeId} value={filters.outcome} onChange={event => onChange({ ...filters, outcome: event.target.value })}>
                <option value="">All outcomes</option>{auditOutcomes.map(outcome => <option value={outcome} key={outcome}>{outcome[0].toUpperCase() + outcome.slice(1)}</option>)}
            </select></div>
            <Input label="Environment ID" placeholder="Exact ID, if recorded" value={filters.environmentId} onChange={event => onChange({ ...filters, environmentId: event.target.value })} />
            <Input label="From (local time)" type="datetime-local" step="1" value={filters.from} onChange={event => onChange({ ...filters, from: event.target.value })} />
            <Input label="To (local time)" type="datetime-local" step="1" value={filters.to} onChange={event => onChange({ ...filters, to: event.target.value })} />
        </div>
        {error && <PageState kind="error" title="Check the audit filters" description={error} />}
        <div className="ops-audit-filter-actions"><Button type="submit">Apply filters</Button><Button variant="outline" onClick={reset}>Reset</Button>
            <p className="ops-audit-note">{changed ? 'Showing the last applied filters. Apply to run this selection.' : pending ? 'Loading the applied filters…' : 'Exact matching · newest first · up to 50 events per page'}</p>
        </div>
        <p className="ops-audit-note">These filters query the local audit log across recorded environments. The connection toolbar does not restrict the results.</p>
    </form>;
}
