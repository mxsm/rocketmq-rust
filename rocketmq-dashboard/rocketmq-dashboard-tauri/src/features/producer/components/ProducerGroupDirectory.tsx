import { useEffect, useMemo, useRef, useState } from 'react';
import { ProducerService } from '../../../services/producer.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import { Pagination } from '../../../components/Pagination';
import { ProducerRequestGuard } from '../requestGuard';
import type { ProducerGroupItem } from '../types/producer.types';

export function ProducerGroupDirectory({ selectedGroup, onSelect }: { selectedGroup: string; onSelect: (group: string) => void }) {
    const [items, setItems] = useState<ProducerGroupItem[] | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState('');
    const [query, setQuery] = useState('');
    const [page, setPage] = useState(1);
    const guard = useRef(new ProducerRequestGuard());
    const load = async () => {
        const isCurrent = guard.current.begin();
        setLoading(true);
        setError('');
        setItems(null);
        try {
            const groups = await ProducerService.listProducerGroups();
            if (!isCurrent()) return;
            setItems(groups);
            setPage(1);
        } catch (error) {
            if (isCurrent()) setError(dashboardErrorMessage(error, 'Unable to read the Producer directory.'));
        } finally { if (isCurrent()) setLoading(false); }
    };
    useEffect(() => { void load(); return () => guard.current.invalidate(); }, []);
    const filtered = useMemo(() => (items ?? []).filter(item => item.producerGroup.toLowerCase().includes(query.trim().toLowerCase())), [items, query]);
    const totalPages = Math.max(1, Math.ceil(filtered.length / 8));
    return <section aria-label="Discovered Producer groups" className="rounded-xl border bg-white p-5 dark:border-gray-800 dark:bg-gray-900">
        <header className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <h2 className="font-semibold">Discovered Producer groups</h2>
            <input aria-label="Search Producer groups" placeholder="Search group name" value={query} onChange={event => { setQuery(event.target.value); setPage(1); }} className="rounded border bg-transparent px-3 py-2 text-sm" />
            <button type="button" disabled={loading} onClick={() => void load()} className="rounded border px-3 py-2 text-sm">{loading ? 'Loading…' : 'Refresh directory'}</button>
        </header>
        <p className="mb-3 text-sm text-gray-500">Discovery reports group names and Broker connection counts, not a complete client list. Coverage is not reported; counts can overlap across Brokers. Select a group, then query its Topic connections below. Manual group input remains available.</p>
        {error && <p role="alert" className="text-red-600">{error}</p>}
        {!loading && !error && items === null && <p>Directory has not been queried.</p>}
        {!loading && items?.length === 0 && <p>No Producer groups were reported. Discovery coverage is unavailable.</p>}
        {!loading && Boolean(items?.length) && filtered.length === 0 && <p>No group matches this search.</p>}
        <div className="grid gap-2 sm:grid-cols-2">{filtered.slice((page - 1) * 8, page * 8).map(item => <button key={item.producerGroup} type="button"
            aria-pressed={selectedGroup === item.producerGroup} onClick={() => onSelect(item.producerGroup)}
            className={`flex items-center justify-between gap-3 rounded border p-3 text-left text-sm ${selectedGroup === item.producerGroup ? 'border-blue-500 bg-blue-50 dark:bg-blue-950' : ''}`}>
            <span className="break-all font-mono">{item.producerGroup}</span><span>{item.reportedConnectionCount} reported connections</span>
        </button>)}</div>
        {totalPages > 1 && <Pagination currentPage={page} totalPages={totalPages} onPageChange={setPage} />}
    </section>;
}
