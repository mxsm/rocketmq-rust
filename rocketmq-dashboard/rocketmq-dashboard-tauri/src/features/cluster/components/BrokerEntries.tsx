import { useMemo, useState } from 'react';
import { Copy } from 'lucide-react';
import { toast } from 'sonner';
import { DETAIL_CATEGORIES, categorizeDetailKey } from '../../../components/ui/detailCategories';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { PageState } from '../../../components/layout/PageState';

export function BrokerEntries({ entries, onChange, disabled = false }: {
    entries: Record<string, string>; onChange?: (key: string, value: string) => void; disabled?: boolean;
}) {
    const [query, setQuery] = useState('');
    const [category, setCategory] = useState('All');
    const [copied, setCopied] = useState('');
    const rows = useMemo(() => Object.entries(entries).map(([key, value]) => ({ key, value, category: categorizeDetailKey(key) })), [entries]);
    const visible = rows.filter(row => (category === 'All' || row.category === category)
        && (row.key + ' ' + row.value).toLowerCase().includes(query.trim().toLowerCase()));
    const copy = async (key: string, value: string) => {
        try { await navigator.clipboard.writeText(value); setCopied(key); }
        catch { toast.error('Unable to copy. Select and copy the value manually.'); }
    };
    return <div className="ops-broker-entries">
        <div className="ops-broker-entry-filters">
            <Input label="Search entries" placeholder="Key or value" value={query} onChange={event => setQuery(event.target.value)} />
            <label className="ops-broker-category"><span>Category</span><select value={category} onChange={event => setCategory(event.target.value)}>
                {DETAIL_CATEGORIES.map(name => <option key={name} value={name}>{name}</option>)}
            </select></label>
            {!onChange && <Button variant="outline" icon={Copy} onClick={() => void copy('all', rows.map(row => row.key + '=' + row.value).join('\n'))}>
                {copied === 'all' ? 'Copied' : 'Copy all'}</Button>}
        </div>
        <div className="ops-cluster-table-scroll" role="region" aria-label={onChange ? 'Editable Broker values' : 'Broker key value entries'} tabIndex={0}>
            <table className="ops-cluster-entry-table"><thead><tr><th scope="col">Key</th><th scope="col">Value</th>{!onChange && <th scope="col"><span className="sr-only">Actions</span></th>}</tr></thead>
                <tbody>{visible.map(row => <tr key={row.key}><th scope="row"><div className="ops-broker-entry-key"><span>{row.key}</span><small>{row.category}</small></div></th>
                    <td>{onChange ? <Input aria-label={row.key} value={row.value} readOnly={disabled}
                        onChange={event => onChange(row.key, event.target.value)} />
                        : <code className="ops-broker-entry-value" tabIndex={0} aria-label={'Value for ' + row.key}>
                            {row.value || <span className="ops-cluster-muted">Not set</span>}</code>}</td>
                    {!onChange && <td><Button variant="ghost" icon={Copy} aria-label={'Copy ' + row.key} onClick={() => void copy(row.key, row.value)}>
                        {copied === row.key ? 'Copied' : null}</Button></td>}
                </tr>)}</tbody>
            </table>
        </div>
        {!visible.length && <PageState kind="empty" title={rows.length ? 'No matching entries' : 'No entries returned'}
            description={rows.length ? 'Change the key, value or category filter.' : 'The Broker returned an empty result.'} />}
    </div>;
}
