import { Copy, Search } from 'lucide-react';
import { useMemo, useState } from 'react';
import EmptyState from './EmptyState';

export interface KeyValueRow {
  key: string;
  value: string;
}

interface KeyValueTableProps {
  rows: KeyValueRow[];
  emptyTitle: string;
}

export default function KeyValueTable({ rows, emptyTitle }: KeyValueTableProps) {
  const [query, setQuery] = useState('');

  const filteredRows = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) return rows;
    return rows.filter((row) => `${row.key} ${row.value}`.toLowerCase().includes(normalized));
  }, [query, rows]);

  const copyRow = (row: KeyValueRow) => {
    void navigator.clipboard?.writeText(`${row.key}=${row.value}`);
  };

  return (
    <section className="kv-shell">
      <div className="kv-toolbar">
        <label className="search-box">
          <Search size={16} aria-hidden="true" />
          <input value={query} placeholder="Search key or value" onChange={(event) => setQuery(event.target.value)} />
        </label>
        <span>{filteredRows.length} rows</span>
      </div>
      {filteredRows.length === 0 ? (
        <EmptyState title={emptyTitle} />
      ) : (
        <div className="kv-list">
          {filteredRows.map((row) => (
            <div className="kv-row" key={row.key}>
              <div className="kv-key" title={row.key}>
                {row.key}
              </div>
              <code className="kv-value">{row.value}</code>
              <button type="button" className="icon-button" title="Copy row" onClick={() => copyRow(row)}>
                <Copy size={14} aria-hidden="true" />
              </button>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
