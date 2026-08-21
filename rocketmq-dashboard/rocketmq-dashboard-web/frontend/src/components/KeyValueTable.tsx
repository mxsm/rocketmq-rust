import { Copy, Search } from 'lucide-react';
import { useMemo, useState } from 'react';
import EmptyState from './EmptyState';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from './ui/Dialog';

export interface KeyValueRow {
  key: string;
  value: string;
}

interface KeyValueTableProps {
  rows: KeyValueRow[];
  emptyTitle: string;
}

interface JsonValue {
  formatted: string;
  summary: string;
}

function parseJsonValue(value: string): JsonValue | null {
  try {
    const parsed: unknown = JSON.parse(value);
    if (parsed === null || typeof parsed !== 'object') return null;

    if (Array.isArray(parsed)) {
      return {
        formatted: JSON.stringify(parsed, null, 2),
        summary: `JSON array · ${parsed.length} ${parsed.length === 1 ? 'item' : 'items'}`
      };
    }

    return {
      formatted: JSON.stringify(parsed, null, 2),
      summary: `JSON object · ${Object.keys(parsed).length} ${Object.keys(parsed).length === 1 ? 'field' : 'fields'}`
    };
  } catch {
    return null;
  }
}

export default function KeyValueTable({ rows, emptyTitle }: KeyValueTableProps) {
  const [query, setQuery] = useState('');
  const [jsonDetail, setJsonDetail] = useState<(JsonValue & { key: string }) | null>(null);

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
          {filteredRows.map((row) => {
            const jsonValue = parseJsonValue(row.value);

            return (
              <div className="kv-row" key={row.key}>
                <div className="kv-key" title={row.key}>
                  {row.key}
                </div>
                <div className="kv-value" title={jsonValue ? undefined : row.value}>
                  {jsonValue ? (
                    <button
                      type="button"
                      className="kv-json-button"
                      aria-label={`View JSON for ${row.key}`}
                      onClick={() => setJsonDetail({ key: row.key, ...jsonValue })}
                    >
                      {jsonValue.summary}
                    </button>
                  ) : row.value}
                </div>
                <button type="button" className="icon-button" title="Copy row" onClick={() => copyRow(row)}>
                  <Copy size={14} aria-hidden="true" />
                </button>
              </div>
            );
          })}
        </div>
      )}
      <Dialog open={jsonDetail !== null} onOpenChange={(open) => { if (!open) setJsonDetail(null); }}>
        <DialogContent className="kv-json-dialog">
          <DialogHeader>
            <DialogTitle>{jsonDetail?.key ?? 'JSON value'}</DialogTitle>
            <DialogDescription>{jsonDetail?.summary ?? 'Formatted JSON value'}</DialogDescription>
          </DialogHeader>
          {jsonDetail ? <pre className="kv-json-preview">{jsonDetail.formatted}</pre> : null}
        </DialogContent>
      </Dialog>
    </section>
  );
}
