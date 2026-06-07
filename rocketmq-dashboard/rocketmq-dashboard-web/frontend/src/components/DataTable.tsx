import { RefreshCw, Search } from 'lucide-react';
import type { ReactNode } from 'react';
import { useMemo, useState } from 'react';
import EmptyState from './EmptyState';

export interface DataTableColumn<T> {
  header: string;
  render: (row: T) => ReactNode;
  width?: string;
}

interface DataTableProps<T> {
  rows: T[];
  columns: DataTableColumn<T>[];
  getRowId: (row: T) => string;
  searchPlaceholder?: string;
  emptyTitle?: string;
  onRefresh?: () => void;
}

const pageSize = 10;

export default function DataTable<T>({
  rows,
  columns,
  getRowId,
  searchPlaceholder = 'Search',
  emptyTitle = 'No rows',
  onRefresh
}: DataTableProps<T>) {
  const [query, setQuery] = useState('');
  const [page, setPage] = useState(1);

  const filteredRows = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) {
      return rows;
    }
    return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(normalized));
  }, [query, rows]);

  const pageCount = Math.max(1, Math.ceil(filteredRows.length / pageSize));
  const currentPage = Math.min(page, pageCount);
  const visibleRows = filteredRows.slice((currentPage - 1) * pageSize, currentPage * pageSize);

  return (
    <section className="table-shell">
      <div className="table-toolbar">
        <label className="search-box">
          <Search size={16} aria-hidden="true" />
          <input
            value={query}
            placeholder={searchPlaceholder}
            onChange={(event) => {
              setQuery(event.target.value);
              setPage(1);
            }}
          />
        </label>
        {onRefresh ? (
          <button type="button" className="icon-button" onClick={onRefresh} title="Refresh">
            <RefreshCw size={16} aria-hidden="true" />
          </button>
        ) : null}
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.header} style={{ width: column.width }}>
                  {column.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row) => (
              <tr key={getRowId(row)}>
                {columns.map((column) => (
                  <td key={column.header}>{column.render(row)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {visibleRows.length === 0 ? <EmptyState title={emptyTitle} /> : null}
      <div className="table-footer">
        <span>
          {filteredRows.length} rows / page {currentPage} of {pageCount}
        </span>
        <div className="pagination">
          <button type="button" className="button button-secondary" disabled={currentPage <= 1} onClick={() => setPage(currentPage - 1)}>
            Previous
          </button>
          <button
            type="button"
            className="button button-secondary"
            disabled={currentPage >= pageCount}
            onClick={() => setPage(currentPage + 1)}
          >
            Next
          </button>
        </div>
      </div>
    </section>
  );
}
