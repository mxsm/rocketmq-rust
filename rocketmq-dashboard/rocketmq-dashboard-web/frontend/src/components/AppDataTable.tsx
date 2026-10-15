import { useState, type AnimationEvent, type KeyboardEvent, type ReactNode } from 'react';
import EmptyState from './EmptyState';
import ErrorState from './ErrorState';
import LoadingState from './LoadingState';
import { Button } from './ui/Button';

export interface AppDataTableColumn<T> {
  id: string;
  header: ReactNode;
  cell: (row: T) => ReactNode;
  width?: string;
  align?: 'left' | 'right' | 'center';
}

/** Rows that fade in one after another the first time a table shows data. */
const ENTRANCE_ROW_COUNT = 12;

interface AppDataTableProps<T> {
  ariaLabel: string;
  rows: T[];
  columns: AppDataTableColumn<T>[];
  getRowId: (row: T) => string;
  page: number;
  pageSize: number;
  total: number;
  onPageChange: (page: number) => void;
  hasNextPage?: boolean;
  onRowActivate?: (row: T, origin: HTMLElement) => void;
  loading?: boolean;
  error?: string | null;
  onRetry?: () => void;
  retryLabel?: string;
  emptyTitle?: string;
  emptyDetail?: string;
}

export default function AppDataTable<T>({
  ariaLabel,
  rows,
  columns,
  getRowId,
  page,
  pageSize,
  total,
  onPageChange,
  hasNextPage,
  onRowActivate,
  loading = false,
  error,
  onRetry,
  retryLabel,
  emptyTitle = 'No rows',
  emptyDetail
}: AppDataTableProps<T>) {
  const countedPages = Math.max(1, Math.ceil(total / pageSize));
  const pageCount = hasNextPage === undefined
    ? countedPages
    : hasNextPage
      ? Math.max(countedPages, page + 1)
      : Math.max(countedPages, page);
  const currentPage = Math.min(Math.max(page, 1), pageCount);
  const firstRow = total === 0 ? 0 : (currentPage - 1) * pageSize + 1;
  const lastRow = total === 0 ? 0 : Math.min(firstRow + rows.length - 1, total);

  // The entrance plays once per table. After the last staggered row lands the marker goes
  // away, so paging, filtering and refreshes swap rows without replaying it.
  const [entered, setEntered] = useState(false);
  const handleRowEntranceEnd = (event: AnimationEvent<HTMLTableSectionElement>) => {
    const body = event.currentTarget;
    if (event.target === body.rows[Math.min(body.rows.length, ENTRANCE_ROW_COUNT) - 1]) setEntered(true);
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLTableRowElement>, row: T) => {
    if (!onRowActivate || event.target !== event.currentTarget || (event.key !== 'Enter' && event.key !== ' ')) return;
    event.preventDefault();
    onRowActivate(row, event.currentTarget);
  };

  const isInteractiveTarget = (target: EventTarget | null) => (
    target instanceof Element
    && target.closest('a, button, input, select, textarea, [role="button"], [role="link"], [role="menuitem"]') !== null
  );

  return (
    <section className="app-data-table">
      {loading ? <LoadingState label={`Loading ${ariaLabel.toLowerCase()}`} /> : null}
      {!loading && error ? <ErrorState message={error} onRetry={onRetry} retryLabel={retryLabel} /> : null}
      {!loading && !error && rows.length === 0 ? <EmptyState title={emptyTitle} detail={emptyDetail} /> : null}
      {!loading && !error && rows.length > 0 ? (
        <div className="app-data-table-scroll" role="region" aria-label={ariaLabel} tabIndex={0}>
          <table>
            <thead>
              <tr>
                {columns.map((column) => (
                  <th key={column.id} scope="col" data-align={column.align} style={{ width: column.width, textAlign: column.align }}>
                    {column.header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody data-entering={entered ? undefined : 'true'} onAnimationEnd={handleRowEntranceEnd}>
              {rows.map((row) => (
                <tr
                  key={getRowId(row)}
                  tabIndex={onRowActivate ? 0 : undefined}
                  className={onRowActivate ? 'app-data-table-row-interactive' : undefined}
                  onClick={onRowActivate ? (event) => {
                    if (!isInteractiveTarget(event.target)) onRowActivate(row, event.currentTarget);
                  } : undefined}
                  onKeyDown={(event) => handleKeyDown(event, row)}
                >
                  {columns.map((column) => (
                    <td key={column.id} data-align={column.align} style={{ textAlign: column.align }}>
                      {column.cell(row)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
      {!loading && !error ? (
        <footer className="app-data-table-footer">
          <span>
            {firstRow}-{lastRow} of {total}{hasNextPage === true ? '+' : ''}
          </span>
          <div className="app-data-table-pagination">
            <Button
              type="button"
              variant="outline"
              size="sm"
              aria-label="Previous page"
              disabled={currentPage <= 1}
              onClick={() => onPageChange(currentPage - 1)}
            >
              Previous
            </Button>
            <span aria-label={`Page ${currentPage} of ${pageCount}`}>{currentPage} / {pageCount}</span>
            <Button
              type="button"
              variant="outline"
              size="sm"
              aria-label="Next page"
              disabled={hasNextPage === undefined ? currentPage >= pageCount : !hasNextPage}
              onClick={() => onPageChange(currentPage + 1)}
            >
              Next
            </Button>
          </div>
        </footer>
      ) : null}
    </section>
  );
}
