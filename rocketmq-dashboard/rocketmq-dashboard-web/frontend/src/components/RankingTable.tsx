import type { CSSProperties } from 'react';
import EmptyState from './EmptyState';

export interface RankingTableRow {
  name: string;
  value: number;
  detail: string;
}

interface RankingTableProps {
  rows: RankingTableRow[];
  valueLabel: string;
  accent: string;
  emptyTitle: string;
  emptyDetail: string;
  formatValue?: (value: number) => string;
}

export default function RankingTable({
  rows,
  valueLabel,
  accent,
  emptyTitle,
  emptyDetail,
  formatValue = (value) => String(Math.round(value))
}: RankingTableProps) {
  const maxValue = Math.max(...rows.map((row) => row.value), 1);

  if (rows.length === 0) {
    return <EmptyState title={emptyTitle} detail={emptyDetail} />;
  }

  return (
    <div className="ranking-table" style={{ '--rank-accent': accent } as CSSProperties}>
      {rows.map((row, index) => {
        const isZero = row.value <= 0;
        const width = isZero ? 0 : Math.max(4, (row.value / maxValue) * 100);

        return (
          <div className={isZero ? 'ranking-row ranking-row-zero' : 'ranking-row'} key={row.name}>
            <span className={index < 3 && !isZero ? 'rank-badge rank-badge-hot' : 'rank-badge'}>{index + 1}</span>
            <div className="ranking-main">
              <div className="ranking-header">
                <strong title={row.name}>{row.name}</strong>
                <span>{row.detail}</span>
              </div>
              <div className="ranking-track" aria-label={`${row.name} ${valueLabel} ${row.value}`}>
                <span style={{ width: `${width}%` }} />
              </div>
            </div>
            <div className="ranking-value">
              <strong>{formatValue(row.value)}</strong>
              <span>{valueLabel}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}
