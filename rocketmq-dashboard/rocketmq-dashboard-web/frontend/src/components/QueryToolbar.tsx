import { RotateCcw, Search } from 'lucide-react';
import type { ReactNode } from 'react';
import { Button } from './ui/Button';
import { Input } from './ui/Input';

interface QueryToolbarProps {
  searchValue: string;
  searchPlaceholder: string;
  onSearchChange: (value: string) => void;
  onReset?: () => void;
  children?: ReactNode;
  actions?: ReactNode;
}

export default function QueryToolbar({
  searchValue,
  searchPlaceholder,
  onSearchChange,
  onReset,
  children,
  actions
}: QueryToolbarProps) {
  return (
    <div className="query-toolbar">
      <label className="query-search">
        <span className="sr-only">{searchPlaceholder}</span>
        <Search size={16} aria-hidden="true" />
        <Input
          type="search"
          value={searchValue}
          placeholder={searchPlaceholder}
          aria-label={searchPlaceholder}
          onChange={(event) => onSearchChange(event.target.value)}
        />
      </label>
      {children ? <div className="query-filters">{children}</div> : null}
      <div className="query-actions">
        {onReset ? (
          <Button type="button" variant="ghost" size="sm" onClick={onReset} aria-label="Reset filters">
            <RotateCcw size={14} aria-hidden="true" />
            Reset
          </Button>
        ) : null}
        {actions}
      </div>
    </div>
  );
}
