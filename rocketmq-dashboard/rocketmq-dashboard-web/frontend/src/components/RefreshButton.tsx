import { RefreshCw } from 'lucide-react';
import { Button } from './ui/Button';

interface RefreshButtonProps {
  refreshing?: boolean;
  onRefresh: () => void;
  compact?: boolean;
}

export default function RefreshButton({ refreshing = false, onRefresh, compact = false }: RefreshButtonProps) {
  return (
    <Button
      type="button"
      variant="outline"
      size={compact ? 'icon' : 'sm'}
      loading={refreshing}
      aria-label={refreshing ? 'Refreshing' : 'Refresh'}
      onClick={onRefresh}
    >
      {!refreshing ? <RefreshCw size={15} aria-hidden="true" /> : null}
      {!compact ? (refreshing ? 'Refreshing' : 'Refresh') : null}
    </Button>
  );
}
