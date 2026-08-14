import { CircleCheck, CircleHelp, CircleX, TriangleAlert } from 'lucide-react';
import { Badge } from './ui/Badge';

interface StatusBadgeProps {
  status: string;
  tone?: 'success' | 'warning' | 'danger' | 'neutral' | 'info';
}

export default function StatusBadge({ status, tone = 'neutral' }: StatusBadgeProps) {
  const Icon = tone === 'success' ? CircleCheck : tone === 'warning' ? TriangleAlert : tone === 'danger' ? CircleX : CircleHelp;
  return (
    <Badge
      className={`status-badge status-${tone}`}
      tone={tone === 'danger' ? 'destructive' : tone}
      role="status"
      aria-label={status}
    >
      <Icon size={12} aria-hidden="true" />
      {status}
    </Badge>
  );
}
