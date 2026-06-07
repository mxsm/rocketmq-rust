interface StatusBadgeProps {
  status: string;
  tone?: 'success' | 'warning' | 'danger' | 'neutral';
}

export default function StatusBadge({ status, tone = 'neutral' }: StatusBadgeProps) {
  return <span className={`status-badge status-${tone}`}>{status}</span>;
}
