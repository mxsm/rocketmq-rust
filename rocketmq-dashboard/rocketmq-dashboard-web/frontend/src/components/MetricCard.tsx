import type { ReactNode } from 'react';
import { Card } from './ui/Card';

export type MetricAccent = 'primary' | 'blue' | 'cyan' | 'pink' | 'amber';

interface MetricCardProps {
  label: string;
  value: string | number;
  detail?: string;
  icon?: ReactNode;
  /** Colors the round icon and the marker under the value. */
  accent?: MetricAccent;
}

export default function MetricCard({ label, value, detail, icon, accent = 'primary' }: MetricCardProps) {
  return (
    <Card className="metric-card" role="group" aria-label={`${label}: ${value}`} data-accent={accent}>
      <div className="metric-card-top">
        <span>{label}</span>
        {icon ? <div className="metric-icon">{icon}</div> : null}
      </div>
      <strong>{value}</strong>
      {detail ? <small>{detail}</small> : null}
    </Card>
  );
}
