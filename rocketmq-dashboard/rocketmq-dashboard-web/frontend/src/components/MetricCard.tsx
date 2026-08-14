import type { ReactNode } from 'react';
import { Card } from './ui/Card';

interface MetricCardProps {
  label: string;
  value: string | number;
  detail?: string;
  icon?: ReactNode;
}

export default function MetricCard({ label, value, detail, icon }: MetricCardProps) {
  return (
    <Card className="metric-card" role="group" aria-label={`${label}: ${value}`}>
      <div className="metric-card-top">
        <span>{label}</span>
        {icon ? <div className="metric-icon">{icon}</div> : null}
      </div>
      <strong>{value}</strong>
      {detail ? <small>{detail}</small> : null}
    </Card>
  );
}
