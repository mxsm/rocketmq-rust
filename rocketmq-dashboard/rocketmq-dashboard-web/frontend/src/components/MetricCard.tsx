import type { ReactNode } from 'react';
import { useCountUp } from '../lib/useCountUp';
import { Card } from './ui/Card';

export type MetricAccent = 'primary' | 'blue' | 'cyan' | 'pink' | 'amber';

interface MetricCardProps {
  label: string;
  /** A number counts up to its next value when it changes; a string is shown as is. */
  value: string | number;
  detail?: string;
  icon?: ReactNode;
  /** Colors the round icon and the marker under the value. */
  accent?: MetricAccent;
}

function MetricNumber({ value }: { value: number }) {
  return <>{useCountUp(value)}</>;
}

export default function MetricCard({ label, value, detail, icon, accent = 'primary' }: MetricCardProps) {
  return (
    <Card className="metric-card" role="group" aria-label={`${label}: ${value}`} data-accent={accent}>
      <div className="metric-card-top">
        <span>{label}</span>
        {icon ? <div className="metric-icon">{icon}</div> : null}
      </div>
      <strong>{typeof value === 'number' ? <MetricNumber value={value} /> : value}</strong>
      {detail ? <small>{detail}</small> : null}
    </Card>
  );
}
