import type { ReactNode } from 'react';
import { StatusDot } from './ui/StatusDot';

interface HealthSignalProps {
  icon: ReactNode;
  label: string;
  value: string;
  tone?: 'success' | 'warning' | 'danger';
}

/** One cell of a health rail: an icon tile, a label, a value and a status light. */
export default function HealthSignal({ icon, label, value, tone }: HealthSignalProps) {
  return (
    <div className={`health-signal${tone ? ` health-${tone}` : ''}`}>
      <span className="health-signal-icon" aria-hidden="true">{icon}</span>
      <span>{label}</span>
      <strong title={value}>{value}</strong>
      <StatusDot className="health-signal-dot" tone={tone ?? 'neutral'} />
    </div>
  );
}
