import type { ReactNode } from 'react';

interface MetricCardProps {
  label: string;
  value: string | number;
  detail?: string;
  icon?: ReactNode;
}

export default function MetricCard({ label, value, detail, icon }: MetricCardProps) {
  return (
    <section className="metric-card">
      <div className="metric-card-top">
        <span>{label}</span>
        {icon ? <div className="metric-icon">{icon}</div> : null}
      </div>
      <strong>{value}</strong>
      {detail ? <small>{detail}</small> : null}
    </section>
  );
}
