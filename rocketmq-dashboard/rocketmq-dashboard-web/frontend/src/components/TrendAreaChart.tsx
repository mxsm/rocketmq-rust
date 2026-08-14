import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import EmptyState from './EmptyState';

export interface TrendAreaChartPoint {
  time: string;
  value: number;
}

interface TrendAreaChartProps {
  data: TrendAreaChartPoint[];
  color: string;
  label: string;
  emptyTitle: string;
  emptyDetail: string;
}

export default function TrendAreaChart({ data, color, label, emptyTitle, emptyDetail }: TrendAreaChartProps) {
  if (data.length === 0) {
    return <EmptyState title={emptyTitle} detail={emptyDetail} />;
  }

  return (
    <div className="dashboard-trend-chart">
      <ResponsiveContainer width="100%" height={252}>
        <LineChart data={data} margin={{ top: 10, right: 12, bottom: 2, left: 12 }}>
          <CartesianGrid stroke="var(--border)" strokeDasharray="4 8" opacity={0.42} vertical={false} />
          <XAxis dataKey="time" axisLine={false} tickLine={false} minTickGap={28} tick={{ fill: 'var(--muted)', fontSize: 12 }} />
          <YAxis axisLine={false} tickLine={false} width={56} tick={{ fill: 'var(--muted)', fontSize: 12 }} />
          <Tooltip
            contentStyle={{
              background: 'var(--surface)',
              border: '1px solid var(--border)',
              borderRadius: 8,
              color: 'var(--text)'
            }}
            labelStyle={{ color: 'var(--muted)' }}
          />
          <Line
            type="monotone"
            dataKey="value"
            name={label}
            stroke={color}
            strokeWidth={2.5}
            dot={false}
            activeDot={{ r: 4, strokeWidth: 0 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
