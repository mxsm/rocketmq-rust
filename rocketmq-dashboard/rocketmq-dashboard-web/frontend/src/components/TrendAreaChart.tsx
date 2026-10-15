import { useId } from 'react';
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { usePrefersReducedMotion } from '../lib/usePrefersReducedMotion';
import EmptyState from './EmptyState';

export interface TrendAreaChartPoint {
  time: string;
  value: number;
}

/** Slot in the chart palette (`--chart-1`, `--chart-2`). */
export type TrendSeries = 1 | 2;

interface TrendAreaChartProps {
  data: TrendAreaChartPoint[];
  series?: TrendSeries;
  label: string;
  emptyTitle: string;
  emptyDetail: string;
}

const AXIS_TICK = { fill: 'var(--muted-foreground)', fontSize: 12 };

export default function TrendAreaChart({ data, series = 1, label, emptyTitle, emptyDetail }: TrendAreaChartProps) {
  const gradientId = `trend-fill-${useId().replace(/:/g, '')}`;
  const reducedMotion = usePrefersReducedMotion();

  if (data.length === 0) {
    return <EmptyState title={emptyTitle} detail={emptyDetail} />;
  }

  const stroke = `var(--chart-${series})`;
  const lastIndex = data.length - 1;

  return (
    <div className="dashboard-trend-chart" data-series={series}>
      <ResponsiveContainer width="100%" height={252}>
        <AreaChart data={data} margin={{ top: 10, right: 12, bottom: 2, left: 12 }}>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" style={{ stopColor: `var(--chart-${series}-fill)` }} />
              <stop offset="100%" style={{ stopColor: `var(--chart-${series}-fade)` }} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke="var(--chart-grid)" strokeDasharray="4 4" vertical={false} />
          <XAxis dataKey="time" axisLine={false} tickLine={false} minTickGap={28} tick={AXIS_TICK} />
          <YAxis axisLine={false} tickLine={false} width={56} tick={AXIS_TICK} />
          <Tooltip
            cursor={{ stroke: 'var(--border-strong)' }}
            contentStyle={{
              background: 'var(--surface-overlay)',
              border: '1px solid var(--border)',
              borderRadius: 8,
              boxShadow: 'var(--shadow-overlay)',
              color: 'var(--foreground)'
            }}
            itemStyle={{ color: 'var(--foreground)' }}
            labelStyle={{ color: 'var(--muted-foreground)' }}
          />
          <Area
            type="monotone"
            dataKey="value"
            name={label}
            stroke={stroke}
            strokeWidth={2}
            fill={`url(#${gradientId})`}
            fillOpacity={1}
            isAnimationActive={!reducedMotion}
            animationDuration={1000}
            animationEasing="ease-out"
            activeDot={{ r: 4, strokeWidth: 0, fill: stroke }}
            dot={({ cx, cy, index }: { cx?: number; cy?: number; index?: number }) => (
              index === lastIndex && cx !== undefined && cy !== undefined ? (
                <g key="latest" className="trend-latest-point">
                  <circle className="trend-latest-halo" cx={cx} cy={cy} r={5} fill={stroke} />
                  <circle cx={cx} cy={cy} r={4} fill={stroke} stroke="var(--card)" strokeWidth={2} />
                </g>
              ) : <g key={`point-${index}`} />
            )}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
