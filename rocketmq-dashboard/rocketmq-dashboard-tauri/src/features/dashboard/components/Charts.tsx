import { useMemo, type ReactNode } from 'react';
import { BarChart3, PieChart, RefreshCw, TrendingUp } from 'lucide-react';
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
  LabelList,
} from 'recharts';
import { Button } from '../../../components/ui/LegacyButton';
import { useDashboardCharts } from '../hooks/useDashboardCharts';

const formatNumber = (value: number) =>
  new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(value);

const compactLabel = (value: string) => {
  if (value.length <= 18) {
    return value;
  }

  return `${value.slice(0, 15)}...`;
};

const chartTooltipStyle = {
  borderRadius: '8px',
  border: '1px solid rgba(71, 85, 105, 0.82)',
  backgroundColor: '#10151c',
  color: '#e2e8f0',
  boxShadow: '0 18px 36px rgba(0,0,0,0.32)',
};

const ChartState = ({
  isLoading,
  error,
  isEmpty,
  children,
}: {
  isLoading: boolean;
  error: string;
  isEmpty: boolean;
  children: ReactNode;
}) => {
  if (error) {
    return <div className="dashboard-chart-state is-error">{error}</div>;
  }

  if (isLoading) {
    return <div className="dashboard-chart-state">Loading chart data...</div>;
  }

  if (isEmpty) {
    return <div className="dashboard-chart-state">No data available</div>;
  }

  return <>{children}</>;
};

const DashboardChartPanel = ({
  title,
  description,
  icon,
  accent,
  className = '',
  headerAction,
  children,
}: {
  title: string;
  description: string;
  icon: ReactNode;
  accent: string;
  className?: string;
  headerAction?: ReactNode;
  children: ReactNode;
}) => (
  <section className={`dashboard-chart-panel ${className}`}>
    <div className="dashboard-chart-header">
      <div className="dashboard-chart-title-wrap">
        <span className="dashboard-chart-icon" style={{ color: accent }}>
          {icon}
        </span>
        <div>
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
      </div>
      {headerAction}
    </div>
    {children}
  </section>
);

interface BrokerTopChartItem {
  name: string;
  messages: number;
  address: string;
}

interface BrokerTpsChartItem {
  name: string;
  address: string;
  produceTps: number;
  consumeTps: number;
}

interface TopicTopChartItem {
  topic: string;
  totalMsg: number;
  producedMsgCount24h: number;
  consumedMsgCount24h: number;
  consumerGroupCount: number;
}

const BrokerTopChart = ({ data }: { data: BrokerTopChartItem[] }) => {
  const maxMessages = Math.max(...data.map((item) => item.messages), 1);
  const chartWidth = Math.max(data.length * 92, 360);
  const chartHeight = 292;
  const plotTop = 30;
  const plotBottom = 58;
  const plotLeft = 18;
  const plotRight = 18;
  const plotHeight = chartHeight - plotTop - plotBottom;
  const slotWidth = (chartWidth - plotLeft - plotRight) / data.length;
  const barWidth = Math.min(48, Math.max(30, slotWidth * 0.56));

  return (
    <div className="dashboard-chart-scroll">
      <svg
        role="img"
        aria-label="Broker Top 10 received messages chart"
        viewBox={`0 0 ${chartWidth} ${chartHeight}`}
        style={{
          display: 'block',
          width: '100%',
          minWidth: `${chartWidth}px`,
          height: `${chartHeight}px`,
        }}
      >
        {[0, 1, 2, 3].map((line) => {
          const y = plotTop + (plotHeight * line) / 3;
          return (
            <line
              key={line}
              x1={plotLeft}
              x2={chartWidth - plotRight}
              y1={y}
              y2={y}
              stroke="rgba(148, 163, 184, 0.22)"
              strokeDasharray="5 5"
            />
          );
        })}
        {data.map((item, index) => {
          const isZero = item.messages <= 0;
          const barHeight = isZero ? 5 : Math.max((item.messages / maxMessages) * plotHeight, 12);
          const x = plotLeft + index * slotWidth + (slotWidth - barWidth) / 2;
          const y = plotTop + plotHeight - barHeight;
          const centerX = x + barWidth / 2;

          return (
            <g
              key={`${item.name}-${item.address}`}
              aria-label={`${item.name} ${formatNumber(item.messages)} messages`}
            >
              <title>{`${item.name} / ${item.address}: ${formatNumber(item.messages)} messages`}</title>
              <rect
                x={x}
                y={plotTop}
                width={barWidth}
                height={plotHeight}
                rx={8}
                fill="rgba(148, 163, 184, 0.14)"
              />
              <rect
                x={x}
                y={y}
                width={barWidth}
                height={barHeight}
                rx={8}
                fill={isZero ? '#64748b' : index === 0 ? '#22d3ee' : '#0891b2'}
              />
              <text
                x={centerX}
                y={Math.max(y - 10, 12)}
                textAnchor="middle"
                dominantBaseline="middle"
                fill="#f8fafc"
                fontSize="12"
                fontWeight="700"
                fontFamily="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
              >
                {formatNumber(item.messages)}
              </text>
              <text
                x={centerX}
                y={plotTop + plotHeight + 28}
                textAnchor="middle"
                dominantBaseline="middle"
                fill="#94a3b8"
                fontSize="12"
                fontWeight="700"
              >
                {compactLabel(item.name)}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
};

const MetricBar = ({
  label,
  value,
  maxValue,
  accentColor,
  trackClassName,
}: {
  label: string;
  value: number;
  maxValue: number;
  accentColor: string;
  trackClassName: string;
}) => {
  const width = `${Math.max((value / Math.max(maxValue, 1)) * 100, value > 0 ? 3 : 0)}%`;

  return (
    <div className="dashboard-metric-bar">
      <div className="dashboard-metric-bar-header">
        <span>
          <i style={{ backgroundColor: accentColor }} />
          {label}
        </span>
        <strong>{formatNumber(value)}</strong>
      </div>
      <div className={`dashboard-metric-track ${trackClassName}`}>
        <span style={{ width, backgroundColor: accentColor }} />
      </div>
    </div>
  );
};

const BrokerTpsCompactCards = ({ data }: { data: BrokerTpsChartItem[] }) => {
  const maxTps = Math.max(...data.flatMap((item) => [item.produceTps, item.consumeTps]), 1);

  return (
    <div className="dashboard-tps-list">
      {data.map((item) => {
        const totalTps = item.produceTps + item.consumeTps;

        return (
          <div key={`${item.name}-${item.address}`} className="dashboard-tps-card">
            <div className="dashboard-tps-card-header">
              <div>
                <strong>{item.name}</strong>
                <span>{item.address}</span>
              </div>
              <div className="dashboard-tps-total">
                <span>Total</span>
                <strong>{formatNumber(totalTps)}</strong>
              </div>
            </div>

            <div className="dashboard-tps-bars">
              <MetricBar
                label="Producer TPS"
                value={item.produceTps}
                maxValue={maxTps}
                accentColor="#fb7185"
                trackClassName="is-producer"
              />
              <MetricBar
                label="Consumer TPS"
                value={item.consumeTps}
                maxValue={maxTps}
                accentColor="#3b82f6"
                trackClassName="is-consumer"
              />
            </div>
          </div>
        );
      })}
    </div>
  );
};

const TopicTopBarChart = ({ data }: { data: TopicTopChartItem[] }) => {
  const chartHeight = Math.max(300, data.length * 34 + 56);

  return (
    <div className="dashboard-topic-chart">
      <div className="dashboard-topic-legend">
        <span>
          <i />
          TotalMsg
        </span>
        <span>24h consumed messages</span>
      </div>

      <div className="dashboard-chart-canvas" style={{ height: `${chartHeight}px` }}>
        <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
          <BarChart
            data={data}
            layout="vertical"
            margin={{ top: 8, right: 92, bottom: 8, left: 8 }}
            barCategoryGap={8}
          >
            <CartesianGrid horizontal={false} stroke="rgba(148, 163, 184, 0.18)" />
            <XAxis
              type="number"
              tick={{ fontSize: 11, fill: '#94a3b8' }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(value) => formatNumber(Number(value))}
            />
            <YAxis
              type="category"
              dataKey="topic"
              width={142}
              tick={{ fontSize: 11, fill: '#cbd5e1', fontWeight: 600 }}
              tickFormatter={compactLabel}
              axisLine={false}
              tickLine={false}
            />
            <Tooltip
              cursor={{ fill: 'rgba(30, 41, 59, 0.45)' }}
              contentStyle={chartTooltipStyle}
              labelFormatter={(topic) => `Topic: ${topic}`}
              formatter={(value: number, name: string, item) => {
                if (name === 'totalMsg') {
                  return [formatNumber(value), 'TotalMsg'];
                }

                return [formatNumber(value), item.name];
              }}
            />
            <Bar dataKey="totalMsg" name="TotalMsg" fill="#34d399" radius={[0, 6, 6, 0]} barSize={18}>
              <LabelList
                dataKey="totalMsg"
                position="right"
                formatter={(value: number) => formatNumber(value)}
                fill="#bbf7d0"
                fontSize={11}
                fontWeight={700}
                fontFamily="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace"
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export const DashboardCharts = () => {
  const {
    brokerOverview,
    topicCurrent,
    isLoading,
    isRefreshing,
    brokerError,
    topicError,
    refresh,
  } = useDashboardCharts();

  const brokerTopData = useMemo(
    () =>
      (brokerOverview?.brokerTop ?? []).map((item) => ({
        name: `${item.brokerName}-${item.brokerId}`,
        messages: item.receivedTotal,
        address: item.address,
      })),
    [brokerOverview?.brokerTop]
  );

  const brokerTpsData = useMemo(
    () =>
      (brokerOverview?.brokerTps ?? []).map((item) => ({
        name: `${item.brokerName}-${item.brokerId}`,
        address: item.address,
        produceTps: Number(item.produceTps.toFixed(2)),
        consumeTps: Number(item.consumeTps.toFixed(2)),
      })),
    [brokerOverview?.brokerTps]
  );

  const topicTopData = useMemo(
    () =>
      (topicCurrent?.topicTop ?? []).map((item) => ({
        topic: item.topic,
        totalMsg: item.totalMsg,
        producedMsgCount24h: item.producedMsgCount24h,
        consumedMsgCount24h: item.consumedMsgCount24h,
        consumerGroupCount: item.consumerGroupCount,
      })),
    [topicCurrent?.topicTop]
  );

  const topicQueueData = useMemo(
    () =>
      (topicCurrent?.topicQueueTop ?? []).map((item) => ({
        name: item.topic,
        queues: item.totalQueueCount,
        readQueues: item.readQueueCount,
        writeQueues: item.writeQueueCount,
      })),
    [topicCurrent?.topicQueueTop]
  );

  const topicCategoryData = useMemo(
    () =>
      (topicCurrent?.topicCategoryDistribution ?? []).map((item) => ({
        name: item.category,
        count: item.count,
      })),
    [topicCurrent?.topicCategoryDistribution]
  );

  const isBrokerLoading = isLoading && brokerTopData.length === 0;
  const isTopicLoading =
    isLoading && topicTopData.length === 0 && topicQueueData.length === 0 && topicCategoryData.length === 0;

  const handleRefresh = async () => {
    await refresh();
  };

  return (
    <section className="dashboard-chart-workspace" aria-label="Dashboard charts">
      <div className="dashboard-section-title">
        <div>
          <span>Monitoring workspace</span>
          <h2>Broker and topic pressure</h2>
        </div>
        <Button variant="ghost" onClick={() => void handleRefresh()} disabled={isRefreshing}>
          <RefreshCw className={`ops-button-icon ${isRefreshing ? 'animate-spin' : ''}`} />
          Refresh charts
        </Button>
      </div>

      <div className="dashboard-charts-grid">
        <DashboardChartPanel
          title="Broker Top 10"
          description="Received messages ranked by broker"
          icon={<BarChart3 className="dashboard-icon" />}
          accent="#22d3ee"
          className="is-third"
        >
          <ChartState isLoading={isBrokerLoading} error={brokerError} isEmpty={brokerTopData.length === 0}>
            <BrokerTopChart data={brokerTopData} />
          </ChartState>
        </DashboardChartPanel>

        <DashboardChartPanel
          title="Broker TPS Snapshot"
          description="Current produce and consume TPS from broker runtime data"
          icon={<TrendingUp className="dashboard-icon" />}
          accent="#fb7185"
          className="is-third"
        >
          <ChartState isLoading={isBrokerLoading} error={brokerError} isEmpty={brokerTpsData.length === 0}>
            <BrokerTpsCompactCards data={brokerTpsData} />
          </ChartState>
        </DashboardChartPanel>

        <DashboardChartPanel
          title="Topic Queue Top 10"
          description="Largest topics by read and write queue count"
          icon={<BarChart3 className="dashboard-icon" />}
          accent="#818cf8"
          className="is-third"
        >
          <ChartState isLoading={isTopicLoading} error={topicError} isEmpty={topicQueueData.length === 0}>
            <div className="dashboard-chart-canvas">
              <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
                <BarChart data={topicQueueData}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="rgba(148, 163, 184, 0.22)" />
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11, fill: '#9ca3af' }}
                    tickFormatter={compactLabel}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
                  <Tooltip
                    cursor={{ fill: 'rgba(30, 41, 59, 0.45)' }}
                    contentStyle={chartTooltipStyle}
                    formatter={(value: number) => [formatNumber(value), 'Queues']}
                  />
                  <Bar dataKey="queues" fill="#818cf8" radius={[4, 4, 0, 0]} barSize={30} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </ChartState>
        </DashboardChartPanel>

        <DashboardChartPanel
          title="Topic TOP 10"
          description="TotalMsg ranked by topic"
          icon={<TrendingUp className="dashboard-icon" />}
          accent="#34d399"
          className="is-wide"
        >
          <ChartState isLoading={isTopicLoading} error={topicError} isEmpty={topicTopData.length === 0}>
            <TopicTopBarChart data={topicTopData} />
          </ChartState>
        </DashboardChartPanel>

        <DashboardChartPanel
          title="Topic Type Distribution"
          description="Current topic categories from the Topic catalog"
          icon={<PieChart className="dashboard-icon" />}
          accent="#f59e0b"
          className="is-narrow"
        >
          <ChartState isLoading={isTopicLoading} error={topicError} isEmpty={topicCategoryData.length === 0}>
            <div className="dashboard-chart-canvas">
              <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
                <BarChart data={topicCategoryData}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="rgba(148, 163, 184, 0.22)" />
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11, fill: '#9ca3af' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis allowDecimals={false} tick={{ fontSize: 11, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
                  <Tooltip
                    cursor={{ fill: 'rgba(30, 41, 59, 0.45)' }}
                    contentStyle={chartTooltipStyle}
                    formatter={(value: number) => [formatNumber(value), 'Topics']}
                  />
                  <Bar dataKey="count" fill="#f59e0b" radius={[4, 4, 0, 0]} barSize={30} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </ChartState>
        </DashboardChartPanel>
      </div>
    </section>
  );
};
