import { useMemo, type ReactNode } from 'react';
import { RefreshCw } from 'lucide-react';
import {
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
    BarChart,
    Bar,
    LineChart,
    Line,
} from 'recharts';
import { Card } from '../../../components/ui/LegacyCard';
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
    borderRadius: '12px',
    border: 'none',
    boxShadow: '0 4px 12px rgba(0,0,0,0.1)',
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
        return (
            <div className="mt-4 rounded-xl border border-red-200 bg-red-50/80 dark:border-red-900/40 dark:bg-red-950/20 px-4 py-3 text-sm text-red-700 dark:text-red-300">
                {error}
            </div>
        );
    }

    if (isLoading) {
        return (
            <div className="mt-4 flex h-[300px] items-center justify-center rounded-xl border border-dashed border-gray-200 text-sm text-gray-500 dark:border-gray-800 dark:text-gray-400">
                Loading chart data...
            </div>
        );
    }

    if (isEmpty) {
        return (
            <div className="mt-4 flex h-[300px] items-center justify-center rounded-xl border border-dashed border-gray-200 text-sm text-gray-500 dark:border-gray-800 dark:text-gray-400">
                No data available
            </div>
        );
    }

    return <>{children}</>;
};

interface BrokerTopChartItem {
    name: string;
    messages: number;
    address: string;
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
        <div className="mt-4 min-w-0 w-full overflow-x-auto pb-1">
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
                                rx={10}
                                fill="rgba(148, 163, 184, 0.16)"
                            />
                            <rect
                                x={x}
                                y={y}
                                width={barWidth}
                                height={barHeight}
                                rx={10}
                                fill={isZero ? '#64748b' : index === 0 ? '#60a5fa' : '#3b82f6'}
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
                produceTps: Number(item.produceTps.toFixed(2)),
                consumeTps: Number(item.consumeTps.toFixed(2)),
            })),
        [brokerOverview?.brokerTps]
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
    const isTopicLoading = isLoading && topicQueueData.length === 0;

    const handleRefresh = async () => {
        await refresh();
    };

    return (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Card title="Broker Top 10" description="Received messages ranked by broker">
                <ChartState
                    isLoading={isBrokerLoading}
                    error={brokerError}
                    isEmpty={brokerTopData.length === 0}
                >
                    <BrokerTopChart data={brokerTopData} />
                </ChartState>
            </Card>

            <Card
                title="Broker TPS Snapshot"
                description="Current produce and consume TPS from broker runtime data"
                headerAction={
                    <Button variant="ghost" onClick={() => void handleRefresh()} disabled={isRefreshing}>
                        <RefreshCw className={`w-4 h-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
                        Refresh
                    </Button>
                }
            >
                <ChartState
                    isLoading={isBrokerLoading}
                    error={brokerError}
                    isEmpty={brokerTpsData.length === 0}
                >
                    <div className="mt-4 min-w-0 h-[300px] w-full">
                        <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
                            <LineChart data={brokerTpsData}>
                                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f0f0f0" />
                                <XAxis
                                    dataKey="name"
                                    tick={{ fontSize: 11, fill: '#9ca3af' }}
                                    tickFormatter={compactLabel}
                                    axisLine={false}
                                    tickLine={false}
                                />
                                <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
                                <Tooltip
                                    contentStyle={chartTooltipStyle}
                                    formatter={(value: number) => [formatNumber(value), 'TPS']}
                                />
                                <Line
                                    type="monotone"
                                    dataKey="produceTps"
                                    name="Produce TPS"
                                    stroke="#ef4444"
                                    strokeWidth={2}
                                    dot={false}
                                />
                                <Line
                                    type="monotone"
                                    dataKey="consumeTps"
                                    name="Consume TPS"
                                    stroke="#2563eb"
                                    strokeWidth={2}
                                    dot={false}
                                />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </ChartState>
            </Card>

            <Card title="Topic Queue Top 10" description="Largest topics by read and write queue count">
                <ChartState
                    isLoading={isTopicLoading}
                    error={topicError}
                    isEmpty={topicQueueData.length === 0}
                >
                    <div className="mt-4 min-w-0 h-[300px] w-full">
                        <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
                            <BarChart data={topicQueueData}>
                                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f0f0f0" />
                                <XAxis
                                    dataKey="name"
                                    tick={{ fontSize: 11, fill: '#9ca3af' }}
                                    tickFormatter={compactLabel}
                                    axisLine={false}
                                    tickLine={false}
                                />
                                <YAxis tick={{ fontSize: 11, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
                                <Tooltip
                                    cursor={{ fill: '#f9fafb' }}
                                    contentStyle={chartTooltipStyle}
                                    formatter={(value: number) => [formatNumber(value), 'Queues']}
                                />
                                <Bar dataKey="queues" fill="#6366f1" radius={[4, 4, 0, 0]} barSize={30} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </ChartState>
            </Card>

            <Card title="Topic Type Distribution" description="Current topic categories from the Topic catalog">
                <ChartState
                    isLoading={isTopicLoading}
                    error={topicError}
                    isEmpty={topicCategoryData.length === 0}
                >
                    <div className="mt-4 min-w-0 h-[300px] w-full">
                        <ResponsiveContainer width="100%" height="100%" minWidth={0} debounce={200}>
                            <BarChart data={topicCategoryData}>
                                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f0f0f0" />
                                <XAxis
                                    dataKey="name"
                                    tick={{ fontSize: 11, fill: '#9ca3af' }}
                                    axisLine={false}
                                    tickLine={false}
                                />
                                <YAxis allowDecimals={false} tick={{ fontSize: 11, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
                                <Tooltip
                                    cursor={{ fill: '#f9fafb' }}
                                    contentStyle={chartTooltipStyle}
                                    formatter={(value: number) => [formatNumber(value), 'Topics']}
                                />
                                <Bar dataKey="count" fill="#0f766e" radius={[4, 4, 0, 0]} barSize={30} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </ChartState>
            </Card>
        </div>
    );
};
