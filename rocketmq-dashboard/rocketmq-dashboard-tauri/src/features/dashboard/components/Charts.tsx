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
    LabelList,
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
    borderRadius: '10px',
    border: '1px solid rgba(71, 85, 105, 0.8)',
    backgroundColor: '#0f172a',
    color: '#e2e8f0',
    boxShadow: '0 16px 32px rgba(0,0,0,0.28)',
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

const MetricBar = ({
    label,
    value,
    maxValue,
    accentColor,
    trackColor,
}: {
    label: string;
    value: number;
    maxValue: number;
    accentColor: string;
    trackColor: string;
}) => {
    const width = `${Math.max((value / Math.max(maxValue, 1)) * 100, value > 0 ? 3 : 0)}%`;

    return (
        <div>
            <div className="mb-1.5 flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 text-xs font-semibold text-slate-200">
                    <span className="h-2.5 w-2.5 rounded-sm" style={{ backgroundColor: accentColor }} />
                    {label}
                </div>
                <div className="font-mono text-sm font-semibold text-white">
                    {formatNumber(value)}
                </div>
            </div>
            <div className="h-3 overflow-hidden rounded-full" style={{ backgroundColor: trackColor }}>
                <div className="h-full rounded-full" style={{ width, backgroundColor: accentColor }} />
            </div>
        </div>
    );
};

const BrokerTpsCompactCards = ({ data }: { data: BrokerTpsChartItem[] }) => {
    const maxTps = Math.max(...data.flatMap((item) => [item.produceTps, item.consumeTps]), 1);

    return (
        <div className="mt-4 flex flex-wrap items-start gap-3">
            {data.map((item) => {
                const totalTps = item.produceTps + item.consumeTps;

                return (
                    <div
                        key={`${item.name}-${item.address}`}
                        className="w-full max-w-[440px] rounded-xl border border-slate-700/80 bg-slate-950/30 p-4"
                    >
                        <div className="mb-4 flex min-w-0 items-start justify-between gap-3">
                            <div className="min-w-0">
                                <div className="truncate text-sm font-semibold text-white">
                                    {item.name}
                                </div>
                                <div className="mt-1 truncate font-mono text-xs text-slate-400">
                                    {item.address}
                                </div>
                            </div>
                            <div className="shrink-0 rounded-lg border border-teal-400/25 bg-teal-950/30 px-3 py-1.5 text-right">
                                <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-teal-300">
                                    Total
                                </div>
                                <div className="font-mono text-sm font-semibold text-white">
                                    {formatNumber(totalTps)}
                                </div>
                            </div>
                        </div>

                        <div className="space-y-3">
                            <MetricBar
                                label="Producer TPS"
                                value={item.produceTps}
                                maxValue={maxTps}
                                accentColor="#fb7185"
                                trackColor="rgba(251, 113, 133, 0.18)"
                            />
                            <MetricBar
                                label="Consumer TPS"
                                value={item.consumeTps}
                                maxValue={maxTps}
                                accentColor="#3b82f6"
                                trackColor="rgba(59, 130, 246, 0.18)"
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
        <div className="mt-4">
            <div className="mb-3 flex items-center justify-between gap-3 text-xs">
                <div className="flex items-center gap-2 font-semibold text-slate-300">
                    <span className="h-2.5 w-2.5 rounded-sm bg-indigo-400" />
                    TotalMsg
                </div>
                <div className="text-slate-500">
                    24h consumed messages
                </div>
            </div>

            <div className="min-w-0 w-full" style={{ height: `${chartHeight}px` }}>
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
                        <Bar dataKey="totalMsg" name="TotalMsg" fill="#818cf8" radius={[0, 6, 6, 0]} barSize={18}>
                            <LabelList
                                dataKey="totalMsg"
                                position="right"
                                formatter={(value: number) => formatNumber(value)}
                                fill="#e0e7ff"
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
                    <BrokerTpsCompactCards data={brokerTpsData} />
                </ChartState>
            </Card>

            <Card title="Topic TOP 10" description="TotalMsg ranked by topic">
                <ChartState
                    isLoading={isTopicLoading}
                    error={topicError}
                    isEmpty={topicTopData.length === 0}
                >
                    <TopicTopBarChart data={topicTopData} />
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
                                <Bar dataKey="count" fill="#2dd4bf" radius={[4, 4, 0, 0]} barSize={30} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                </ChartState>
            </Card>
        </div>
    );
};
