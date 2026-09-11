import type { ClusterBrokerCardItem, ClusterHomePageResponse } from '../cluster/types/cluster.types';
import type { OverviewMetric } from './types/dashboard.types';

export type BrokerState = 'active' | 'inactive' | 'unknown' | 'unavailable';

export function brokerState(broker: ClusterBrokerCardItem): BrokerState {
    if (broker.statusLoadError != null) return 'unavailable';
    const active = broker.rawStatus.brokerActive?.toLowerCase();
    return active === 'true' ? 'active' : active === 'false' ? 'inactive' : 'unknown';
}

type BrokerMetric = 'produceTps' | 'consumeTps' | 'todayReceivedTotal' | 'todayProduce' | 'yesterdayProduce';

export function brokerMetric(broker: ClusterBrokerCardItem, metric: BrokerMetric): number | null {
    if (broker.statusLoadError != null) return null;
    const sourceKeys: Record<BrokerMetric, string[]> = {
        produceTps: ['putTps'],
        consumeTps: [broker.rawStatus.getTransferedTps?.trim() ? 'getTransferedTps' : 'getTransferredTps'],
        todayReceivedTotal: ['msgGetTotalTodayNow'],
        todayProduce: ['msgPutTotalTodayMorning', 'msgPutTotalTodayNow'],
        yesterdayProduce: ['msgPutTotalYesterdayMorning', 'msgPutTotalTodayMorning'],
    };
    // The existing DTO defaults missing/malformed fields to zero; retain their absence in the UI.
    const valid = sourceKeys[metric].every(key => {
        const raw = broker.rawStatus[key];
        if (!raw?.trim()) return false;
        const token = metric.endsWith('Tps') ? raw.trim().split(/\s+/)[0] : raw;
        const numeric = metric.endsWith('Tps') ? /^\+?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/ : /^\+?\d+$/;
        return numeric.test(token) && Number.isFinite(Number(token)) && Number(token) >= 0;
    });
    const value = broker[metric];
    return valid && Number.isFinite(value) && value >= 0 ? value : null;
}

export function throughputMetric(data: ClusterHomePageResponse | null): OverviewMetric {
    if (!data) return { value: null, quality: 'unknown' };
    if (data.items.length === 0) return { value: 0, quality: 'complete' };
    const values = data.items.map(broker => {
        const produce = brokerMetric(broker, 'produceTps');
        const consume = brokerMetric(broker, 'consumeTps');
        return produce === null || consume === null ? null : produce + consume;
    });
    const known = values.filter((value): value is number => value !== null);
    return {
        value: known.length ? known.reduce((sum, value) => sum + value, 0) : null,
        quality: !known.length ? 'unknown' : known.length === values.length ? 'complete' : 'partial',
    };
}

export const formatMetric = (value: number | null, decimals = 0) => value === null
    ? 'Unknown'
    : value.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });

export const qualityDescription = (metric: OverviewMetric) => {
    if (metric.value === null || metric.quality === 'unknown') return 'Unavailable in this observation';
    if (metric.quality === 'partial') return 'Partial observation; some targets are missing';
    if (metric.quality === 'reported') return 'Reported; coverage unknown';
    return 'Complete observation';
};
