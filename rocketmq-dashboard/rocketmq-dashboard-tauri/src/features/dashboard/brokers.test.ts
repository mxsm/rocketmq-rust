import { expect, it } from 'vitest';
import type { ClusterBrokerCardItem, ClusterHomePageResponse } from '../cluster/types/cluster.types';
import { brokerMetric, brokerState, throughputMetric } from './brokers';

const broker = (extra: Partial<ClusterBrokerCardItem> = {}): ClusterBrokerCardItem => ({
    clusterName: 'cluster', brokerName: 'broker', brokerId: 0, role: 'MASTER', address: '127.0.0.1:10911',
    version: '', produceTps: 3, consumeTps: 2, todayReceivedTotal: 50, todayProduce: 20,
    yesterdayProduce: 10, yesterdayConsume: 0, todayConsume: 0, isActive: true, statusLoadError: null,
    rawStatus: { brokerActive: 'true', putTps: '3 2 1', getTransferedTps: '2 1 0', msgGetTotalTodayNow: '50',
        msgPutTotalTodayNow: '100', msgPutTotalTodayMorning: '80', msgPutTotalYesterdayMorning: '70' },
    ...extra,
});
const catalog = (items: ClusterBrokerCardItem[]): ClusterHomePageResponse => ({
    clusters: ['cluster'], items, currentNamesrv: '127.0.0.1:9876', useVipChannel: false, useTls: false,
    summary: { totalClusters: 1, totalBrokers: items.length, activeBrokers: 2, inactiveBrokers: 0, totalMasters: 2, totalSlaves: 0, brokersWithStatusErrors: 0 },
});

it('does not infer activity from discovery or a default boolean', () => {
    expect(brokerState(broker())).toBe('active');
    expect(brokerState(broker({ rawStatus: { brokerActive: 'false' } }))).toBe('inactive');
    expect(brokerState(broker({ rawStatus: {} }))).toBe('unknown');
    expect(brokerState(broker({ statusLoadError: 'Unavailable' }))).toBe('unavailable');
});

it('suppresses zero-defaulted missing fields and failed Broker readings', () => {
    expect(brokerMetric(broker({ rawStatus: {}, produceTps: 0 }), 'produceTps')).toBeNull();
    expect(brokerMetric(broker({ statusLoadError: 'Unavailable' }), 'todayReceivedTotal')).toBeNull();
    expect(brokerMetric(broker({ rawStatus: { putTps: 'NaN' } }), 'produceTps')).toBeNull();
    expect(brokerMetric(broker({ rawStatus: { putTps: '0x10' }, produceTps: 0 }), 'produceTps')).toBeNull();
    expect(brokerMetric(broker({ rawStatus: { putTps: '0 0 0' }, produceTps: 0 }), 'produceTps')).toBe(0);
    expect(brokerMetric(broker({ rawStatus: { msgPutTotalTodayNow: '100' } }), 'todayProduce')).toBeNull();
});

it('honors the alternate consume TPS spelling and requires both daily counter endpoints', () => {
    expect(brokerMetric(broker({ rawStatus: { getTransferredTps: '2 1 0' } }), 'consumeTps')).toBe(2);
    expect(brokerMetric(broker(), 'todayProduce')).toBe(20);
    expect(brokerMetric(broker(), 'yesterdayProduce')).toBe(10);
});

it('qualifies throughput with missing sources instead of presenting unknown as zero', () => {
    expect(throughputMetric(null)).toEqual({ value: null, quality: 'unknown' });
    expect(throughputMetric(catalog([]))).toEqual({ value: 0, quality: 'complete' });
    expect(throughputMetric(catalog([broker(), broker()]))).toEqual({ value: 10, quality: 'complete' });
    expect(throughputMetric(catalog([broker(), broker({ statusLoadError: 'Unavailable' })]))).toEqual({ value: 5, quality: 'partial' });
    expect(throughputMetric(catalog([broker({ rawStatus: {} })]))).toEqual({ value: null, quality: 'unknown' });
});
