import { expect, it } from 'vitest';
import { brokerIdentity, brokerKey, brokerRate, brokerState, brokerTps } from './brokerIdentity';
import type { ClusterBrokerCardItem } from './types/cluster.types';

const broker: ClusterBrokerCardItem = { clusterName: 'cluster-a', brokerName: 'broker-a', brokerId: 0, role: 'MASTER',
    address: 'a:10911', version: '', produceTps: 5, consumeTps: 3, todayReceivedTotal: 0, yesterdayProduce: 0,
    yesterdayConsume: 0, todayProduce: 0, todayConsume: 0, isActive: true,
    rawStatus: { brokerActive: 'true', putTps: '5 1min', getTransferedTps: '3 1min' } };

it('binds selection to the full identity even when a replacement reuses the address', () => {
    const key = brokerKey(broker);
    expect(brokerIdentity(broker)).toEqual({ clusterName: 'cluster-a', brokerName: 'broker-a', brokerId: 0, address: 'a:10911' });
    for (const replacement of [{ ...broker, brokerName: 'replacement' }, { ...broker, brokerId: 1 }, { ...broker, clusterName: 'other' }]) {
        expect(brokerKey(replacement)).not.toBe(key);
    }
});

it('does not equate discovery or an absent active flag with runtime health', () => {
    expect(brokerState(broker)).toBe('Active');
    expect(brokerState({ ...broker, rawStatus: { brokerActive: 'false' } })).toBe('Inactive');
    expect(brokerState({ ...broker, rawStatus: {} })).toBe('Unknown');
    expect(brokerState({ ...broker, statusLoadError: 'Unavailable' })).toBe('Unavailable');
});

it('requires actual finite rate observations and accepts the existing transfer-key fallback', () => {
    expect(brokerTps(broker)).toBe(8);
    expect(brokerRate({ ...broker, rawStatus: { getTransferredTps: '3 1min' } }, 'consume')).toBe(3);
    for (const rawStatus of [{}, { putTps: '5' }, { putTps: 'invalid', getTransferedTps: '3' }, { putTps: 'NaN', getTransferedTps: '3' }, { putTps: '0x10', getTransferedTps: '3' }]) {
        expect(brokerTps({ ...broker, rawStatus })).toBeNull();
    }
    expect(brokerTps({ ...broker, statusLoadError: 'Unavailable' })).toBeNull();
    expect(brokerTps({ ...broker, produceTps: Infinity })).toBeNull();
});
