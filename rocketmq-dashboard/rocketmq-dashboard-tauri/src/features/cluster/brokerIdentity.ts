import type { ClusterBrokerCardItem } from './types/cluster.types';

export type BrokerIdentity = Pick<ClusterBrokerCardItem, 'clusterName' | 'brokerName' | 'brokerId' | 'address'>;

export const brokerIdentity = ({ clusterName, brokerName, brokerId, address }: BrokerIdentity): BrokerIdentity =>
    ({ clusterName, brokerName, brokerId, address });

export const brokerKey = (broker: BrokerIdentity): string =>
    JSON.stringify([broker.clusterName, broker.brokerName, broker.brokerId, broker.address]);

export function brokerState(broker: ClusterBrokerCardItem): 'Active' | 'Inactive' | 'Unavailable' | 'Unknown' {
    if (broker.statusLoadError) return 'Unavailable';
    const active = broker.rawStatus.brokerActive?.toLowerCase();
    if (active === 'true') return 'Active';
    if (active === 'false') return 'Inactive';
    return 'Unknown';
}

export function brokerRate(broker: ClusterBrokerCardItem, direction: 'produce' | 'consume'): number | null {
    const raw = direction === 'produce' ? broker.rawStatus.putTps
        : broker.rawStatus.getTransferedTps?.trim() ? broker.rawStatus.getTransferedTps : broker.rawStatus.getTransferredTps;
    const value = direction === 'produce' ? broker.produceTps : broker.consumeTps;
    const token = raw?.trim().split(/\s+/)[0] ?? '';
    const decimal = /^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/.test(token);
    return !broker.statusLoadError && decimal && Number.isFinite(Number(token)) && Number.isFinite(value) ? value : null;
}

export function brokerTps(broker: ClusterBrokerCardItem): number | null {
    const produce = brokerRate(broker, 'produce');
    const consume = brokerRate(broker, 'consume');
    return produce !== null && consume !== null && Number.isFinite(produce + consume) ? produce + consume : null;
}
