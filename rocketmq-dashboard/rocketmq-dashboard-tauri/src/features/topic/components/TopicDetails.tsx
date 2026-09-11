import { useCallback, useState, type ReactNode } from 'react';
import { useAppStore } from '../../../stores/app.store';
import { TopicService } from '../../../services/topic.service';
import { useReadResource } from '../../../hooks/useReadResource';
import { PageState } from '../../../components/layout/PageState';
import { PageSection } from '../../../components/layout/PageSection';
import { StatusBadge } from '../../../components/layout/StatusBadge';
import { Button } from '../../../components/ui/LegacyButton';
import type { TopicListItem, TopicRouteView, TopicStatusView, TopicConsumerInfoResponse, TopicConfigView } from '../types/topic.types';
import type { TopicAction } from '../topicActionContext';
import { topicPermission, isProtectedTopic } from '../topicModel';
import { TopicSnapshotCopy } from './TopicSnapshotCopy';

export type TopicDetailTab = 'overview' | 'route' | 'status' | 'consumers' | 'config';
export type TopicDetailData = { kind: 'route'; value: TopicRouteView } | { kind: 'status'; value: TopicStatusView }
    | { kind: 'consumers'; value: TopicConsumerInfoResponse } | { kind: 'config'; value: TopicConfigView };

export function useTopicDetail(topic: string | null, tab: TopicDetailTab, brokerName: string) {
    const load = useCallback(async (): Promise<TopicDetailData> => {
        if (tab === 'config') {
            const value = await TopicService.getTopicConfig({ topic: topic!, brokerName: brokerName || null });
            if (value.topicName !== topic || (brokerName && value.brokerName !== brokerName)) throw new Error('Unexpected Topic or Broker response');
            return { kind: 'config', value };
        }
        const result: TopicDetailData = tab === 'status' ? { kind: 'status', value: await TopicService.getTopicStats({ topic: topic! }) }
            : tab === 'consumers' ? { kind: 'consumers', value: await TopicService.getTopicConsumers({ topic: topic! }) }
                : { kind: 'route', value: await TopicService.getTopicRoute({ topic: topic! }) };
        if (result.value.topic !== topic) throw new Error('Unexpected Topic response');
        return result;
    }, [topic, tab, brokerName]);
    return useReadResource(topic ? load : null, 'Topic details could not be read.');
}

export function TopicProperties({ rows }: { rows: [string, ReactNode][] }) {
    return <dl className="ops-topic-properties">{rows.map(([label, value]) => <div key={label}><dt>{label}</dt>
        <dd tabIndex={typeof value === 'string' && value.length > 200 ? 0 : undefined}>{value}</dd></div>)}</dl>;
}
export function TopicTable({ label, headings, children }: { label: string; headings: string[]; children: ReactNode }) {
    return <div className="ops-topic-table-scroll" role="region" aria-label={label} tabIndex={0}><table><thead><tr>
        {headings.map(heading => <th scope="col" key={heading}>{heading}</th>)}</tr></thead><tbody>{children}</tbody></table></div>;
}
const timestamp = (value: number) => value > 0 && Number.isFinite(value) && !Number.isNaN(new Date(value).getTime()) ? new Date(value).toLocaleString() : 'Not reported';

export function TopicDetails({ topic, tab, data, onAction, onShowConsumers, allowMutation }: {
    topic: TopicListItem; tab: TopicDetailTab; data: TopicDetailData | null; onAction: (action: TopicAction) => void;
    onShowConsumers: () => void; allowMutation: boolean;
}) {
    const { openBroker, openConsumer } = useAppStore();
    const [queueQuery, setQueueQuery] = useState('');
    return <div className="ops-topic-detail-panels">
        {tab === 'overview' && <PageSection title="Basic information">
            <TopicProperties rows={[
                ['Topic name', topic.topic], ['Type', <StatusBadge tone={isProtectedTopic(topic) ? 'neutral' : 'success'}>{topic.category}</StatusBadge>],
                ...(topic.messageType !== topic.category ? [['Message type', topic.messageType || 'Unknown'] as [string, ReactNode]] : []),
                ['Cluster', topic.clusters.join(', ') || 'No cluster route'],
                ['Brokers', topic.brokers.join(', ') || 'No Broker route'], ['Read queues', topic.readQueueCount],
                ['Write queues', topic.writeQueueCount], ['Permission', topicPermission(topic.perm)], ['Ordered', topic.order ? 'On' : 'Off'],
            ]} />
        </PageSection>}
        {data?.kind === 'route' && <PageSection title={tab === 'overview' ? 'Broker queue distribution' : 'Routes'}
            action={<Button variant="ghost" onClick={onShowConsumers}>View consumers</Button>}>
            <TopicTable label="Topic Broker routes" headings={['Broker / Cluster', 'Addresses', 'Read queues', 'Write queues', 'Permission']}>
                {data.value.queues.map((queue, index) => {
                    const broker = data.value.brokers.find(item => item.brokerName === queue.brokerName);
                    return <tr key={queue.brokerName + ':' + index}><th scope="row">{queue.brokerName}<small>{broker?.clusterName || 'Not reported'}</small></th>
                        <td>{broker?.addresses.length ? broker.addresses.map(address => <button key={address.brokerId + ':' + address.address} type="button"
                            className="ops-topic-link" onClick={() => openBroker(address.address, 'status')}>{address.brokerId}: {address.address}</button>) : 'Not reported'}</td>
                        <td>{queue.readQueueNums}</td><td>{queue.writeQueueNums}</td><td>{topicPermission(queue.perm)}</td></tr>;
                })}
                {data.value.brokers.filter(broker => !data.value.queues.some(queue => queue.brokerName === broker.brokerName)).map(broker =>
                    <tr key={broker.clusterName + ':' + broker.brokerName}><th scope="row">{broker.brokerName}<small>{broker.clusterName}</small></th>
                        <td>{broker.addresses.map(address => <button key={address.brokerId + ':' + address.address} type="button" className="ops-topic-link"
                            onClick={() => openBroker(address.address, 'status')}>{address.brokerId}: {address.address}</button>)}</td>
                        <td colSpan={3}>No queue data returned</td></tr>)}
            </TopicTable>
            {!data.value.queues.length && <PageState kind="empty" title="No queue routes returned" />}
            {tab === 'route' && <><TopicProperties rows={[
                ['Broker addresses', data.value.brokers.reduce((sum, broker) => sum + broker.addresses.length, 0)],
                ['Total read / write queues', data.value.queues.reduce((sum, queue) => sum + queue.readQueueNums, 0) + ' / ' + data.value.queues.reduce((sum, queue) => sum + queue.writeQueueNums, 0)],
            ]} /><TopicSnapshotCopy label="Copy route" value={data.value} />
                <details><summary>Full route snapshot</summary><pre className="ops-topic-code" tabIndex={0}>{JSON.stringify(data.value, null, 2)}</pre></details></>}
        </PageSection>}
        {data?.kind === 'status' && <PageSection title="Statistics">
            <TopicProperties rows={ [['Total message count', data.value.totalMessageCount], ['Queues', data.value.queueCount],
                ['Queues with recorded activity', data.value.offsets.filter(row => row.lastUpdateTimestamp > 0 || row.maxOffset > row.minOffset).length],
                ['Sum of minimum offsets', data.value.offsets.reduce((sum, row) => sum + row.minOffset, 0)],
                ['Sum of maximum offsets', data.value.offsets.reduce((sum, row) => sum + row.maxOffset, 0)]] } />
            <label className="ops-topic-select"><span>Filter Broker or queue</span><input value={queueQuery} onChange={event => setQueueQuery(event.target.value)} /></label>
            <TopicTable label="Topic queue offsets" headings={['Broker', 'Queue', 'Min / max', 'Range', 'Last update', 'Snapshot']}>
                {data.value.offsets.filter(row => (row.brokerName + ' ' + row.queueId).toLowerCase().includes(queueQuery.trim().toLowerCase())).map(row =>
                    <tr key={row.brokerName + ':' + row.queueId}><th scope="row">{row.brokerName}</th><td>{row.queueId}</td>
                        <td>{row.minOffset} / {row.maxOffset}</td><td>{Math.max(row.maxOffset - row.minOffset, 0)}</td><td>{timestamp(row.lastUpdateTimestamp)}</td>
                        <td><TopicSnapshotCopy compact label={'Copy ' + row.brokerName + ' queue ' + row.queueId} value={{ topic: topic.topic, ...row }} /></td></tr>)}
            </TopicTable>
            {!data.value.offsets.some(row => (row.brokerName + ' ' + row.queueId).toLowerCase().includes(queueQuery.trim().toLowerCase())) &&
                <PageState kind="empty" title={queueQuery ? 'No queues match this filter' : 'No queue statistics returned'} />}
        </PageSection>}
        {data?.kind === 'consumers' && <PageSection title="Consumers">
            <TopicTable label="Topic Consumers" headings={['Consumer group', 'Observed lag', 'Inflight', 'Consume TPS', 'Details']}>
                {data.value.items.map(item => <tr key={item.consumerGroup}><th scope="row">{item.consumerGroup}</th>
                    <td>{item.totalDiff}</td><td>{item.inflightDiff}</td><td>{Number.isFinite(item.consumeTps) ? item.consumeTps.toFixed(2) : 'Unknown'}</td>
                    <td><Button variant="ghost" onClick={() => openConsumer(item.consumerGroup, 'progress', { mode: 'name_server' })}>Inspect</Button></td></tr>)}
            </TopicTable>
            {!data.value.items.length && <PageState kind="empty" title="No Consumer groups returned for this Topic" />}
        </PageSection>}
        {data?.kind === 'config' && <PageSection title="Configuration" action={!isProtectedTopic(topic) && data.value.brokerName
            ? <Button variant="danger" disabled={!allowMutation} onClick={() => onAction({ kind: 'delete_broker', topic, brokerName: data.value.brokerName })}>Delete from this Broker</Button> : undefined}>
            <p className="ops-topic-note">Values returned by {data.value.brokerName || 'an unspecified Broker'}. A single response is not proof that every Broker has the same configuration.</p>
            {data.value.inconsistentFields.length > 0 && <PageState kind="partial" title="Configuration differs across Brokers" description={data.value.inconsistentFields.join(', ')} />}
            <TopicProperties rows={ [['Topic', data.value.topicName], ['Broker', data.value.brokerName], ['Cluster', data.value.clusterName || 'Not reported'],
                ['Read queues', data.value.readQueueNums], ['Write queues', data.value.writeQueueNums], ['Permission', topicPermission(data.value.perm)],
                ['Message type', data.value.messageType || 'Unknown'], ['Ordered', data.value.order ? 'On' : 'Off']] } />
            <h4>Attributes</h4><TopicProperties rows={Object.entries(data.value.attributes)} />
            {!Object.keys(data.value.attributes).length && <p className="ops-topic-note">No attributes returned.</p>}
            <TopicSnapshotCopy label="Copy configuration" value={data.value} />
            <details><summary>Full configuration snapshot</summary><pre className="ops-topic-code" tabIndex={0}>{JSON.stringify(data.value, null, 2)}</pre></details>
        </PageSection>}
    </div>;
}
