import { useCallback, useState, type ReactNode } from 'react';
import { useReadResource } from '../../../hooks/useReadResource';
import { ConsumerService } from '../../../services/consumer.service';
import { useAppStore } from '../../../stores/app.store';
import { PageSection } from '../../../components/layout/PageSection';
import { PageState } from '../../../components/layout/PageState';
import { StatusBadge } from '../../../components/layout/StatusBadge';
import { Button } from '../../../components/ui/LegacyButton';
import { Input } from '../../../components/ui/LegacyInput';
import { checkConsumerIdentity, consumerCount, consumerNumberFields, consumerSwitches, consumerTimestamp, topicLagByBroker } from '../consumerModel';
import { consumerScopeKey, consumerScopeLabel } from '../scope';
import type { ConsumerConfigSummary, ConsumerConnectionView, ConsumerGroupListItem, ConsumerQueryScope, ConsumerTopicDetailView } from '../types/consumer.types';
import { ConsumerDiagnostics } from './ConsumerDiagnostics';

export type ConsumerDetailTab = 'overview' | 'progress' | 'clients' | 'config' | 'reset';
export type ConsumerDetailData = { kind: 'progress'; value: ConsumerTopicDetailView }
    | { kind: 'clients'; value: ConsumerConnectionView } | { kind: 'config'; value: ConsumerConfigSummary };
export function useConsumerDetails(group: string | null, scope: ConsumerQueryScope, tab: ConsumerDetailTab) {
    const scopeKey = consumerScopeKey(scope);
    const load = useCallback(async (): Promise<ConsumerDetailData> => {
        if (tab === 'config') return { kind: 'config', value: checkConsumerIdentity(await ConsumerService.queryConsumerConfigSummary(group!), group!) };
        if (tab === 'clients') return { kind: 'clients', value: checkConsumerIdentity(await ConsumerService.queryConsumerConnection({ consumerGroup: group!, scope }), group!) };
        return { kind: 'progress', value: checkConsumerIdentity(await ConsumerService.queryConsumerTopicDetail({ consumerGroup: group!, scope }), group!) };
    }, [group, scopeKey, tab]);
    return useReadResource(group ? load : null, 'Consumer details could not be read.');
}
export function ConsumerProperties({ rows }: { rows: [string, ReactNode][] }) {
    return <dl className="ops-consumer-properties">{rows.map(([label, value]) => <div key={label}><dt>{label}</dt>
        <dd tabIndex={typeof value === 'string' && value.length > 200 ? 0 : undefined}>{value ?? 'Not reported'}</dd></div>)}</dl>;
}
export function ConsumerJson({ label, value, open = false }: { label: string; value: unknown; open?: boolean }) {
    return <details open={open}><summary>{label}</summary><pre className="ops-consumer-code" tabIndex={0}>{typeof value === 'string' ? value : JSON.stringify(value, null, 2)}</pre></details>;
}
export function ConsumerDetails({ consumer, scope, tab, data, blocked, mutationBlocked, onTab, onEdit, onReset }: {
    consumer: ConsumerGroupListItem; scope: ConsumerQueryScope; tab: ConsumerDetailTab; data: ConsumerDetailData | null; blocked: boolean; mutationBlocked: boolean;
    onTab: (tab: ConsumerDetailTab) => void; onEdit: (address: string) => void; onReset: (topic: string) => void;
}) {
    if (tab === 'config') return data?.kind === 'config' ? <ConsumerConfiguration data={data.value} blocked={blocked || mutationBlocked} onEdit={onEdit} /> : null;
    if (tab === 'clients') return data?.kind === 'clients' ? <ConsumerConnections data={data.value} scope={scope} blocked={blocked} /> : null;
    const progress = data?.kind === 'progress' ? data.value : null;
    if (tab === 'reset') return <div className="ops-consumer-stack"><p>Choose a Topic, then review the exact group, timestamp and force option in the offset dialog. Offset writes use NameServer Broker discovery.</p>
        {scope.mode === 'proxy' && <PageState kind="partial" title="Offset changes require NameServer mode" description="Switch query scope to NameServer and review current progress before changing offsets." />}
        {progress && <ConsumerProgress data={progress} onReset={onReset} allowReset={!blocked && !mutationBlocked && scope.mode === 'name_server'} />}</div>;
    if (tab === 'progress') return progress ? <ConsumerProgress data={progress} /> : null;
    return <div className="ops-consumer-overview">
        <PageSection title="Topic lag (by Broker)">{progress ? <ConsumerLagTable data={progress} /> : <p className="ops-consumer-note">Progress is not available for this observation.</p>}</PageSection>
        <div className="ops-consumer-stack"><PageSection title="Group details"><ConsumerProperties rows={[
            ['Consumer group', consumer.rawGroupName], ['Reported clients', consumerCount(consumer.connectionCount)],
            ['Topics in progress', progress ? consumerCount(progress.topicCount) : 'Unknown'], ['Observed lag', progress ? consumerCount(progress.totalDiff) : 'Unknown'],
            ['Catalog updated', consumerTimestamp(consumer.updateTimestamp)],
        ]} /><details className="ops-consumer-runtime"><summary>Runtime metadata</summary><ConsumerProperties rows={[
            ['Catalog consume TPS', consumer.consumeTps > 0 ? consumerCount(consumer.consumeTps) : 'Not verified'], ['Message model', consumer.messageModel || 'Unknown'],
            ['Consume type', consumer.consumeType || 'Unknown'], ['Version', consumer.version != null ? consumer.versionDesc : 'Not reported'],
        ]} /></details></PageSection><PageSection className="ops-consumer-config-summary" title="Configuration summary" description="Compare the configuration returned by each discovered Broker."
            action={<Button variant="outline" onClick={() => onTab('config')}>Compare Broker configuration</Button>} /></div>
    </div>;
}
function ConsumerLagTable({ data }: { data: ConsumerTopicDetailView }) {
    const { openTopic } = useAppStore();
    const brokers = [...new Set(data.topics.flatMap(topic => topic.queueStatInfoList.map(queue => queue.brokerName)))].sort();
    return <>{!data.topics.length ? <PageState kind="empty" title="No Topic progress returned" /> : <div className="ops-consumer-scroll" role="region" aria-label="Topic lag by Broker" tabIndex={0}><table>
        <thead><tr><th scope="col">Topic</th>{brokers.map(name => <th scope="col" key={name}>{name}</th>)}<th scope="col">Total lag</th></tr></thead>
        <tbody>{data.topics.map(topic => { const values = topicLagByBroker(topic); return <tr key={topic.topic}>
            <th scope="row"><button type="button" className="ops-consumer-link" onClick={() => openTopic(topic.topic, 'status')}>{topic.topic}</button></th>
            {brokers.map(name => <td key={name}>{values.has(name) ? consumerCount(values.get(name)!) : 'Not reported'}</td>)}<td>{consumerCount(topic.diffTotal)}</td>
        </tr>; })}</tbody></table></div>}<p className="ops-consumer-note">Lag reflects returned queue observations. Missing Broker rows are not counted as zero.</p></>;
}
function ConsumerProgress({ data, onReset, allowReset = false }: { data: ConsumerTopicDetailView; onReset?: (topic: string) => void; allowReset?: boolean }) {
    const [selection, setSelection] = useState<string | null>(null);
    const selected = selection === null ? data.topics[0] : data.topics.find(topic => topic.topic === selection);
    const { openTopic } = useAppStore();
    if (!data.topics.length) return <PageState kind="empty" title="No Topic progress returned" />;
    return <div className="ops-consumer-stack"><label className="ops-consumer-select"><span>Progress Topic</span><select value={selected?.topic ?? selection ?? ''} onChange={event => setSelection(event.target.value)}>
        {selection && !selected && <option value={selection}>{selection} · unavailable</option>}{data.topics.map(topic => <option key={topic.topic}>{topic.topic}</option>)}</select></label>
        {selected ? <><div className="ops-consumer-actions"><Button variant="outline" onClick={() => openTopic(selected.topic, 'status')}>Open Topic statistics</Button>
            {onReset && <Button variant="danger" disabled={!allowReset} onClick={() => onReset(selected.topic)}>Review offset reset</Button>}</div>
            <ConsumerProperties rows={ [['Topic', selected.topic], ['Observed lag', consumerCount(selected.diffTotal)], ['Last message timestamp', consumerTimestamp(selected.lastTimestamp)], ['Reported queues', selected.queueStatInfoList.length]] } />
            <div className="ops-consumer-scroll" role="region" aria-label="Consumer queue progress" tabIndex={0}><table><thead><tr>{['Broker', 'Queue', 'Broker offset', 'Consumer offset', 'Lag', 'Client', 'Last message'].map(label => <th scope="col" key={label}>{label}</th>)}</tr></thead>
                <tbody>{selected.queueStatInfoList.map((queue, index) => <tr key={queue.brokerName + ':' + queue.queueId + ':' + index}>
                    <th scope="row">{queue.brokerName}</th><td>{queue.queueId}</td><td>{queue.brokerOffset}</td><td>{queue.consumerOffset}</td><td>{consumerCount(queue.diffTotal)}</td><td>{queue.clientInfo || 'Not reported'}</td><td>{consumerTimestamp(queue.lastTimestamp)}</td>
                </tr>)}</tbody></table></div></> : <PageState kind="empty" title="Selected Topic is no longer in this progress result" description="Choose another Topic explicitly." />}</div>;
}
function ConsumerConnections({ data, scope, blocked }: { data: ConsumerConnectionView; scope: ConsumerQueryScope; blocked: boolean }) {
    const [client, setClient] = useState('');
    const selected = data.connections.find(item => item.clientId === client);
    return <div className="ops-consumer-stack"><ConsumerProperties rows={ [['Query source', consumerScopeLabel(scope)], ['Reported connections', data.connectionCount],
        ['Message model', data.messageModel], ['Consume type', data.consumeType], ['Consume from', data.consumeFromWhere], ['Subscriptions', data.subscriptions.length]] } />
        {scope.mode === 'proxy' && <PageState kind="partial" title="Client diagnostics are unavailable in Proxy mode" description="Switch to NameServer mode to request RunningInfo or a thread stack." />}
        <div className="ops-consumer-scroll" role="region" aria-label="Consumer connections" tabIndex={0}><table><thead><tr>{['Client ID', 'Address', 'Language', 'Version', 'Diagnostics'].map(label => <th scope="col" key={label}>{label}</th>)}</tr></thead>
            <tbody>{data.connections.map(item => <tr key={item.clientId} data-selected={client === item.clientId}><th scope="row">{item.clientId}</th><td>{item.clientAddr}</td><td>{item.language}</td><td>{item.versionDesc} ({item.version})</td>
                <td><Button variant="outline" disabled={blocked || scope.mode === 'proxy'} onClick={() => setClient(item.clientId)}>Inspect client</Button></td></tr>)}</tbody></table></div>
        {!data.connections.length && <PageState kind="empty" title="No connected clients returned" description="The group may exist without a currently connected client." />}
        {client && !selected && <PageState kind="stale" title="Selected client is no longer in the connection result" />}
        {selected && <ConsumerDiagnostics key={data.consumerGroup + ':' + consumerScopeKey(scope) + ':' + client} request={{ consumerGroup: data.consumerGroup, clientId: client, scope }} disabled={blocked} />}
        <ConsumerJson label={'Subscriptions (' + data.subscriptions.length + ')'} value={data.subscriptions} />
    </div>;
}
function ConsumerConfiguration({ data, blocked, onEdit }: { data: ConsumerConfigSummary; blocked: boolean; onEdit: (address: string) => void }) {
    const [address, setAddress] = useState('');
    const [search, setSearch] = useState('');
    const selected = data.targets.find(item => item.brokerAddress === address);
    const config = selected?.config;
    const validConfig = config && config.consumerGroup === data.consumerGroup && config.brokerAddress === address && config.brokerName === selected?.brokerName;
    return <div className="ops-consumer-stack"><p className="ops-consumer-note">Configuration discovery and writes use NameServer and direct Broker addresses, independently of the connection/progress query mode.</p>
        <StatusBadge tone={data.complete ? 'success' : 'warning'}>{data.complete ? 'All discovered Brokers read' : 'Incomplete coverage'}</StatusBadge>
        {data.discoveryFailures.length > 0 && <PageState kind="partial" title="Some Broker inventory is unavailable" description={data.discoveryFailures.join(', ')} />}
        <p>Different fields: {data.inconsistentFields.join(', ') || 'None among successful reads'}. {!data.complete && 'Consistency across all Brokers is unknown.'}</p>
        <div className="ops-consumer-scroll" role="region" aria-label="Consumer Broker configuration comparison" tabIndex={0}><table><thead><tr><th scope="col">Broker / Cluster</th><th scope="col">Address</th><th scope="col">Read result</th><th scope="col">Inspect</th></tr></thead>
            <tbody>{data.targets.map(item => <tr key={item.brokerName + ':' + item.brokerAddress} data-selected={address === item.brokerAddress}><th scope="row">{item.brokerName} / {item.clusterName}</th><td>{item.brokerAddress}</td>
                <td>{item.error || (item.config ? 'Read' : 'Not returned')}</td><td><Button variant="outline" onClick={() => setAddress(item.brokerAddress)}>View details</Button></td></tr>)}</tbody></table></div>
        <ConsumerJson label="Common values among successful reads" value={data.effective} />
        {address && !selected && <PageState kind="stale" title="Selected Broker is no longer in this result" />}
        {selected?.error && <PageState kind="error" title={'Configuration unavailable on ' + selected.brokerName} description={selected.error} />}
        {config && !validConfig && <PageState kind="error" title="Returned configuration does not match the selected Broker" />}
        {validConfig && <PageSection title={selected!.brokerName + ' configuration'} description={address}
            action={<Button variant="outline" disabled={blocked} onClick={() => onEdit(address)}>Edit this Broker configuration</Button>}>
            <Input label="Filter configuration fields" value={search} onChange={event => setSearch(event.target.value)} />
            {data.inconsistentFields.length > 0 && <ConsumerJson label="Different field values on this Broker" value={Object.fromEntries(data.inconsistentFields.map(field => [field, config[field as keyof typeof config]]))} open />}
            <ConsumerProperties rows={[...consumerSwitches.map(([field, label]): [string, ReactNode] => [label, config[field] ? 'Enabled' : 'Disabled']),
                ...consumerNumberFields.map(([field, label]): [string, ReactNode] => [label, config[field]])].filter(([label]) => label.toLowerCase().includes(search.toLowerCase()))} />
            <ConsumerJson label="Retry policy" value={config.groupRetryPolicyJson} /><ConsumerJson label={'Subscription Topics (' + config.subscriptionTopicCount + ')'} value={config.subscriptionTopics} />
            <ConsumerJson label="Custom attributes" value={config.attributes} />
        </PageSection>}
        {!address && <p className="ops-consumer-note">Choose a Broker to inspect its values and use that exact configuration as the edit source.</p>}
    </div>;
}
