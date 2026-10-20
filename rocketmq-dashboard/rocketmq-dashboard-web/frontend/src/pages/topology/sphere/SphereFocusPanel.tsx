import { ChevronsDown, ChevronsUp, Crosshair, ExternalLink, X } from 'lucide-react';
import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { StatusDot, type StatusDotTone } from '../../../components/ui/StatusDot';
import { GROUP_PREVIEW, type GroupSide } from '../TopologyMap';
import { formatCount, formatRate, formatTps } from '../topology-model';
import {
  kindName,
  linkedNodes,
  nodeMetric,
  nodeName,
  nodeTone,
  replicaBehind,
  type ConsumerNode,
  type ControllerNode,
  type MasterNode,
  type ProducerNode,
  type SlaveNode,
  type SphereModel,
  type SphereNode
} from './sphere-model';

interface SphereFocusPanelProps {
  model: SphereModel;
  selected: string | null;
  onSelect: (id: string | null) => void;
  expanded: Record<GroupSide, boolean>;
  onToggleGroups: (side: GroupSide) => void;
}

const plural = (count: number, one: string, many: string) => `${formatCount(count)} ${count === 1 ? one : many}`;
const formatTime = (timestamp: number) =>
  new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });

function formatBytes(bytes: number) {
  if (bytes < 1024) return `${formatCount(bytes)} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unit]}`;
}

function dotTone(node: SphereNode): StatusDotTone {
  switch (nodeTone(node)) {
    case 'danger':
      return 'danger';
    case 'warning':
      return 'warning';
    case 'idle':
    case 'unknown':
      return 'neutral';
    case 'current':
      return 'primary';
    default:
      return node.kind === 'producer' || node.kind === 'consumer' ? 'info' : 'success';
  }
}

/** The page a node opens, with the label of the link; the dashboard has no page for a Controller. */
function pageOf(node: SphereNode): { to: string; label: string } | null {
  switch (node.kind) {
    case 'master':
    case 'slave':
      return { to: `/brokers/${encodeURIComponent(node.group.brokerName)}`, label: 'Open broker' };
    case 'consumer':
      return { to: `/consumers/${encodeURIComponent(node.consumer.group)}`, label: 'Open consumer group' };
    case 'producer':
      return { to: '/producers', label: 'Open producers' };
    case 'nameserver':
      return { to: '/config', label: 'Open OPS' };
    case 'controller':
      return null;
  }
}

function overline(node: SphereNode) {
  if (node.kind === 'master') return `Broker group · ${node.clusterName}`;
  if (node.kind === 'slave') return `${node.group.election === 'controller' ? 'Replica' : 'Slave'} · ${node.group.brokerName}`;
  if (node.kind === 'controller') return 'Controller · Raft';
  return kindName(node.kind);
}

function modeText(node: MasterNode) {
  const { group } = node;
  if (!group.masterAddress) return 'No master registered';
  if (group.election === 'controller') return `Controller mode · ${plural(group.slaveAddresses.length, 'replica', 'replicas')}`;
  return group.slaveAddresses.length === 0 ? 'Single master' : `Master/Slave · ${plural(group.slaveAddresses.length, 'slave', 'slaves')}`;
}

/** The Controller that leads the Raft group of a Controller-mode broker group. */
function leaderOf(group: MasterNode['group'], model: SphereModel): ControllerNode | undefined {
  const addresses = new Set(group.controllerAddresses ?? []);
  return model.nodes.find(
    (node): node is ControllerNode => node.kind === 'controller' && node.controller.leader === true && addresses.has(node.controller.address)
  );
}

const ELECTION_TEXT = { configured: 'Configured', controller: 'Controllers', unknown: 'Unknown' } as const;

/** Why a node needs attention, short enough to leave its name readable. */
function attentionText(node: SphereNode) {
  if (node.kind !== 'slave') return nodeMetric(node);
  if (node.inSyncStateSet === false) return 'out of sync set';
  const lag = node.replica?.lagBytes;
  return lag === undefined ? 'not connected' : `lag ${formatBytes(lag)}`;
}

function replicaFacts(node: SlaveNode): Fact[] {
  const facts: Fact[] = [];
  const behind = replicaBehind(node);
  if (node.inSyncStateSet !== null) facts.push(['Sync state set', node.inSyncStateSet ? 'In' : 'Out', node.inSyncStateSet ? 'success' : 'warning']);
  const { replica } = node;
  if (replica && replica.inSync !== undefined) {
    if (replica.lagBytes === undefined) facts.push(['Replication', 'Not connected', 'warning']);
    else facts.push(['Lag', formatBytes(replica.lagBytes), behind && node.inSyncStateSet === null ? 'warning' : undefined]);
    if (replica.transferBytesPerSecond !== undefined) facts.push(['Transfer', `${formatBytes(replica.transferBytesPerSecond)}/s`]);
  }
  return facts;
}

type Fact = [label: string, value: string, tone?: 'primary' | 'cyan' | 'warning' | 'danger' | 'success'];

function factsOf(node: SphereNode, model: SphereModel): Fact[] {
  switch (node.kind) {
    case 'master': {
      const { group } = node;
      const controllerMode = group.election === 'controller';
      const facts: Fact[] = [
        ['Master', group.masterAddress ?? 'Not registered', group.masterAddress ? undefined : 'danger'],
        [controllerMode ? 'Replicas' : 'Slaves', group.slaveAddresses.length === 0 ? 'None' : group.slaveAddresses.join(', ')],
        ['Cluster', node.clusterName]
      ];
      if (group.election) facts.push(['Master chosen by', ELECTION_TEXT[group.election]]);
      if (controllerMode) {
        const leader = leaderOf(group, model);
        facts.push(['Controller leader', leader?.controller.address ?? 'None known', leader ? undefined : 'warning']);
      }
      const state = group.controllerState;
      if (state) {
        facts.push(['Master epoch', formatCount(state.masterEpoch)]);
        const members = state.inSyncBrokerIds.length + state.outOfSyncBrokerIds.length;
        facts.push(['Sync state set', `${formatCount(state.inSyncBrokerIds.length)} of ${plural(members, 'replica', 'replicas')} · epoch ${formatCount(state.syncStateSetEpoch)}`, state.outOfSyncBrokerIds.length > 0 ? 'warning' : 'success']);
      } else if (group.inSyncSlaveCount !== undefined && group.slaveAddresses.length > 0) {
        facts.push(['Connected slaves', formatCount((group.replicas ?? []).filter((replica) => replica.lagBytes !== undefined).length)]);
      }
      facts.push(['Produce', `${formatTps(group.produceTps)} TPS`, 'primary'], ['Consume', `${formatTps(group.consumeTps)} TPS`, 'cyan'], ['Version', group.version || 'Unknown']);
      return facts;
    }
    case 'slave':
      return [
        ['Address', node.address],
        ...(node.replica ? [['Broker ID', String(node.replica.brokerId)] as Fact] : []),
        ['Broker group', node.group.brokerName],
        ['Master', node.group.masterAddress ?? 'Not registered', node.group.masterAddress ? undefined : 'danger'],
        ...replicaFacts(node),
        ['Cluster', node.clusterName]
      ];
    case 'controller': {
      const { controller } = node;
      const role = controller.leader === undefined ? 'No leader known' : controller.leader ? 'Leader' : 'Follower';
      const facts: Fact[] = [
        ['Status', controller.status === 'available' ? 'Available' : 'Unavailable', controller.status === 'available' ? 'success' : 'danger'],
        ['Role', role, controller.leader === undefined ? 'warning' : controller.leader ? 'primary' : undefined]
      ];
      if (controller.leaderId !== undefined) facts.push(['Raft leader', `node ${controller.leaderId}`]);
      if (controller.group !== undefined) facts.push(['Raft group', controller.group]);
      if (controller.peerCount !== undefined) facts.push(['Peers', formatCount(controller.peerCount)]);
      if (controller.appliedLogIndex !== undefined) facts.push(['Applied index', formatCount(controller.appliedLogIndex)]);
      if (controller.committedLogIndex !== undefined) facts.push(['Committed index', formatCount(controller.committedLogIndex)]);
      facts.push(['Elects masters of', node.brokerNames.join(', ') || 'None']);
      return facts;
    }
    case 'nameserver': {
      const { nameServer } = node;
      const status = { available: 'Available', unavailable: 'Unavailable', unknown: 'Unknown' }[nameServer.status];
      const facts: Fact[] = [
        ['Status', status, nameServer.status === 'available' ? 'success' : nameServer.status === 'unavailable' ? 'danger' : undefined],
        ['Role', nameServer.current ? 'Current' : 'Configured']
      ];
      if (nameServer.checkedAt !== null) facts.push(['Checked', formatTime(nameServer.checkedAt)]);
      const count = (kind: string, end: 'from' | 'to') => model.links.filter((link) => link.kind === kind && link[end] === node.id).length;
      facts.push(['Heartbeats from', plural(count('heartbeat', 'to'), 'master', 'masters')]);
      facts.push(['Route lookups', plural(count('route', 'from'), 'client group', 'client groups')]);
      return facts;
    }
    case 'producer':
      return [
        ['Connections', formatCount(node.producer.connectionCount)],
        ['Brokers', node.producer.brokerNames.join(', ') || 'None reported'],
        ...(node.producer.topics.length > 0 ? [['Topics', node.producer.topics.join(', ')] as Fact] : [])
      ];
    case 'consumer': {
      const { consumer } = node;
      return [
        ['Connections', formatCount(consumer.connectionCount)],
        ['Consume', `${formatRate(consumer.consumeTps)} TPS`, 'cyan'],
        ['Lag', formatCount(consumer.diffTotal), nodeTone(node) === 'warning' ? 'warning' : undefined],
        ['Brokers', consumer.brokerNames.join(', ') || 'None reported']
      ];
    }
  }
}

function NodeDetails({ node, model, onSelect }: { node: SphereNode; model: SphereModel; onSelect: (id: string | null) => void }) {
  const page = pageOf(node);
  const linked = linkedNodes(model, node.id);
  return (
    <>
      <div className="topology-sphere-focus-head">
        <div>
          <span className="topology-overline">{overline(node)}</span>
          <h3>{nodeName(node)}</h3>
        </div>
        <button type="button" className="topology-sphere-control" aria-label="Clear focus" title="Clear focus" onClick={() => onSelect(null)}>
          <X size={14} aria-hidden="true" />
        </button>
      </div>
      <dl className="topology-sphere-facts">
        {factsOf(node, model).map(([label, value, tone]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd data-tone={tone}>{value}</dd>
          </div>
        ))}
      </dl>
      {page ? (
        <Link className="topology-sphere-page-link" to={page.to}>
          {page.label}
          <ExternalLink size={13} aria-hidden="true" />
        </Link>
      ) : null}
      {linked.length > 0 ? (
        <section className="topology-sphere-section" aria-label="Linked nodes">
          <span className="topology-overline">{`Linked · ${formatCount(linked.length)}`}</span>
          <ul className="topology-sphere-list">
            {linked.map(({ node: other, kind }) => (
              <li key={`${kind}:${other.id}`}>
                <button type="button" className="topology-sphere-row" onClick={() => onSelect(other.id)}>
                  <StatusDot tone={dotTone(other)} />
                  <span className="topology-sphere-row-name">{nodeName(other)}</span>
                  <span className="topology-sphere-row-value">{kind}</span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </>
  );
}

interface GroupToggleProps {
  side: GroupSide;
  shown: number;
  total: number;
  expanded: boolean;
  onToggle: (side: GroupSide) => void;
}

function GroupToggle({ side, shown, total, expanded, onToggle }: GroupToggleProps) {
  if (total <= GROUP_PREVIEW) return null;
  const group = side === 'producers' ? 'producer' : 'consumer';
  const action = expanded ? `Show the top ${GROUP_PREVIEW} ${group} groups` : `Show all ${group} groups`;
  const Icon = expanded ? ChevronsUp : ChevronsDown;
  return (
    <button type="button" className="topology-sphere-toggle" aria-expanded={expanded} onClick={() => onToggle(side)}>
      <Icon size={14} aria-hidden="true" />
      {action}
      <span className="sr-only">{`, ${formatCount(shown)} of ${formatCount(total)} shown`}</span>
    </button>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="topology-sphere-section" aria-label={title}>
      <span className="topology-overline">{title}</span>
      {children}
    </section>
  );
}

function Row({ node, value, onSelect }: { node: SphereNode; value: string; onSelect: (id: string) => void }) {
  const tone = nodeTone(node);
  return (
    <li>
      <button type="button" className="topology-sphere-row" data-tone={tone === 'warning' || tone === 'danger' ? tone : undefined} onClick={() => onSelect(node.id)}>
        <StatusDot tone={dotTone(node)} />
        <span className="topology-sphere-row-name">{nodeName(node)}</span>
        <span className="topology-sphere-row-value">{value}</span>
      </button>
    </li>
  );
}

function Overview({ model, onSelect, expanded, onToggleGroups }: Omit<SphereFocusPanelProps, 'selected'>) {
  const masters = model.nodes.filter((node): node is MasterNode => node.kind === 'master');
  const controllers = model.nodes.filter((node): node is ControllerNode => node.kind === 'controller');
  const consumers = model.nodes
    .filter((node): node is ConsumerNode => node.kind === 'consumer')
    .sort((left, right) => right.consumer.diffTotal - left.consumer.diffTotal || nodeName(left).localeCompare(nodeName(right)));
  const producers = model.nodes
    .filter((node): node is ProducerNode => node.kind === 'producer')
    .sort((left, right) => right.producer.connectionCount - left.producer.connectionCount || nodeName(left).localeCompare(nodeName(right)));
  // Most severe first: nodes that cannot work, then the largest backlogs.
  const severity = (node: SphereNode) => (nodeTone(node) === 'danger' ? 0 : 1);
  const backlog = (node: SphereNode) => (node.kind === 'consumer' ? node.consumer.diffTotal : 0);
  const attention = model.nodes
    .filter((node) => {
      const tone = nodeTone(node);
      return tone === 'danger' || tone === 'warning';
    })
    .sort((left, right) => severity(left) - severity(right) || backlog(right) - backlog(left) || nodeName(left).localeCompare(nodeName(right)));
  const { groups } = model;
  return (
    <>
      <div className="topology-sphere-focus-intro">
        <h3>
          <Crosshair size={15} aria-hidden="true" />
          Focus
        </h3>
        <p>Select a node on a sphere or below to bring it to the front and trace its links.</p>
      </div>
      {attention.length > 0 ? (
        <Section title="Needs attention">
          <ul className="topology-sphere-list">
            {attention.map((node) => <Row key={node.id} node={node} value={attentionText(node)} onSelect={onSelect} />)}
          </ul>
        </Section>
      ) : null}
      <Section title={`Broker groups · ${formatCount(masters.length)}`}>
        <ul className="topology-sphere-list">
          {masters.map((node) => <Row key={node.id} node={node} value={modeText(node)} onSelect={onSelect} />)}
        </ul>
      </Section>
      {controllers.length > 0 ? (
        <Section title={`Controllers · ${formatCount(controllers.length)}`}>
          <ul className="topology-sphere-list">
            {controllers.map((node) => <Row key={node.id} node={node} value={nodeMetric(node)} onSelect={onSelect} />)}
          </ul>
        </Section>
      ) : null}
      <Section title={`Consumer groups · ${formatCount(groups.consumers)} of ${formatCount(groups.consumersInScope)}`}>
        <ul className="topology-sphere-list">
          {consumers.map((node) => <Row key={node.id} node={node} value={`lag ${formatCount(node.consumer.diffTotal)}`} onSelect={onSelect} />)}
        </ul>
        <GroupToggle side="consumers" shown={groups.consumers} total={groups.consumersInScope} expanded={expanded.consumers} onToggle={onToggleGroups} />
      </Section>
      <Section title={`Producer groups · ${formatCount(groups.producers)} of ${formatCount(groups.producersInScope)}`}>
        <ul className="topology-sphere-list">
          {producers.map((node) => <Row key={node.id} node={node} value={plural(node.producer.connectionCount, 'connection', 'connections')} onSelect={onSelect} />)}
        </ul>
        <GroupToggle side="producers" shown={groups.producers} total={groups.producersInScope} expanded={expanded.producers} onToggle={onToggleGroups} />
      </Section>
      <Section title="Reading the spheres">
        <ul className="topology-sphere-key">
          <li data-kind="nameserver">Left sphere: the NameServers</li>
          <li data-kind="producer">Northern half: producer groups</li>
          <li data-kind="broker">Equator: clusters, each master with its slaves</li>
          <li data-kind="consumer">Southern half: consumer groups</li>
          <li data-kind="heartbeat">Pink lines: masters register with the NameServers</li>
          <li data-kind="route">Violet lines: client groups look up their routes</li>
          {controllers.length > 0 ? (
            <>
              <li data-kind="controller">Right sphere: the Controllers, the leader ringed</li>
              <li data-kind="election">Silver lines: brokers report to the leading Controller, which elects their master</li>
            </>
          ) : null}
        </ul>
      </Section>
    </>
  );
}

/** Beside the sphere: the selected node with its links, or an overview of what the spheres hold. */
export default function SphereFocusPanel({ model, selected, onSelect, expanded, onToggleGroups }: SphereFocusPanelProps) {
  const node = selected ? model.byId.get(selected) : undefined;
  return (
    <aside className="topology-sphere-focus" aria-label="Focus">
      {node ? (
        <NodeDetails node={node} model={model} onSelect={onSelect} />
      ) : (
        <Overview model={model} onSelect={onSelect} expanded={expanded} onToggleGroups={onToggleGroups} />
      )}
    </aside>
  );
}
