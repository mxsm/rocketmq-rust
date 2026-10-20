import React, {useEffect, useId, useRef, useState, type CSSProperties} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import SectionHeading from './SectionHeading';
import {ArrowRightIcon, PauseIcon, PlayIcon} from './icons';
import {Reveal, useInView, usePrefersReducedMotion} from './motion';
import primitives from './primitives.module.css';
import styles from './ArchitectureFlow.module.css';

type Tone = 'control' | 'produce' | 'store' | 'consume';
type NodeId = 'namesrv' | 'producer' | 'consumer' | 'master' | 'slave' | 'controller' | 'proxy';
type RouteId =
  | 'register'
  | 'routeProducer'
  | 'routeConsumer'
  | 'send'
  | 'proxy'
  | 'replicate'
  | 'pull'
  | 'ack'
  | 'controlMaster'
  | 'controlSlave';

type Route = {
  /** Path in diagram coordinates, drawn in the direction packets travel. */
  d: string;
  tone: Tone;
  dashed?: boolean;
  packets: number;
  /** Seconds for one packet to traverse the path. */
  duration: number;
  /** Request/response: packets travel out and back along the same path. */
  bounce?: boolean;
};

const ROUTES: Record<RouteId, Route> = {
  register: {d: 'M440 190V98', tone: 'control', dashed: true, packets: 2, duration: 1.6},
  routeProducer: {d: 'M112 196C112 104 214 60 340 60', tone: 'control', dashed: true, packets: 1, duration: 2.6, bounce: true},
  routeConsumer: {d: 'M768 196C768 104 666 60 540 60', tone: 'control', dashed: true, packets: 1, duration: 2.6, bounce: true},
  send: {d: 'M200 242H316', tone: 'produce', packets: 3, duration: 1.2},
  proxy: {d: 'M680 404C628 404 620 296 564 296', tone: 'produce', dashed: true, packets: 2, duration: 1.8},
  replicate: {d: 'M440 322V358', tone: 'store', packets: 2, duration: 1},
  pull: {d: 'M564 232H680', tone: 'consume', packets: 3, duration: 1.2},
  ack: {d: 'M680 256H564', tone: 'consume', dashed: true, packets: 1, duration: 1.6},
  controlMaster: {d: 'M200 404C252 404 262 300 316 300', tone: 'control', dashed: true, packets: 1, duration: 1.8},
  controlSlave: {d: 'M200 420H316', tone: 'control', dashed: true, packets: 2, duration: 1.4},
};
const ROUTE_IDS = Object.keys(ROUTES) as RouteId[];

type StepId = 'register' | 'discover' | 'send' | 'persist' | 'consume' | 'failover';

type Step = {
  id: StepId;
  routes: RouteId[];
  nodes: NodeId[];
  durationMs: number;
};

const STEPS: Step[] = [
  {id: 'register', routes: ['register'], nodes: ['master', 'slave', 'namesrv'], durationMs: 4600},
  {id: 'discover', routes: ['routeProducer', 'routeConsumer'], nodes: ['producer', 'consumer', 'namesrv'], durationMs: 4600},
  {id: 'send', routes: ['send', 'proxy'], nodes: ['producer', 'proxy', 'master'], durationMs: 4600},
  {id: 'persist', routes: ['replicate'], nodes: ['master', 'slave'], durationMs: 4600},
  {id: 'consume', routes: ['pull', 'ack'], nodes: ['master', 'consumer'], durationMs: 4600},
  // Longer: the promotion sequence needs time to play out.
  {id: 'failover', routes: ['controlMaster', 'controlSlave'], nodes: ['controller', 'slave'], durationMs: 6400},
];

const COMMIT_LOG_BLOCKS = 9;
const CONSUME_QUEUE_BLOCKS = 8;

function Packets({route, pathId}: {route: Route; pathId: string}): React.JSX.Element {
  return (
    <>
      {Array.from({length: route.packets}, (_, index) => (
        <circle key={index} r="4.2" className={clsx(styles.packet, styles[route.tone])}>
          <animateMotion
            dur={`${route.duration}s`}
            // Negative offsets spread the packets evenly along the path.
            begin={`${(-index * route.duration) / route.packets}s`}
            repeatCount="indefinite"
            {...(route.bounce ? {keyPoints: '0;1;0', keyTimes: '0;0.5;1', calcMode: 'linear'} : {})}>
            <mpath href={`#${pathId}`} />
          </animateMotion>
        </circle>
      ))}
    </>
  );
}

function LogBlocks({
  x,
  y,
  count,
  width,
  height,
  pitch,
}: {
  x: number;
  y: number;
  count: number;
  width: number;
  height: number;
  pitch: number;
}): React.JSX.Element {
  return (
    <g className={styles.blocks}>
      {Array.from({length: count}, (_, index) => (
        <rect
          key={index}
          x={x + index * pitch}
          y={y}
          width={width}
          height={height}
          rx="2.5"
          style={{'--block': index} as CSSProperties}
        />
      ))}
    </g>
  );
}

type NodeBoxProps = {
  x: number;
  y: number;
  width: number;
  height: number;
  tone: Tone;
  title: string;
  subtitle: string;
  active: boolean;
  className?: string;
  children?: React.ReactNode;
};

function NodeBox({x, y, width, height, tone, title, subtitle, active, className, children}: NodeBoxProps): React.JSX.Element {
  return (
    <g
      className={clsx(styles.node, styles[tone], active && styles.nodeActive, className)}
      transform={`translate(${x} ${y})`}>
      <rect className={styles.nodeBox} width={width} height={height} rx="16" />
      <text className={styles.nodeTitle} x="18" y="31">
        {title}
      </text>
      <text className={styles.nodeSub} x="18" y="51">
        {subtitle}
      </text>
      {children}
    </g>
  );
}

function Badge({x, label, className}: {x: number; label: string; className?: string}): React.JSX.Element {
  return (
    <g className={clsx(styles.badge, className)} transform={`translate(${x} 15)`}>
      <rect width="66" height="22" rx="11" />
      <text x="33" y="15">
        {label}
      </text>
    </g>
  );
}

export default function ArchitectureFlow(): React.JSX.Element {
  const idPrefix = useId().replace(/:/g, '');
  const reducedMotion = usePrefersReducedMotion();
  const [sectionRef, inView] = useInView<HTMLElement>({threshold: 0.25});
  const scrollRef = useRef<HTMLDivElement>(null);
  const [stepIndex, setStepIndex] = useState(0);
  const [autoplay, setAutoplay] = useState(true);
  const [engaged, setEngaged] = useState(false);

  const step = STEPS[stepIndex];
  const playing = autoplay && inView && !engaged && !reducedMotion;

  // On narrow screens the diagram scrolls sideways; start centred on the broker group.
  useEffect(() => {
    const scroller = scrollRef.current;
    if (scroller) {
      scroller.scrollLeft = (scroller.scrollWidth - scroller.clientWidth) / 2;
    }
  }, []);

  useEffect(() => {
    if (!playing) {
      return undefined;
    }
    const timer = window.setTimeout(() => setStepIndex((current) => (current + 1) % STEPS.length), step.durationMs);
    return () => window.clearTimeout(timer);
  }, [playing, step]);

  const stepCopy: Record<StepId, {title: string; detail: string}> = {
    register: {
      title: translate({id: 'homepage.arch.step.register.title', message: 'Register'}),
      detail: translate({
        id: 'homepage.arch.step.register.detail',
        message: 'Brokers register their topics with the NameServer and keep the route table fresh with heartbeats.',
      }),
    },
    discover: {
      title: translate({id: 'homepage.arch.step.discover.title', message: 'Discover'}),
      detail: translate({
        id: 'homepage.arch.step.discover.detail',
        message: 'Producers and consumers ask the NameServer for a topic route, then connect straight to the right Broker.',
      }),
    },
    send: {
      title: translate({id: 'homepage.arch.step.send.title', message: 'Send'}),
      detail: translate({
        id: 'homepage.arch.step.send.detail',
        message: 'The producer picks a queue and sends asynchronously, over remoting or over gRPC through the Proxy.',
      }),
    },
    persist: {
      title: translate({id: 'homepage.arch.step.persist.title', message: 'Persist'}),
      detail: translate({
        id: 'homepage.arch.step.persist.detail',
        message: 'The Broker appends to the CommitLog, builds ConsumeQueue and index entries, and replicates to its slave.',
      }),
    },
    consume: {
      title: translate({id: 'homepage.arch.step.consume.title', message: 'Consume'}),
      detail: translate({
        id: 'homepage.arch.step.consume.detail',
        message: 'Consumers receive messages through Push, LitePull or POP, then commit offsets or acknowledge receipts.',
      }),
    },
    failover: {
      title: translate({id: 'homepage.arch.step.failover.title', message: 'Fail over'}),
      detail: translate({
        id: 'homepage.arch.step.failover.detail',
        message: 'When a master goes down, the Raft-based Controller elects a new one from the in-sync replicas.',
      }),
    },
  };

  const isActiveNode = (node: NodeId): boolean => step.nodes.includes(node);
  const failover = step.id === 'failover';
  const masterLabel = translate({id: 'homepage.arch.badge.master', message: 'MASTER'});
  const slaveLabel = translate({id: 'homepage.arch.badge.slave', message: 'SLAVE'});

  return (
    <section ref={sectionRef} className={clsx(primitives.section, styles.section)} aria-labelledby={`${idPrefix}-title`}>
      <div className={styles.glow} aria-hidden="true" />
      <div className={primitives.container}>
        <SectionHeading
          id={`${idPrefix}-title`}
          eyebrow={translate({id: 'homepage.arch.eyebrow', message: 'Architecture in motion'})}
          title={translate({id: 'homepage.arch.title', message: 'Follow a message from producer to consumer'})}
          lead={translate({
            id: 'homepage.arch.lead',
            message:
              'Route discovery, sending, persistence, replication and delivery: every hop of the RocketMQ model, implemented in async Rust.',
          })}
        />

        <div className={styles.layout}>
          <Reveal className={styles.diagramCard}>
            <div ref={scrollRef} className={styles.diagramScroll}>
              <svg
                className={clsx(styles.diagram, styles[`step-${step.id}`])}
                viewBox="0 0 880 520"
                role="img"
                aria-label={translate({
                  id: 'homepage.arch.diagram.label',
                  message:
                    'Cluster diagram: producer, NameServer, a broker group with master and slave, consumer, Controller and Proxy. The highlighted links show the selected step.',
                })}>
                {/* Broker group outline */}
                <g className={styles.group}>
                  <rect x="296" y="150" width="288" height="352" rx="24" />
                  <text x="316" y="176">
                    {translate({id: 'homepage.arch.group', message: 'BROKER GROUP'})}
                  </text>
                </g>

                {/* Links */}
                <g className={styles.routes}>
                  {ROUTE_IDS.map((routeId) => {
                    const route = ROUTES[routeId];
                    const active = step.routes.includes(routeId);
                    return (
                      <path
                        key={routeId}
                        id={`${idPrefix}-${routeId}`}
                        d={route.d}
                        className={clsx(
                          styles.route,
                          styles[route.tone],
                          route.dashed && styles.routeDashed,
                          active && styles.routeActive,
                        )}
                      />
                    );
                  })}
                </g>

                {/* Nodes */}
                <NodeBox
                  x={340}
                  y={22}
                  width={200}
                  height={76}
                  tone="control"
                  title="NameServer"
                  subtitle={translate({id: 'homepage.arch.node.namesrv', message: 'route registry · :9876'})}
                  active={isActiveNode('namesrv')}
                />
                <NodeBox
                  x={24}
                  y={196}
                  width={176}
                  height={92}
                  tone="produce"
                  title="Producer"
                  subtitle={translate({id: 'homepage.arch.node.producer', message: 'async Rust client'})}
                  active={isActiveNode('producer')}>
                  <text className={styles.nodeNote} x="18" y="74">
                    batch · ordered · txn
                  </text>
                </NodeBox>
                <NodeBox
                  x={680}
                  y={196}
                  width={176}
                  height={92}
                  tone="consume"
                  title="Consumer"
                  subtitle={translate({id: 'homepage.arch.node.consumer', message: 'consumer group'})}
                  active={isActiveNode('consumer')}>
                  <text className={styles.nodeNote} x="18" y="74">
                    Push · LitePull · POP
                  </text>
                </NodeBox>
                <NodeBox
                  x={24}
                  y={374}
                  width={176}
                  height={92}
                  tone="control"
                  title="Controller"
                  subtitle={translate({id: 'homepage.arch.node.controller', message: 'Raft quorum'})}
                  active={isActiveNode('controller')}
                  className={clsx(failover && styles.electing)}>
                  <g className={styles.quorum} transform="translate(18 66)">
                    <circle cx="6" cy="6" r="5" />
                    <circle cx="26" cy="6" r="5" />
                    <circle cx="46" cy="6" r="5" />
                  </g>
                </NodeBox>
                <NodeBox
                  x={680}
                  y={374}
                  width={176}
                  height={92}
                  tone="produce"
                  title="Proxy"
                  subtitle={translate({id: 'homepage.arch.node.proxy', message: 'gRPC ingress · :8081'})}
                  active={isActiveNode('proxy')}>
                  <text className={styles.nodeNote} x="18" y="74">
                    cluster · local mode
                  </text>
                </NodeBox>

                <NodeBox
                  x={316}
                  y={190}
                  width={248}
                  height={132}
                  tone="store"
                  title="Broker"
                  subtitle=":10911 · remoting"
                  active={isActiveNode('master')}
                  className={clsx(styles.master, failover && styles.masterDown)}>
                  {failover ? (
                    <Badge
                      x={164}
                      className={styles.badgeDown}
                      label={translate({id: 'homepage.arch.badge.offline', message: 'OFFLINE'})}
                    />
                  ) : (
                    <Badge x={164} label={masterLabel} />
                  )}
                  <text className={styles.nodeNote} x="18" y="83">
                    CommitLog
                  </text>
                  <LogBlocks x={110} y={72} count={COMMIT_LOG_BLOCKS} width={11} height={14} pitch={14} />
                  <text className={styles.nodeNote} x="18" y="110">
                    ConsumeQueue
                  </text>
                  <LogBlocks x={110} y={101} count={CONSUME_QUEUE_BLOCKS} width={8} height={10} pitch={12} />
                </NodeBox>

                <NodeBox
                  x={316}
                  y={358}
                  width={248}
                  height={124}
                  tone="store"
                  title="Broker"
                  subtitle={translate({id: 'homepage.arch.node.slave', message: 'HA replica'})}
                  active={isActiveNode('slave')}
                  className={clsx(styles.slave, failover && styles.slavePromoted)}>
                  <Badge x={164} className={styles.badgeBefore} label={slaveLabel} />
                  {failover && <Badge x={164} className={styles.badgeAfter} label={masterLabel} />}
                  <text className={styles.nodeNote} x="18" y="83">
                    CommitLog
                  </text>
                  <LogBlocks x={110} y={72} count={COMMIT_LOG_BLOCKS} width={11} height={14} pitch={14} />
                </NodeBox>

                {/* Packets ride on top of the nodes' edges. */}
                {!reducedMotion && inView && (
                  <g aria-hidden="true">
                    {step.routes.map((routeId) => (
                      <Packets key={routeId} route={ROUTES[routeId]} pathId={`${idPrefix}-${routeId}`} />
                    ))}
                  </g>
                )}
              </svg>
            </div>

            <ul className={styles.legend}>
              <li className={styles.control}>{translate({id: 'homepage.arch.legend.control', message: 'Control plane'})}</li>
              <li className={styles.produce}>{translate({id: 'homepage.arch.legend.produce', message: 'Produce'})}</li>
              <li className={styles.store}>{translate({id: 'homepage.arch.legend.store', message: 'Store and replicate'})}</li>
              <li className={styles.consume}>{translate({id: 'homepage.arch.legend.consume', message: 'Consume'})}</li>
            </ul>
          </Reveal>

          <Reveal className={styles.panel} delay={120}>
            <ol
              className={styles.steps}
              onPointerEnter={() => setEngaged(true)}
              onPointerLeave={() => setEngaged(false)}
              onFocus={() => setEngaged(true)}
              onBlur={() => setEngaged(false)}>
              {STEPS.map((item, index) => {
                const current = index === stepIndex;
                return (
                  <li key={item.id}>
                    <button
                      type="button"
                      className={clsx(styles.step, current && styles.stepCurrent)}
                      aria-current={current ? 'step' : undefined}
                      onClick={() => setStepIndex(index)}>
                      <span className={styles.stepIndex}>{String(index + 1).padStart(2, '0')}</span>
                      <span className={styles.stepBody}>
                        <span className={styles.stepTitle}>{stepCopy[item.id].title}</span>
                        <span className={styles.stepDetail}>
                          <span>{stepCopy[item.id].detail}</span>
                        </span>
                      </span>
                      {current && playing && (
                        <span
                          className={styles.stepProgress}
                          style={{animationDuration: `${item.durationMs}ms`}}
                          aria-hidden="true"
                        />
                      )}
                    </button>
                  </li>
                );
              })}
            </ol>

            <div className={styles.panelFooter}>
              <button type="button" className={styles.toggle} onClick={() => setAutoplay((current) => !current)}>
                {autoplay ? <PauseIcon /> : <PlayIcon />}
                {autoplay
                  ? translate({id: 'homepage.arch.pause', message: 'Pause tour'})
                  : translate({id: 'homepage.arch.play', message: 'Play tour'})}
              </button>
              <Link className={primitives.textLink} to="/docs/architecture/overview">
                {translate({id: 'homepage.arch.link', message: 'Architecture overview'})}
                <ArrowRightIcon />
              </Link>
            </div>
          </Reveal>
        </div>
      </div>
    </section>
  );
}
