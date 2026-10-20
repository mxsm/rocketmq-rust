import React, {useEffect, useRef, useState} from 'react';
import clsx from 'clsx';
import {translate} from '@docusaurus/Translate';
import {useInView, usePrefersReducedMotion} from './motion';
import styles from './HeroTerminal.module.css';

type ClusterEvent = 'namesrv' | 'broker' | 'send';

type ScriptStep =
  | {kind: 'command'; text: string}
  | {kind: 'output'; tone: 'dim' | 'ok' | 'send'; text: string; delay: number; event?: ClusterEvent};

type TerminalLine = {
  id: number;
  kind: 'command' | 'output';
  tone?: 'dim' | 'ok' | 'send';
  text: string;
};

type ClusterState = {
  namesrv: boolean;
  broker: boolean;
  sent: number;
};

const SEND_COUNT = 12;
const QUEUE_COUNT = 4;
const LOOP_PAUSE_MS = 4200;
const IDLE_CLUSTER: ClusterState = {namesrv: false, broker: false, sent: 0};

/**
 * Mirrors the README quick start. Commands are real; the status lines are a
 * condensed illustration of what each process reports.
 */
const SCRIPT: ScriptStep[] = [
  {kind: 'command', text: 'cargo run --bin rocketmq-namesrv-rust'},
  {kind: 'output', tone: 'dim', text: '    Finished `dev` profile in 0.42s', delay: 460},
  {kind: 'output', tone: 'ok', text: 'NameServer ready on 127.0.0.1:9876', delay: 520, event: 'namesrv'},
  {kind: 'command', text: 'cargo run --bin rocketmq-broker-rust -- -n 127.0.0.1:9876'},
  {kind: 'output', tone: 'dim', text: '    Finished `dev` profile in 0.57s', delay: 460},
  {kind: 'output', tone: 'ok', text: 'Broker registered with NameServer', delay: 560, event: 'broker'},
  {kind: 'command', text: 'cargo run -p rocketmq-client-rust --example producer'},
  ...Array.from(
    {length: SEND_COUNT},
    (_, index): ScriptStep => ({
      kind: 'output',
      tone: 'send',
      text: `TopicTest  queue=${index % QUEUE_COUNT}  offset=${Math.floor(index / QUEUE_COUNT)}`,
      delay: index === 0 ? 620 : 150,
      event: 'send',
    }),
  ),
];

function toLines(steps: ScriptStep[]): TerminalLine[] {
  return steps.map((step, id) => ({id, kind: step.kind, tone: step.kind === 'output' ? step.tone : undefined, text: step.text}));
}

function StatusChip({
  className,
  name,
  detail,
  online,
}: {
  className: string;
  name: string;
  detail: string;
  online: boolean;
}): React.JSX.Element {
  return (
    <div className={clsx(styles.chip, className, online && styles.chipOnline)}>
      <span className={styles.chipDot} />
      <span className={styles.chipText}>
        <strong>{name}</strong>
        <small>{detail}</small>
      </span>
      <span className={styles.chipState}>
        {online
          ? translate({id: 'homepage.hero.chip.online', message: 'online'})
          : translate({id: 'homepage.hero.chip.waiting', message: 'waiting'})}
      </span>
    </div>
  );
}

/** Terminal that replays the local quick start, with status chips that light up as each service comes online. */
export default function HeroTerminal(): React.JSX.Element {
  const reducedMotion = usePrefersReducedMotion();
  const [rootRef, inView] = useInView<HTMLDivElement>({threshold: 0.15});
  const bodyRef = useRef<HTMLDivElement>(null);
  const [lines, setLines] = useState<TerminalLine[]>([]);
  const [typing, setTyping] = useState('');
  const [cluster, setCluster] = useState<ClusterState>(IDLE_CLUSTER);

  useEffect(() => {
    if (reducedMotion) {
      // No replay: show the finished session.
      setLines(toLines(SCRIPT));
      setTyping('');
      setCluster({namesrv: true, broker: true, sent: SEND_COUNT});
      return undefined;
    }
    if (!inView) {
      return undefined;
    }

    let cancelled = false;
    const timers = new Set<number>();
    const wait = (ms: number): Promise<void> =>
      new Promise((resolve) => {
        const timer = window.setTimeout(() => {
          timers.delete(timer);
          resolve();
        }, ms);
        timers.add(timer);
      });

    const play = async (): Promise<void> => {
      let nextId = 0;
      while (!cancelled) {
        setLines([]);
        setTyping('');
        setCluster(IDLE_CLUSTER);
        await wait(700);

        for (const step of SCRIPT) {
          if (cancelled) {
            return;
          }
          const line: TerminalLine = {
            id: nextId,
            kind: step.kind,
            tone: step.kind === 'output' ? step.tone : undefined,
            text: step.text,
          };
          nextId += 1;

          if (step.kind === 'command') {
            for (let length = 1; length <= step.text.length; length += 1) {
              setTyping(step.text.slice(0, length));
              await wait(16 + Math.random() * 34);
              if (cancelled) {
                return;
              }
            }
            await wait(320);
            setTyping('');
          } else {
            await wait(step.delay);
            const {event} = step;
            if (event) {
              setCluster((current) =>
                event === 'send' ? {...current, sent: current.sent + 1} : {...current, [event]: true},
              );
            }
          }
          if (cancelled) {
            return;
          }
          setLines((current) => [...current.slice(-40), line]);
        }
        await wait(LOOP_PAUSE_MS);
      }
    };
    void play();

    return () => {
      cancelled = true;
      timers.forEach((timer) => window.clearTimeout(timer));
    };
  }, [inView, reducedMotion]);

  // Keep the newest line visible, like a real terminal.
  useEffect(() => {
    const body = bodyRef.current;
    if (body) {
      body.scrollTop = body.scrollHeight;
    }
  }, [lines, typing]);

  const flowing = cluster.sent > 0 && cluster.sent < SEND_COUNT;

  return (
    <div ref={rootRef} className={styles.root}>
      <div className={styles.glow} aria-hidden="true" />

      <div
        className={styles.frame}
        role="img"
        aria-label={translate({
          id: 'homepage.hero.terminal.label',
          message:
            'Terminal replay: start a NameServer, start a Broker, then send messages to TopicTest with the Rust client.',
        })}>
        <div className={styles.window} aria-hidden="true">
          <div className={styles.bar}>
            <span className={styles.lights}>
              <i />
              <i />
              <i />
            </span>
            <span className={styles.barTitle}>rocketmq-rust — zsh</span>
            <span className={styles.barBadge}>v1.0.0</span>
          </div>

          <div ref={bodyRef} className={styles.body}>
            {lines.map((line) =>
              line.kind === 'command' ? (
                <div key={line.id} className={styles.line}>
                  <span className={styles.prompt}>$</span>
                  <span className={styles.command}>{line.text}</span>
                </div>
              ) : (
                <div key={line.id} className={clsx(styles.line, styles.output, line.tone && styles[line.tone])}>
                  {line.tone === 'ok' && <span className={styles.mark}>✓</span>}
                  {line.tone === 'send' && <span className={styles.tag}>SEND_OK</span>}
                  <span>{line.text}</span>
                </div>
              ),
            )}
            <div className={styles.line}>
              <span className={styles.prompt}>$</span>
              <span className={styles.command}>{typing}</span>
              <span className={styles.caret} />
            </div>
          </div>
        </div>
      </div>

      <div className={styles.chips} aria-hidden="true">
        <StatusChip className={styles.chipNameServer} name="NameServer" detail=":9876" online={cluster.namesrv} />
        <StatusChip className={styles.chipBroker} name="Broker" detail=":10911" online={cluster.broker} />
        <div className={clsx(styles.chip, styles.chipTopic, cluster.sent > 0 && styles.chipOnline)}>
          <span className={clsx(styles.bars, flowing && styles.barsFlowing)}>
            <i />
            <i />
            <i />
            <i />
            <i />
          </span>
          <span className={styles.chipText}>
            <strong>TopicTest</strong>
            <small>{translate({id: 'homepage.hero.chip.messages', message: 'messages sent'})}</small>
          </span>
          <span className={styles.chipCount}>{cluster.sent}</span>
        </div>
      </div>
    </div>
  );
}
