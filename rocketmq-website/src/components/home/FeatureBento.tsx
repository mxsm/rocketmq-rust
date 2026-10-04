import React, {useCallback, useEffect, useId, useRef} from 'react';
import clsx from 'clsx';
import {translate} from '@docusaurus/Translate';
import {
  AsyncVisual,
  ObservabilityVisual,
  PatternsVisual,
  ProtocolVisual,
  QuorumVisual,
  SafetyVisual,
  SecurityVisual,
  StorageVisual,
} from './FeatureVisuals';
import SectionHeading from './SectionHeading';
import {
  ActivityIcon,
  BoltIcon,
  LayersIcon,
  LockIcon,
  PlugIcon,
  QuorumIcon,
  ShieldIcon,
  ShuffleIcon,
} from './icons';
import {Reveal} from './motion';
import primitives from './primitives.module.css';
import styles from './FeatureBento.module.css';

type Accent = 'orange' | 'violet' | 'cyan' | 'green' | 'rose' | 'amber';

type FeatureCardProps = {
  accent: Accent;
  icon: React.ReactNode;
  title: string;
  description: string;
  visual: React.ReactNode;
  wide?: boolean;
  index: number;
};

function FeatureCard({accent, icon, title, description, visual, wide, index}: FeatureCardProps): React.JSX.Element {
  return (
    <Reveal as="article" className={clsx(styles.card, styles[accent], wide && styles.cardWide)} delay={(index % 3) * 90}>
      <div className={styles.cardInner}>
        <div className={styles.visual} aria-hidden="true">
          {visual}
        </div>
        <div className={styles.copy}>
          <span className={styles.icon}>{icon}</span>
          <h3 className={styles.cardTitle}>{title}</h3>
          <p className={styles.cardText}>{description}</p>
        </div>
      </div>
    </Reveal>
  );
}

export default function FeatureBento(): React.JSX.Element {
  const headingId = useId();
  const gridRef = useRef<HTMLDivElement>(null);
  const pointer = useRef({x: 0, y: 0, frame: 0});

  useEffect(() => () => window.cancelAnimationFrame(pointer.current.frame), []);

  // One listener feeds every card its local pointer position for the spotlight border.
  const handlePointerMove = useCallback((event: React.PointerEvent<HTMLDivElement>) => {
    const state = pointer.current;
    state.x = event.clientX;
    state.y = event.clientY;
    if (state.frame) {
      return;
    }
    state.frame = window.requestAnimationFrame(() => {
      state.frame = 0;
      const cards = gridRef.current?.children;
      if (!cards) {
        return;
      }
      for (const card of Array.from(cards) as HTMLElement[]) {
        const bounds = card.getBoundingClientRect();
        card.style.setProperty('--spot-x', `${(state.x - bounds.left).toFixed(1)}px`);
        card.style.setProperty('--spot-y', `${(state.y - bounds.top).toFixed(1)}px`);
      }
    });
  }, []);

  // Shared with the rotating words in the hero.
  const patterns = [
    translate({id: 'homepage.pattern.ordered', message: 'ordered'}),
    translate({id: 'homepage.pattern.delayed', message: 'delayed'}),
    translate({id: 'homepage.pattern.transactional', message: 'transactional'}),
    translate({id: 'homepage.pattern.batched', message: 'batched'}),
    translate({id: 'homepage.pattern.requestReply', message: 'request-reply'}),
  ];

  const features: Array<Omit<FeatureCardProps, 'index'>> = [
    {
      accent: 'orange',
      wide: true,
      icon: <ShieldIcon />,
      title: translate({id: 'homepage.why.safety.title', message: 'Memory safety without a garbage collector'}),
      description: translate({
        id: 'homepage.why.safety.text',
        message:
          'Ownership and borrowing rule out data races and use-after-free at compile time. With no GC, there are no stop-the-world pauses on the message path.',
      }),
      visual: (
        <SafetyVisual
          verdict={translate({
            id: 'homepage.why.safety.verdict',
            message: 'Caught by the compiler, not in production',
          })}
        />
      ),
    },
    {
      accent: 'cyan',
      icon: <BoltIcon />,
      title: translate({id: 'homepage.why.async.title', message: 'Async from socket to disk'}),
      description: translate({
        id: 'homepage.why.async.text',
        message:
          'Built on Tokio. Networking, request processing and background services are non-blocking, with owned runtimes and orderly shutdown.',
      }),
      visual: <AsyncVisual />,
    },
    {
      accent: 'violet',
      icon: <PlugIcon />,
      title: translate({id: 'homepage.why.protocol.title', message: 'Speaks RocketMQ'}),
      description: translate({
        id: 'homepage.why.protocol.text',
        message:
          'Implements the remoting protocol and the gRPC messaging API, so Rust and Java clients and servers interoperate at documented boundaries.',
      }),
      visual: <ProtocolVisual />,
    },
    {
      accent: 'green',
      icon: <LayersIcon />,
      title: translate({id: 'homepage.why.storage.title', message: 'Pluggable storage engines'}),
      description: translate({
        id: 'homepage.why.storage.text',
        message:
          'An append-only CommitLog with ConsumeQueue and index files, a RocksDB backend, and tiered storage for data beyond local disks.',
      }),
      visual: <StorageVisual />,
    },
    {
      accent: 'amber',
      icon: <ShuffleIcon />,
      title: translate({id: 'homepage.why.patterns.title', message: 'Every messaging pattern'}),
      description: translate({
        id: 'homepage.why.patterns.text',
        message:
          'Ordered, delayed, transactional, batch and request-reply messaging, consumed through Push, LitePull or POP.',
      }),
      visual: <PatternsVisual patterns={patterns} />,
    },
    {
      accent: 'violet',
      icon: <QuorumIcon />,
      title: translate({id: 'homepage.why.ha.title', message: 'High availability with Raft'}),
      description: translate({
        id: 'homepage.why.ha.text',
        message:
          'A Raft-based Controller coordinates master election and in-sync replicas across Broker groups.',
      }),
      visual: <QuorumVisual />,
    },
    {
      accent: 'rose',
      icon: <LockIcon />,
      title: translate({id: 'homepage.why.security.title', message: 'Security built in'}),
      description: translate({
        id: 'homepage.why.security.text',
        message: 'Authentication, ACL authorization and TLS guard client and administrative access.',
      }),
      visual: <SecurityVisual />,
    },
    {
      accent: 'cyan',
      icon: <ActivityIcon />,
      title: translate({id: 'homepage.why.observability.title', message: 'Observable by design'}),
      description: translate({
        id: 'homepage.why.observability.text',
        message: 'Prometheus metrics plus OpenTelemetry traces and logs over OTLP, enabled through opt-in build features.',
      }),
      visual: <ObservabilityVisual />,
    },
  ];

  return (
    <section className={clsx(primitives.section, styles.section)} aria-labelledby={headingId}>
      <div className={styles.glow} aria-hidden="true" />
      <div className={primitives.container}>
        <SectionHeading
          id={headingId}
          eyebrow={translate({id: 'homepage.why.eyebrow', message: 'Why RocketMQ-Rust'})}
          title={translate({id: 'homepage.why.title', message: 'Engineered for the hot path'})}
          lead={translate({
            id: 'homepage.why.lead',
            message:
              'The RocketMQ model you already know, rebuilt in a language that turns whole classes of runtime failures into compile errors.',
          })}
        />

        <div ref={gridRef} className={styles.grid} onPointerMove={handlePointerMove}>
          {features.map((feature, index) => (
            <FeatureCard key={feature.title} {...feature} index={index} />
          ))}
        </div>
      </div>
    </section>
  );
}
