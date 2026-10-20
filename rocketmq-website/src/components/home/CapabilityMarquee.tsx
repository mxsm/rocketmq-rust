import React from 'react';
import clsx from 'clsx';
import {translate} from '@docusaurus/Translate';
import styles from './CapabilityMarquee.module.css';

/** Libraries and platforms the workspace builds on or targets; proper nouns, not translated. */
const TECHNOLOGIES = [
  'Tokio',
  'Tonic gRPC',
  'OpenRaft',
  'RocksDB',
  'OpenTelemetry',
  'Prometheus',
  'Rustls',
  'Serde',
  'Ratatui',
  'Linux',
  'macOS',
  'Windows',
  'Docker',
  'Kubernetes',
  'crates.io',
];

function Row({items, variant}: {items: string[]; variant: 'pills' | 'plain'}): React.JSX.Element {
  const renderTrack = (hidden: boolean): React.JSX.Element => (
    <ul className={styles.track} aria-hidden={hidden || undefined}>
      {items.map((item) => (
        <li key={item} className={styles.item}>
          {item}
        </li>
      ))}
    </ul>
  );

  return (
    <div className={clsx(styles.row, styles[variant])}>
      {renderTrack(false)}
      {/* Second copy makes the loop seamless. */}
      {renderTrack(true)}
    </div>
  );
}

export default function CapabilityMarquee(): React.JSX.Element {
  const capabilities = [
    translate({id: 'homepage.marquee.ordered', message: 'Ordered messages'}),
    translate({id: 'homepage.marquee.delayed', message: 'Delayed delivery'}),
    translate({id: 'homepage.marquee.transactional', message: 'Transactional messages'}),
    translate({id: 'homepage.marquee.batch', message: 'Batch sending'}),
    translate({id: 'homepage.marquee.requestReply', message: 'Request-reply'}),
    translate({id: 'homepage.marquee.push', message: 'Push consumer'}),
    translate({id: 'homepage.marquee.litePull', message: 'LitePull consumer'}),
    translate({id: 'homepage.marquee.pop', message: 'POP consumption'}),
    translate({id: 'homepage.marquee.filter', message: 'Tag and SQL92 filtering'}),
    translate({id: 'homepage.marquee.acl', message: 'ACL authorization'}),
    translate({id: 'homepage.marquee.tls', message: 'TLS transport'}),
    translate({id: 'homepage.marquee.raft', message: 'Raft controller'}),
    translate({id: 'homepage.marquee.grpc', message: 'gRPC proxy'}),
    translate({id: 'homepage.marquee.tiered', message: 'Tiered storage'}),
  ];

  return (
    <section
      id="explore"
      className={styles.marquee}
      aria-label={translate({id: 'homepage.marquee.label', message: 'Capabilities and technology stack'})}>
      <Row items={capabilities} variant="pills" />
      <Row items={TECHNOLOGIES} variant="plain" />
    </section>
  );
}
