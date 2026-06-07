import React from 'react';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import clsx from 'clsx';
import styles from './DeveloperStyleHero.module.css';

const statusItems = [
  {
    titleId: 'homepage.hero.status.nameserver.title',
    title: 'NameServer',
    detailId: 'homepage.hero.status.nameserver.detail',
    detail: 'route discovery',
  },
  {
    titleId: 'homepage.hero.status.broker.title',
    title: 'Broker',
    detailId: 'homepage.hero.status.broker.detail',
    detail: 'commit + dispatch',
  },
  {
    titleId: 'homepage.hero.status.storage.title',
    title: 'CommitLog',
    detailId: 'homepage.hero.status.storage.detail',
    detail: 'durable storage',
  },
  {
    titleId: 'homepage.hero.status.client.title',
    title: 'Rust Client',
    detailId: 'homepage.hero.status.client.detail',
    detail: 'producer + consumer',
  },
];

const motionStages = [
  {
    id: 'homepage.hero.stage.route',
    message: 'Route Lookup',
  },
  {
    id: 'homepage.hero.stage.produce',
    message: 'Producer Send',
  },
  {
    id: 'homepage.hero.stage.append',
    message: 'Broker Append',
  },
  {
    id: 'homepage.hero.stage.commitlog',
    message: 'CommitLog Flush',
  },
  {
    id: 'homepage.hero.stage.consume',
    message: 'Consumer Pull',
  },
  {
    id: 'homepage.hero.stage.offset',
    message: 'Offset Commit',
  },
];

function ParticleField(): React.JSX.Element {
  return (
    <div className={styles.particleField} aria-hidden="true">
      <span className={clsx(styles.particleNode, styles.particleOne)} />
      <span className={clsx(styles.particleNode, styles.particleTwo)} />
      <span className={clsx(styles.particleNode, styles.particleThree)} />
      <span className={clsx(styles.particleNode, styles.particleFour)} />
      <span className={clsx(styles.particleNode, styles.particleFive)} />
      <span className={clsx(styles.particleLink, styles.linkOne)} />
      <span className={clsx(styles.particleLink, styles.linkTwo)} />
      <span className={clsx(styles.particleLink, styles.linkThree)} />
      <span className={clsx(styles.particleLink, styles.linkFour)} />
      <span className={clsx(styles.particleLink, styles.linkFive)} />
      <span className={clsx(styles.particleLink, styles.linkSix)} />
    </div>
  );
}

function RuntimeVisual(): React.JSX.Element {
  const instanceId = React.useId().replace(/:/g, '');
  const gradientId = `mq-core-gradient-${instanceId}`;
  const producePathId = `mq-produce-path-${instanceId}`;
  const routePathId = `mq-route-path-${instanceId}`;
  const commitPathId = `mq-commit-path-${instanceId}`;
  const deliverPathId = `mq-deliver-path-${instanceId}`;

  return (
    <div className={styles.runtimeVisual} aria-label="RocketMQ-Rust runtime topology">
      <div className={styles.runtimeChrome} aria-hidden="true">
        <div className={styles.windowControls}>
          <span />
          <span />
          <span />
        </div>
        <span className={styles.runtimeTitle}>rocketmq-rust / broker topology</span>
        <span className={styles.runtimeBadge}>message flow</span>
      </div>

      <div className={styles.runtimeCanvas}>
        <svg className={styles.runtimeSvg} viewBox="0 0 720 390" role="presentation">
          <defs>
            <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="#9b6cff" />
              <stop offset="52%" stopColor="#d858b8" />
              <stop offset="100%" stopColor="#ff7a1a" />
            </linearGradient>
          </defs>

          <g className={styles.runtimeGrid}>
            <path d="M70 70H650" />
            <path d="M70 150H650" />
            <path d="M70 230H650" />
            <path d="M70 310H650" />
            <path d="M140 38V360" />
            <path d="M280 38V360" />
            <path d="M420 38V360" />
            <path d="M560 38V360" />
          </g>

          <g className={styles.runtimeOrbits}>
            <ellipse cx="360" cy="205" rx="158" ry="128" />
            <ellipse cx="360" cy="205" rx="102" ry="82" />
            <path d="M234 118C288 68 433 67 492 121" />
            <path d="M226 296C291 358 459 350 513 281" />
          </g>

          <path id={producePathId} className={clsx(styles.flowTrack, styles.flowProduce)} d="M170 168C232 167 272 182 310 204" />
          <path id={routePathId} className={clsx(styles.flowTrack, styles.flowRoute)} d="M364 93C344 121 337 153 346 180" />
          <path id={commitPathId} className={clsx(styles.flowTrack, styles.flowCommit)} d="M374 244C407 275 444 293 493 301" />
          <path id={deliverPathId} className={clsx(styles.flowTrack, styles.flowDelivery)} d="M426 206C479 188 518 171 582 166" />

          <g className={clsx(styles.svgNode, styles.producerNode)} transform="translate(58 128)">
            <rect width="152" height="74" rx="16" />
            <text x="20" y="31">Producer</text>
            <text className={styles.svgNodeMeta} x="20" y="53">batch send</text>
          </g>

          <g className={clsx(styles.svgNode, styles.nameServerNode)} transform="translate(288 50)">
            <rect width="168" height="76" rx="16" />
            <text x="20" y="32">NameServer</text>
            <text className={styles.svgNodeMeta} x="20" y="54">route table</text>
          </g>

          <g className={clsx(styles.svgNode, styles.consumerNode)} transform="translate(538 128)">
            <rect width="154" height="74" rx="16" />
            <text x="20" y="31">Consumer</text>
            <text className={styles.svgNodeMeta} x="20" y="53">pull consume</text>
          </g>

          <g className={clsx(styles.svgNode, styles.storageNode)} transform="translate(98 286)">
            <rect width="166" height="74" rx="16" />
            <text x="20" y="31">CommitLog</text>
            <text className={styles.svgNodeMeta} x="20" y="53">durable store</text>
          </g>

          <g className={clsx(styles.svgNode, styles.brokerNode)} transform="translate(468 286)">
            <rect width="168" height="74" rx="16" />
            <text x="20" y="31">Broker</text>
            <text className={styles.svgNodeMeta} x="20" y="53">dispatch queue</text>
          </g>

          <g className={styles.coreNode} transform="translate(292 154)">
            <rect width="136" height="136" rx="34" fill={`url(#${gradientId})`} />
            <text x="68" y="79">MQ</text>
            <text className={styles.coreCaption} x="68" y="102">rust core</text>
          </g>

          <g className={styles.flowPackets}>
            <circle className={clsx(styles.flowOrb, styles.produceOrb)} r="6">
              <animateMotion dur="4.8s" repeatCount="indefinite" begin="0s">
                <mpath href={`#${producePathId}`} />
              </animateMotion>
            </circle>
            <circle className={clsx(styles.flowOrb, styles.routeOrb)} r="5">
              <animateMotion dur="5.6s" repeatCount="indefinite" begin="-1.4s">
                <mpath href={`#${routePathId}`} />
              </animateMotion>
            </circle>
            <circle className={clsx(styles.flowOrb, styles.commitOrb)} r="5">
              <animateMotion dur="5.2s" repeatCount="indefinite" begin="-2.2s">
                <mpath href={`#${commitPathId}`} />
              </animateMotion>
            </circle>
            <circle className={clsx(styles.flowOrb, styles.deliverOrb)} r="6">
              <animateMotion dur="4.6s" repeatCount="indefinite" begin="-3s">
                <mpath href={`#${deliverPathId}`} />
              </animateMotion>
            </circle>
          </g>
        </svg>

        <div className={styles.runtimeReadout} aria-hidden="true">
          <span className={styles.promptDot} />
          <code>broker.append_commit_log(message)</code>
          <span className={styles.readoutPulse}>committed</span>
        </div>
      </div>

    </div>
  );
}

function StageTimeline(): React.JSX.Element {
  return (
    <div className={styles.stageTimeline} aria-label="RocketMQ message lifecycle">
      {motionStages.map((stage, index) => (
        <div key={stage.id} className={styles.stageItem}>
          <span>{String(index).padStart(2, '0')}</span>
          <strong>
            {translate({
              id: stage.id,
              message: stage.message,
            })}
          </strong>
        </div>
      ))}
    </div>
  );
}

export default function DeveloperStyleHero(): React.JSX.Element {
  return (
    <header className={styles.hero}>
      <ParticleField />

      <div className={styles.heroInner}>
        <div className={styles.livePill}>
          <span className={styles.liveDot} />
          <span>
            {translate({
              id: 'homepage.hero.pill',
              message: 'APACHE ROCKETMQ ARCHITECTURE - RUST IMPLEMENTATION - ASYNC RUNTIME',
            })}
          </span>
        </div>

        <h1 className={styles.title}>
          <span>Rocket</span>
          <span className={styles.titleMq}>MQ</span>
          <span>-Rust</span>
        </h1>

        <p className={styles.subheadline}>
          {translate({
            id: 'homepage.hero.subheadline',
            message: 'High-performance messaging middleware built with Rust',
          })}
        </p>

        <p className={styles.description}>
          {translate({
            id: 'homepage.hero.description',
            message:
              "RocketMQ-Rust brings Apache RocketMQ's proven messaging model to Rust: producers send messages to brokers, NameServer provides routing, CommitLog persists data, and consumers process messages through async, type-safe APIs.",
          })}
        </p>

        <div className={styles.ctaRow}>
          <Link className={clsx(styles.button, styles.buttonPrimary)} to="/docs/introduction">
            {translate({
              id: 'homepage.hero.getStarted',
              message: 'Get Started',
            })}
            <span aria-hidden="true">-&gt;</span>
          </Link>
          <Link className={clsx(styles.button, styles.buttonSecondary)} to="https://github.com/mxsm/rocketmq-rust">
            {translate({
              id: 'homepage.hero.github',
              message: 'GitHub',
            })}
            <span aria-hidden="true">-&gt;</span>
          </Link>
          <a className={clsx(styles.button, styles.buttonGhost)} href="#motion-system">
            {translate({
              id: 'homepage.hero.motionSpec',
              message: 'Architecture flow',
            })}
            <span aria-hidden="true">-&gt;</span>
          </a>
        </div>

        <RuntimeVisual />
        <StageTimeline />

        <div className={styles.statusStrip} aria-label="RocketMQ-Rust runtime modules">
          {statusItems.map((item) => (
            <div key={item.title} className={styles.statusItem}>
              <strong>
                {translate({
                  id: item.titleId,
                  message: item.title,
                })}
              </strong>
              <span>
                {translate({
                  id: item.detailId,
                  message: item.detail,
                })}
              </span>
            </div>
          ))}
        </div>
      </div>
    </header>
  );
}
