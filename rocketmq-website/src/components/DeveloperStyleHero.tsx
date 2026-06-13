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

const heroMetrics = [
  {
    value: 'Tokio',
    labelId: 'homepage.hero.metric.runtime',
    label: 'async runtime',
  },
  {
    value: 'CommitLog',
    labelId: 'homepage.hero.metric.storage',
    label: 'durable storage',
  },
  {
    value: '5.x',
    labelId: 'homepage.hero.metric.model',
    label: 'RocketMQ model',
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

function LaunchArtwork(): React.JSX.Element {
  const streamInstanceId = React.useId().replace(/:/g, '');
  const streamGradientId = `stream-core-gradient-${streamInstanceId}`;
  const streamRoutePrimaryId = `stream-route-primary-${streamInstanceId}`;
  const streamRouteSecondaryId = `stream-route-secondary-${streamInstanceId}`;
  const streamRouteTertiaryId = `stream-route-tertiary-${streamInstanceId}`;

  return (
    <div className={styles.launchArtwork} aria-hidden="true">
      <div className={clsx(styles.launchPanel, styles.launchPanelLeft, styles.streamPanel)}>
        <svg className={styles.streamSvg} viewBox="0 0 420 520" role="presentation">
          <defs>
            <linearGradient id={streamGradientId} x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="#18d7f5" />
              <stop offset="48%" stopColor="#9b6cff" />
              <stop offset="100%" stopColor="#ff7a1a" />
            </linearGradient>
          </defs>

          <g className={styles.streamBackplane}>
            <path d="M78 132H312" />
            <path d="M54 214H352" />
            <path d="M82 296H322" />
            <path d="M112 88V372" />
            <path d="M210 66V424" />
            <path d="M308 104V396" />
          </g>

          <g className={styles.streamHalo}>
            <ellipse cx="210" cy="258" rx="142" ry="92" />
            <ellipse cx="210" cy="258" rx="92" ry="58" />
          </g>

          <path
            id={streamRoutePrimaryId}
            className={clsx(styles.streamRoute, styles.streamRoutePrimary)}
            d="M46 162C104 126 154 154 187 205C207 236 234 252 286 236C322 225 350 236 374 262"
          />
          <path
            id={streamRouteSecondaryId}
            className={clsx(styles.streamRoute, styles.streamRouteSecondary)}
            d="M42 302C98 330 149 323 181 286C213 248 248 224 314 192C338 180 358 160 376 130"
          />
          <path
            id={streamRouteTertiaryId}
            className={clsx(styles.streamRoute, styles.streamRouteTertiary)}
            d="M70 244C120 244 151 244 183 244C217 244 248 272 276 318C296 350 322 364 358 360"
          />

          <g className={styles.streamCore}>
            <rect x="165" y="214" width="96" height="96" rx="24" fill={`url(#${streamGradientId})`} />
            <path d="M190 242H236M190 262H222M190 282H238" />
          </g>

          <g className={styles.streamEndpoints}>
            <circle cx="46" cy="162" r="5" />
            <circle cx="42" cy="302" r="5" />
            <circle cx="358" cy="360" r="5" />
            <circle cx="376" cy="130" r="4" />
          </g>

          <g className={styles.streamPackets}>
            <circle className={clsx(styles.streamPacket, styles.streamPacketPrimary)} r="5">
              <animateMotion dur="5.4s" repeatCount="indefinite" begin="0s">
                <mpath href={`#${streamRoutePrimaryId}`} />
              </animateMotion>
            </circle>
            <circle className={clsx(styles.streamPacket, styles.streamPacketSecondary)} r="4">
              <animateMotion dur="6.2s" repeatCount="indefinite" begin="-1.8s">
                <mpath href={`#${streamRouteSecondaryId}`} />
              </animateMotion>
            </circle>
            <circle className={clsx(styles.streamPacket, styles.streamPacketTertiary)} r="4">
              <animateMotion dur="5.8s" repeatCount="indefinite" begin="-3s">
                <mpath href={`#${streamRouteTertiaryId}`} />
              </animateMotion>
            </circle>
          </g>
        </svg>
      </div>
      <div className={clsx(styles.launchPanel, styles.launchPanelRight, styles.orbitPanel)}>
        <span className={styles.orbitHorizon} />
        <span className={styles.orbitArc} />
        <span className={styles.orbitArcSecondary} />
        <span className={styles.orbitPacketOne} />
        <span className={styles.orbitPacketTwo} />
        <span className={styles.orbitPacketThree} />
      </div>
    </div>
  );
}

function HeroMetrics(): React.JSX.Element {
  return (
    <div className={styles.heroMetrics} aria-label="RocketMQ-Rust implementation highlights">
      {heroMetrics.map((metric) => (
        <div key={metric.value} className={styles.heroMetric}>
          <strong>{metric.value}</strong>
          <span>
            {translate({
              id: metric.labelId,
              message: metric.label,
            })}
          </span>
        </div>
      ))}
    </div>
  );
}

function SignalRail(): React.JSX.Element {
  return (
    <div className={styles.signalRail} aria-hidden="true">
      <div className={styles.signalHeader}>
        <span>topic: orders.created</span>
        <strong>live flow</strong>
      </div>
      <div className={styles.signalRows}>
        <span style={{'--delay': '0s'} as React.CSSProperties} />
        <span style={{'--delay': '-0.7s'} as React.CSSProperties} />
        <span style={{'--delay': '-1.4s'} as React.CSSProperties} />
        <span style={{'--delay': '-2.1s'} as React.CSSProperties} />
      </div>
      <div className={styles.signalFooter}>
        <span>route</span>
        <span>append</span>
        <span>dispatch</span>
      </div>
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
      <LaunchArtwork />
      <ParticleField />

      <div className={styles.heroInner}>
        <div className={styles.heroBody}>
          <div className={styles.heroCopy}>
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
              <span className={styles.titleBrand}>
                <span>Rocket</span>
                <span className={styles.titleMq}>MQ</span>
              </span>
              <span className={styles.titleRust}>-Rust</span>
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

            <HeroMetrics />
          </div>

          <div className={styles.heroVisualColumn}>
            <RuntimeVisual />
            <SignalRail />
          </div>
        </div>

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
