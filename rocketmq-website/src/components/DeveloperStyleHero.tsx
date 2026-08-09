import React from 'react';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import clsx from 'clsx';
import styles from './DeveloperStyleHero.module.css';

type MetricTone = 'runtime' | 'safety' | 'protocol' | 'status';

type MetricItem = {
  value: string;
  detailId: string;
  detail: string;
  tone: MetricTone;
};

const metrics: MetricItem[] = [
  {
    value: 'Tokio',
    detailId: 'homepage.hero.metric.tokio',
    detail: 'Asynchronous by design',
    tone: 'runtime',
  },
  {
    value: 'Type Safe',
    detailId: 'homepage.hero.metric.safety',
    detail: 'Memory safety without compromise',
    tone: 'safety',
  },
  {
    value: 'RocketMQ 5.x',
    detailId: 'homepage.hero.metric.protocol',
    detail: 'Protocol compatible',
    tone: 'protocol',
  },
];

function ArrowIcon(): React.JSX.Element {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true">
      <path d="M4 10h11M11 5l5 5-5 5" />
    </svg>
  );
}

function GitHubIcon(): React.JSX.Element {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 2a10 10 0 0 0-3.16 19.49c.5.09.68-.22.68-.48v-1.86c-2.78.6-3.37-1.18-3.37-1.18-.45-1.16-1.11-1.47-1.11-1.47-.91-.62.07-.61.07-.61 1 .07 1.53 1.03 1.53 1.03.9 1.53 2.35 1.09 2.92.83.09-.65.35-1.09.64-1.34-2.22-.25-4.55-1.11-4.55-4.94 0-1.09.39-1.98 1.03-2.68-.1-.25-.45-1.27.1-2.64 0 0 .84-.27 2.75 1.02A9.6 9.6 0 0 1 12 6.84a9.5 9.5 0 0 1 2.5.34c1.91-1.3 2.75-1.02 2.75-1.02.55 1.37.2 2.39.1 2.64.64.7 1.03 1.59 1.03 2.68 0 3.84-2.34 4.68-4.57 4.93.36.31.68.92.68 1.86V21c0 .27.18.58.69.48A10 10 0 0 0 12 2Z" />
    </svg>
  );
}

function MetricIcon({tone}: {tone: MetricTone}): React.JSX.Element {
  if (tone === 'runtime') {
    return (
      <svg viewBox="0 0 36 36" aria-hidden="true">
        <path d="m20 2-12 18h9l-2 14 13-20h-9l1-12Z" />
      </svg>
    );
  }

  if (tone === 'safety') {
    return (
      <svg viewBox="0 0 36 36" aria-hidden="true">
        <path d="M18 3 31 8v9c0 8-5 13-13 16C10 30 5 25 5 17V8l13-5Z" />
        <path d="m12 18 4 4 8-9" />
      </svg>
    );
  }

  if (tone === 'protocol') {
    return (
      <svg viewBox="0 0 36 36" aria-hidden="true">
        <path d="m18 3 13 7-13 7L5 10l13-7Z" />
        <path d="m5 10 13 7 13-7v16l-13 7-13-7V10Z" />
        <path d="M18 17v16" />
      </svg>
    );
  }

  return <span className={styles.statusDot} aria-hidden="true" />;
}

function TopologyVisual(): React.JSX.Element {
  const instanceId = React.useId().replace(/:/g, '');
  const producerRouteId = `producer-route-${instanceId}`;
  const brokerRouteId = `broker-route-${instanceId}`;
  const consumerRouteId = `consumer-route-${instanceId}`;
  const storageRouteId = `storage-route-${instanceId}`;

  return (
    <div className={styles.topologyPanel}>
      <div className={styles.visualStatus}>
        <span />
        {translate({
          id: 'homepage.hero.clusterStatus',
          message: 'Cluster: healthy',
        })}
      </div>

      <svg
        className={styles.topologySvg}
        viewBox="0 0 760 470"
        role="img"
        aria-labelledby={`topology-title-${instanceId}`}>
        <title id={`topology-title-${instanceId}`}>
          RocketMQ-Rust producer, NameServer, broker, CommitLog, and consumer topology
        </title>
        <defs>
          <linearGradient id={`broker-gradient-${instanceId}`} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#ff9a3d" />
            <stop offset="100%" stopColor="#ff5a18" />
          </linearGradient>
          <filter id={`soft-glow-${instanceId}`} x="-80%" y="-80%" width="260%" height="260%">
            <feGaussianBlur stdDeviation="4" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        <g className={styles.topologyGrid} aria-hidden="true">
          <path d="M20 70H740M20 150H740M20 230H740M20 310H740M20 390H740" />
          <path d="M80 22V448M200 22V448M320 22V448M440 22V448M560 22V448M680 22V448" />
        </g>

        <g className={styles.routeTracks} aria-hidden="true">
          <path
            className={clsx(styles.route, styles.routeDiscovery)}
            d="M190 205C242 205 238 91 305 91"
          />
          <path
            className={clsx(styles.route, styles.routeDiscovery)}
            d="M455 91C520 91 518 205 570 205"
          />
          <path
            id={producerRouteId}
            className={clsx(styles.route, styles.routeProducer)}
            d="M190 240C236 240 260 234 305 232"
          />
          <path
            id={brokerRouteId}
            className={clsx(styles.route, styles.routeLookup)}
            d="M380 138V174"
          />
          <path
            id={consumerRouteId}
            className={clsx(styles.route, styles.routeConsumer)}
            d="M455 232C498 232 526 240 570 240"
          />
          <path
            id={storageRouteId}
            className={clsx(styles.route, styles.routeStorage)}
            d="M380 290V330"
          />
        </g>

        <g className={styles.packetLayer} aria-hidden="true">
          <circle className={clsx(styles.packet, styles.packetProducer)} r="5">
            <animateMotion dur="3.2s" repeatCount="indefinite">
              <mpath href={`#${producerRouteId}`} />
            </animateMotion>
          </circle>
          <circle className={clsx(styles.packet, styles.packetLookup)} r="4">
            <animateMotion dur="2.8s" repeatCount="indefinite" begin="-1.1s">
              <mpath href={`#${brokerRouteId}`} />
            </animateMotion>
          </circle>
          <circle className={clsx(styles.packet, styles.packetConsumer)} r="5">
            <animateMotion dur="3.4s" repeatCount="indefinite" begin="-1.7s">
              <mpath href={`#${consumerRouteId}`} />
            </animateMotion>
          </circle>
          <circle className={clsx(styles.packet, styles.packetStorage)} r="5">
            <animateMotion dur="2.6s" repeatCount="indefinite" begin="-0.8s">
              <mpath href={`#${storageRouteId}`} />
            </animateMotion>
          </circle>
        </g>

        <g className={clsx(styles.topologyNode, styles.producerNode)} transform="translate(35 180)">
          <rect width="155" height="116" rx="18" />
          <text className={styles.nodeLabel} x="77.5" y="28">Producer</text>
          <g className={styles.nodeIcon} transform="translate(54 43)">
            <path d="m23 0 23 13v26L23 52 0 39V13L23 0Z" />
            <path d="m0 13 23 13 23-13M23 26v26" />
          </g>
          <circle className={styles.port} cx="155" cy="25" r="4" />
          <circle className={styles.port} cx="155" cy="60" r="4" />
        </g>

        <g className={clsx(styles.topologyNode, styles.nameServerNode)} transform="translate(305 38)">
          <rect width="150" height="100" rx="18" />
          <text className={styles.nodeLabel} x="75" y="28">NameServer</text>
          <g className={styles.nodeIcon} transform="translate(53 43)">
            <path d="M22 0 44 12v25L22 49 0 37V12L22 0Z" />
            <circle cx="22" cy="14" r="4" />
            <circle cx="11" cy="33" r="4" />
            <circle cx="33" cy="33" r="4" />
            <path d="m20 18-7 11m11-11 7 11M15 33h14" />
          </g>
          <circle className={styles.port} cx="0" cy="53" r="4" />
          <circle className={styles.port} cx="75" cy="100" r="4" />
          <circle className={styles.port} cx="150" cy="53" r="4" />
        </g>

        <g className={clsx(styles.topologyNode, styles.brokerNode)} transform="translate(305 174)">
          <rect width="150" height="116" rx="18" />
          <text className={styles.nodeLabel} x="75" y="28">Broker</text>
          <g className={styles.nodeIcon} transform="translate(50 43)">
            <path d="m25 0 25 13-25 13L0 13 25 0Z" />
            <path d="m0 24 25 13 25-13M0 35l25 13 25-13" />
          </g>
          <circle className={styles.port} cx="0" cy="58" r="4" />
          <circle className={styles.port} cx="75" cy="0" r="4" />
          <circle className={styles.port} cx="150" cy="58" r="4" />
          <circle className={styles.port} cx="75" cy="116" r="4" />
        </g>

        <g className={clsx(styles.topologyNode, styles.consumerNode)} transform="translate(570 180)">
          <rect width="155" height="116" rx="18" />
          <text className={styles.nodeLabel} x="77.5" y="28">Consumer</text>
          <g className={styles.nodeIcon} transform="translate(51 43)">
            <circle cx="26" cy="11" r="9" />
            <circle cx="8" cy="15" r="6" />
            <circle cx="44" cy="15" r="6" />
            <path d="M13 48V39c0-8 5-13 13-13s13 5 13 13v9M0 48v-7c0-6 3-10 9-11m43 18v-7c0-6-3-10-9-11" />
          </g>
          <circle className={styles.port} cx="0" cy="25" r="4" />
          <circle className={styles.port} cx="0" cy="60" r="4" />
        </g>

        <g className={clsx(styles.topologyNode, styles.commitLogNode)} transform="translate(305 330)">
          <rect width="150" height="104" rx="18" />
          <text className={styles.nodeLabel} x="75" y="28">CommitLog</text>
          <g className={styles.nodeIcon} transform="translate(50 48)">
            <ellipse cx="25" cy="7" rx="24" ry="7" />
            <path d="M1 7v28c0 4 11 7 24 7s24-3 24-7V7M1 21c0 4 11 7 24 7s24-3 24-7" />
          </g>
          <circle className={styles.port} cx="75" cy="0" r="4" />
        </g>
      </svg>
    </div>
  );
}

function MetricsRail(): React.JSX.Element {
  return (
    <div className={styles.metricsRail} aria-label="RocketMQ-Rust platform highlights">
      {metrics.map((metric) => (
        <div key={metric.value} className={clsx(styles.metric, styles[metric.tone])}>
          <span className={styles.metricIcon}>
            <MetricIcon tone={metric.tone} />
          </span>
          <span>
            <strong>{metric.value}</strong>
            <small>
              {translate({
                id: metric.detailId,
                message: metric.detail,
              })}
            </small>
          </span>
        </div>
      ))}

      <div className={clsx(styles.metric, styles.status)}>
        <span className={styles.metricIcon}>
          <MetricIcon tone="status" />
        </span>
        <span>
          <strong>
            {translate({
              id: 'homepage.hero.systemStatus',
              message: 'All Systems Operational',
            })}
          </strong>
          <small>
            {translate({
              id: 'homepage.hero.systemStatus.detail',
              message: 'Open-source infrastructure, ready to run',
            })}
          </small>
        </span>
      </div>
    </div>
  );
}

export default function DeveloperStyleHero(): React.JSX.Element {
  return (
    <header className={styles.hero}>
        <div className={styles.heroInner}>
          <div className={styles.heroBody}>
            <div className={styles.heroCopy}>
              <div className={styles.eyebrow}>
                <span />
                {translate({
                  id: 'homepage.hero.eyebrow',
                  message: 'OPEN SOURCE · APACHE ROCKETMQ · RUST',
                })}
              </div>

              <h1 className={styles.title}>
                Rocket<span>MQ</span>-Rust
              </h1>

              <p className={styles.subheadline}>
                {translate({
                  id: 'homepage.hero.subheadline',
                  message: 'High-performance messaging middleware built with Rust',
                })}
              </p>

              <div className={styles.ctaRow}>
                <Link className={clsx(styles.button, styles.buttonPrimary)} to="/docs/introduction">
                  {translate({
                    id: 'homepage.hero.getStarted',
                    message: 'Get Started',
                  })}
                  <ArrowIcon />
                </Link>
                <Link
                  className={clsx(styles.button, styles.buttonSecondary)}
                  to="https://github.com/mxsm/rocketmq-rust">
                  <GitHubIcon />
                  {translate({
                    id: 'homepage.hero.githubCta',
                    message: 'View on GitHub',
                  })}
                </Link>
              </div>
            </div>

            <div className={styles.heroVisual}>
              <TopologyVisual />
            </div>
          </div>

          <MetricsRail />
        </div>
    </header>
  );
}
