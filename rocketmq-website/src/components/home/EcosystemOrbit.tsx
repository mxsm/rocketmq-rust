import React, {useId, useState, type CSSProperties} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import useBaseUrl from '@docusaurus/useBaseUrl';
import SectionHeading from './SectionHeading';
import {ArrowRightIcon} from './icons';
import {Reveal} from './motion';
import primitives from './primitives.module.css';
import styles from './EcosystemOrbit.module.css';

type GroupId = 'services' | 'libraries' | 'operations';

type Ring = {
  id: GroupId;
  /** Orbit radius as a percentage of the diagram width. */
  radius: number;
  /**
   * Degrees added to every slot. The rings turn as one rigid body, so staggered
   * offsets guarantee labels on neighbouring rings never overlap.
   */
  offset: number;
  /** Seconds per comet lap; a negative value runs it counter-clockwise. */
  cometPeriod: number;
  /** Components placed on the ring. Proper nouns, not translated. */
  orbit: string[];
  /** Everything in the group, listed on its card. */
  members: string[];
};

const RINGS: Ring[] = [
  {
    id: 'services',
    radius: 24,
    offset: 0,
    cometPeriod: 8,
    orbit: ['NameServer', 'Broker', 'Proxy', 'Controller'],
    members: ['NameServer', 'Broker', 'Proxy', 'Controller'],
  },
  {
    id: 'libraries',
    radius: 37,
    offset: 45,
    cometPeriod: -12,
    orbit: ['Client', 'Protocol', 'Store', 'Runtime'],
    members: ['Client', 'Protocol', 'Transport', 'Model', 'Store', 'Runtime', 'Auth', 'Filter', 'Observability'],
  },
  {
    id: 'operations',
    radius: 50,
    offset: 22.5,
    cometPeriod: 17,
    orbit: [
      'Admin CLI',
      'Web Dashboard',
      'MCP',
      'GPUI Dashboard',
      'Admin TUI',
      'Tauri Dashboard',
      'AI SRE',
      'Store Inspect',
    ],
    members: [
      'Admin CLI',
      'Admin TUI',
      'Store Inspect',
      'Web Dashboard',
      'GPUI Dashboard',
      'Tauri Dashboard',
      'MCP',
      'MCP Control',
      'AI SRE',
    ],
  },
];

/** Fixed precision keeps server and client markup identical. */
function slotPosition(ring: Ring, index: number): CSSProperties {
  const angle = ((ring.offset + (360 / ring.orbit.length) * index - 90) * Math.PI) / 180;
  return {
    left: `${(50 + ring.radius * Math.cos(angle)).toFixed(3)}%`,
    top: `${(50 + ring.radius * Math.sin(angle)).toFixed(3)}%`,
  };
}

export default function EcosystemOrbit(): React.JSX.Element {
  const headingId = useId();
  const logoUrl = useBaseUrl('/img/Rocketmq-rust-logo.png');
  const [activeGroup, setActiveGroup] = useState<GroupId | null>(null);

  const copy: Record<GroupId, {title: string; text: string; link: string; to: string}> = {
    services: {
      title: translate({id: 'homepage.eco.services.title', message: 'Core services'}),
      text: translate({
        id: 'homepage.eco.services.text',
        message: 'NameServer, Broker, Proxy and Controller: the deployable runtime of a RocketMQ cluster.',
      }),
      link: translate({id: 'homepage.eco.services.link', message: 'Architecture overview'}),
      to: '/docs/architecture/overview',
    },
    libraries: {
      title: translate({id: 'homepage.eco.libraries.title', message: 'Reusable crates'}),
      text: translate({
        id: 'homepage.eco.libraries.text',
        message:
          'Client SDK, protocol, transport, storage engines, runtime and auth, each usable on its own from crates.io.',
      }),
      link: translate({id: 'homepage.eco.libraries.link', message: 'Module map'}),
      to: '/docs/architecture/module-map',
    },
    operations: {
      title: translate({id: 'homepage.eco.operations.title', message: 'Operations and AI'}),
      text: translate({
        id: 'homepage.eco.operations.text',
        message: 'Admin CLI and TUI, web and desktop dashboards, MCP servers and AI SRE workflows.',
      }),
      link: translate({id: 'homepage.eco.operations.link', message: 'Ecosystem guide'}),
      to: '/docs/ecosystem/overview',
    },
  };

  return (
    <section className={clsx(primitives.section, styles.section)} aria-labelledby={headingId}>
      <div className={primitives.container}>
        <SectionHeading
          id={headingId}
          eyebrow={translate({id: 'homepage.eco.eyebrow', message: 'Ecosystem'})}
          title={translate({id: 'homepage.eco.title', message: 'One workspace, a whole messaging platform'})}
          lead={translate({
            id: 'homepage.eco.lead',
            message: 'Services, libraries and operations tooling evolve together in a single repository.',
          })}
        />

        <div className={styles.layout}>
          <Reveal className={styles.stage}>
            <div className={clsx(styles.orbit, activeGroup && styles.orbitFocused)} aria-hidden="true">
              <div className={styles.sweep} />

              {RINGS.map((ring) => (
                <div
                  key={ring.id}
                  className={clsx(styles.ring, styles[ring.id], activeGroup === ring.id && styles.ringActive)}
                  style={{'--radius': `${ring.radius}%`} as CSSProperties}>
                  <span className={styles.track} />
                  <div
                    className={styles.cometLayer}
                    style={{
                      animationDuration: `${Math.abs(ring.cometPeriod)}s`,
                      animationDirection: ring.cometPeriod < 0 ? 'reverse' : 'normal',
                    }}>
                    <span className={styles.comet} />
                  </div>
                </div>
              ))}

              {/* One rotating layer for every label keeps the rings in phase. */}
              <div className={styles.spin}>
                {RINGS.map((ring) => (
                  <div
                    key={ring.id}
                    className={clsx(styles.ring, styles[ring.id], activeGroup === ring.id && styles.ringActive)}>
                    {ring.orbit.map((member, index) => (
                      <span key={member} className={styles.slot} style={slotPosition(ring, index)}>
                        <span className={styles.chip}>
                          <span>{member}</span>
                        </span>
                      </span>
                    ))}
                  </div>
                ))}
              </div>

              <div className={styles.core}>
                <span className={styles.corePulse} />
                <span className={styles.corePulse} />
                <img src={logoUrl} alt="" width="96" height="96" loading="lazy" decoding="async" />
              </div>
            </div>
          </Reveal>

          <ul className={styles.groups}>
            {RINGS.map((ring, index) => {
              const group = copy[ring.id];
              return (
                <Reveal key={ring.id} as="li" delay={index * 110}>
                  <div
                    className={clsx(styles.group, styles[ring.id], activeGroup === ring.id && styles.groupActive)}
                    onPointerEnter={() => setActiveGroup(ring.id)}
                    onPointerLeave={() => setActiveGroup(null)}
                    onFocus={() => setActiveGroup(ring.id)}
                    onBlur={() => setActiveGroup(null)}>
                    <h3 className={styles.groupTitle}>
                      <span className={styles.groupDot} />
                      {group.title}
                    </h3>
                    <p className={styles.groupText}>{group.text}</p>
                    <ul className={styles.members}>
                      {ring.members.map((member) => (
                        <li key={member}>{member}</li>
                      ))}
                    </ul>
                    <Link className={primitives.textLink} to={group.to}>
                      {group.link}
                      <ArrowRightIcon />
                    </Link>
                  </div>
                </Reveal>
              );
            })}
          </ul>
        </div>
      </div>
    </section>
  );
}
