import React, {useEffect, useRef, useState, type CSSProperties} from 'react';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import {Reveal, useInView, usePrefersReducedMotion} from './motion';
import {CRATES_URL, GITHUB_URL, VERSION, WORKSPACE_CRATES} from './site';
import styles from './StatsBand.module.css';

const COUNT_DURATION_MS = 1700;

/**
 * Animates toward `target` once `active`. Renders the final value on the
 * server, so the number is correct without JavaScript; after hydration it
 * waits at `from` until the stat scrolls into view.
 */
function useCountUp(target: number, from: number, active: boolean): number {
  const reducedMotion = usePrefersReducedMotion();
  const [value, setValue] = useState<number | null>(null);
  const current = useRef(from);

  useEffect(() => {
    if (reducedMotion) {
      current.current = target;
      setValue(target);
      return undefined;
    }
    if (!active) {
      setValue(current.current);
      return undefined;
    }

    const start = current.current;
    const startedAt = performance.now();
    let frame = 0;
    const step = (now: number): void => {
      const elapsed = Math.min(1, (now - startedAt) / COUNT_DURATION_MS);
      const eased = 1 - (1 - elapsed) ** 4;
      current.current = start + (target - start) * eased;
      setValue(current.current);
      if (elapsed < 1) {
        frame = window.requestAnimationFrame(step);
      }
    };
    frame = window.requestAnimationFrame(step);
    return () => window.cancelAnimationFrame(frame);
  }, [active, reducedMotion, target]);

  return value ?? target;
}

type StatProps = {
  target: number;
  from: number;
  format: (value: number) => string;
  label: string;
  detail: string;
  href?: string;
  index: number;
};

function Stat({target, from, format, label, detail, href, index}: StatProps): React.JSX.Element {
  const [ref, inView] = useInView<HTMLSpanElement>({once: true, threshold: 0.6});
  const value = useCountUp(target, from, inView);
  const style = {'--stat': index} as CSSProperties;
  const content = (
    <>
      <span ref={ref} className={styles.value}>
        {format(value)}
      </span>
      <span className={styles.label}>{label}</span>
      <span className={styles.detail}>{detail}</span>
    </>
  );

  return (
    <Reveal as="li" className={styles.stat} delay={index * 90}>
      {href ? (
        <Link className={styles.statBody} to={href} style={style}>
          {content}
        </Link>
      ) : (
        <div className={styles.statBody} style={style}>
          {content}
        </div>
      )}
    </Reveal>
  );
}

type StatsBandProps = {
  stars: number;
};

export default function StatsBand({stars}: StatsBandProps): React.JSX.Element {
  const [major, minor] = VERSION.split('.').map(Number);
  const versionTarget = major + minor / 10;
  const numberFormat = new Intl.NumberFormat('en-US');

  return (
    <section
      className={styles.section}
      aria-label={translate({id: 'homepage.stats.label', message: 'Project at a glance'})}>
      <ul className={styles.band}>
        <Stat
          index={0}
          // Counts through the 0.x minor releases up to the current major.
          target={versionTarget}
          from={0.1}
          format={(value) => (value >= versionTarget ? `v${VERSION}` : `v${value.toFixed(1)}.0`)}
          label={translate({id: 'homepage.stats.release.label', message: 'Stable release'})}
          detail={translate({id: 'homepage.stats.release.detail', message: 'Published on crates.io'})}
          href={CRATES_URL}
        />
        <Stat
          index={1}
          target={WORKSPACE_CRATES}
          from={0}
          format={(value) => String(Math.round(value))}
          label={translate({id: 'homepage.stats.crates.label', message: 'Workspace crates'})}
          detail={translate({id: 'homepage.stats.crates.detail', message: 'From protocol to storage, one repository'})}
          href="/docs/architecture/module-map"
        />
        <Stat
          index={2}
          target={0}
          from={99}
          format={(value) => String(Math.round(value))}
          label={translate({id: 'homepage.stats.gc.label', message: 'Garbage-collector pauses'})}
          detail={translate({id: 'homepage.stats.gc.detail', message: 'Rust frees memory without a GC'})}
        />
        <Stat
          index={3}
          target={stars}
          from={0}
          format={(value) => numberFormat.format(Math.round(value))}
          label={translate({id: 'homepage.stats.stars.label', message: 'GitHub stars'})}
          detail={translate({id: 'homepage.stats.stars.detail', message: 'Star the repository'})}
          href={GITHUB_URL}
        />
      </ul>
    </section>
  );
}
