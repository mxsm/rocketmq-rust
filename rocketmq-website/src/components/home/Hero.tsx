import React, {useEffect, useRef, type CSSProperties} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import CopyButton from './CopyButton';
import HeroTerminal from './HeroTerminal';
import NetworkCanvas from './NetworkCanvas';
import {ArrowRightIcon, GitHubIcon, StarIcon} from './icons';
import {GITHUB_URL, INSTALL_COMMAND, RELEASE_URL, VERSION, formatCompact} from './site';
import primitives from './primitives.module.css';
import styles from './Hero.module.css';

type HeroProps = {
  /** Live GitHub star count; a placeholder is rendered by the caller while it loads. */
  stars: number;
};

export default function Hero({stars}: HeroProps): React.JSX.Element {
  const heroRef = useRef<HTMLElement>(null);

  // Publish the pointer position as CSS variables that drive the spotlight and the terminal tilt.
  useEffect(() => {
    const hero = heroRef.current;
    if (!hero || !window.matchMedia('(hover: hover) and (pointer: fine)').matches) {
      return undefined;
    }

    let frame = 0;
    let clientX = 0;
    let clientY = 0;

    const apply = (): void => {
      frame = 0;
      const bounds = hero.getBoundingClientRect();
      const x = clientX - bounds.left;
      const y = clientY - bounds.top;
      hero.style.setProperty('--spot-x', `${x.toFixed(1)}px`);
      hero.style.setProperty('--spot-y', `${y.toFixed(1)}px`);
      hero.style.setProperty('--tilt-x', (x / bounds.width - 0.5).toFixed(3));
      hero.style.setProperty('--tilt-y', (y / bounds.height - 0.5).toFixed(3));
    };
    const handleMove = (event: PointerEvent): void => {
      clientX = event.clientX;
      clientY = event.clientY;
      if (!frame) {
        frame = window.requestAnimationFrame(apply);
      }
    };
    const handleLeave = (): void => {
      hero.style.setProperty('--tilt-x', '0');
      hero.style.setProperty('--tilt-y', '0');
    };

    hero.addEventListener('pointermove', handleMove, {passive: true});
    hero.addEventListener('pointerleave', handleLeave);
    return () => {
      window.cancelAnimationFrame(frame);
      hero.removeEventListener('pointermove', handleMove);
      hero.removeEventListener('pointerleave', handleLeave);
    };
  }, []);

  // Shared with the messaging-pattern chips in FeatureBento.
  const rotatingWords = [
    translate({id: 'homepage.pattern.ordered', message: 'ordered'}),
    translate({id: 'homepage.pattern.delayed', message: 'delayed'}),
    translate({id: 'homepage.pattern.transactional', message: 'transactional'}),
    translate({id: 'homepage.pattern.batched', message: 'batched'}),
    translate({id: 'homepage.pattern.requestReply', message: 'request-reply'}),
  ];
  const rotatorPrefix = translate({id: 'homepage.hero.rotator.prefix', message: 'Built for messages that are'});

  return (
    <header ref={heroRef} className={styles.hero}>
      <div className={styles.backdrop} aria-hidden="true">
        <div className={clsx(styles.aurora, styles.auroraOrange)} />
        <div className={clsx(styles.aurora, styles.auroraViolet)} />
        <div className={clsx(styles.aurora, styles.auroraCyan)} />
        <div className={styles.dotGrid} />
        <NetworkCanvas className={styles.canvas} />
        <div className={styles.spotlight} />
        <div className={styles.fade} />
      </div>

      <div className={styles.inner}>
        <div className={styles.copy}>
          <Link className={styles.release} to={RELEASE_URL} style={{'--order': 0} as CSSProperties}>
            <span className={styles.releaseDot} />
            <span>
              {translate(
                {id: 'homepage.hero.release.label', message: 'v{version} is live on crates.io'},
                {version: VERSION},
              )}
            </span>
            <span className={styles.releaseAction}>
              {translate({id: 'homepage.hero.release.action', message: 'Release notes'})}
              <ArrowRightIcon />
            </span>
          </Link>

          <h1 className={styles.title} style={{'--order': 1} as CSSProperties}>
            <span className={styles.titleLead}>
              {translate({id: 'homepage.hero.title.lead', message: 'Distributed messaging,'})}
            </span>{' '}
            <span className={styles.titleAccent}>
              {translate({id: 'homepage.hero.title.accent', message: 'forged in Rust.'})}
            </span>
          </h1>

          <p className={styles.lead} style={{'--order': 2} as CSSProperties}>
            {translate({
              id: 'homepage.hero.lead',
              message:
                'RocketMQ-Rust is a community-built Rust implementation of Apache RocketMQ: NameServer, Broker, Proxy, Controller and async clients — memory-safe, non-blocking, and speaking the RocketMQ remoting and gRPC protocols.',
            })}
          </p>

          <p className={styles.rotator} style={{'--order': 3} as CSSProperties}>
            <span className={styles.rotatorMark} aria-hidden="true">
              ▸
            </span>
            <span aria-hidden="true">{rotatorPrefix}</span>
            <span className={styles.words} aria-hidden="true">
              {rotatingWords.map((word, index) => (
                <span key={word} style={{'--word': index} as CSSProperties}>
                  {word}
                </span>
              ))}
            </span>
            <span className={styles.srOnly}>{`${rotatorPrefix} ${rotatingWords.join(', ')}`}</span>
          </p>

          <div className={styles.actions} style={{'--order': 4} as CSSProperties}>
            <Link className={clsx(primitives.button, primitives.buttonPrimary)} to="/docs/introduction">
              {translate({id: 'homepage.hero.getStarted', message: 'Get Started'})}
              <ArrowRightIcon />
            </Link>
            <Link className={clsx(primitives.button, primitives.buttonGhost)} to={GITHUB_URL}>
              <GitHubIcon />
              {translate({id: 'homepage.hero.star', message: 'Star on GitHub'})}
              <span className={primitives.buttonCount}>
                <StarIcon />
                {formatCompact(stars)}
              </span>
            </Link>
          </div>

          <div className={styles.install} style={{'--order': 5} as CSSProperties}>
            <span className={styles.installPrompt} aria-hidden="true">
              $
            </span>
            <code className={styles.installCommand}>{INSTALL_COMMAND}</code>
            <CopyButton text={INSTALL_COMMAND} />
          </div>
        </div>

        <div className={styles.visual}>
          <div className={styles.visualFloat}>
            <HeroTerminal />
          </div>
        </div>
      </div>

      <a className={styles.scrollCue} href="#explore">
        <span className={styles.scrollTrack}>
          <span className={styles.scrollDot} />
        </span>
        {translate({id: 'homepage.hero.scroll', message: 'Scroll to explore'})}
      </a>
    </header>
  );
}
