import React, {useId} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {translate} from '@docusaurus/Translate';
import useBaseUrl from '@docusaurus/useBaseUrl';
import {ArrowRightIcon, BookIcon, ChatIcon, GitHubIcon, StarIcon} from './icons';
import {Reveal} from './motion';
import {DISCUSSIONS_URL, GITHUB_URL, GOOD_FIRST_ISSUES_URL, formatCompact} from './site';
import primitives from './primitives.module.css';
import styles from './CommunityCta.module.css';

type CommunityCtaProps = {
  stars: number;
};

export default function CommunityCta({stars}: CommunityCtaProps): React.JSX.Element {
  const headingId = useId();
  const logoUrl = useBaseUrl('/img/Rocketmq-rust-logo.png');

  return (
    <section className={clsx(primitives.section, styles.section)} aria-labelledby={headingId}>
      <div className={primitives.container}>
        <Reveal className={styles.frame}>
          <div className={styles.panel}>
            <div className={styles.backdrop} aria-hidden="true">
              <span className={clsx(styles.orb, styles.orbOrange)} />
              <span className={clsx(styles.orb, styles.orbViolet)} />
              <span className={styles.mesh} />
            </div>

            <div className={styles.rocket} aria-hidden="true">
              <span className={styles.rocketGlow} />
              <img src={logoUrl} alt="" width="112" height="112" loading="lazy" decoding="async" />
            </div>

            <h2 id={headingId} className={styles.title}>
              {translate({id: 'homepage.cta.title', message: 'Build the next generation of messaging with us'})}
            </h2>
            <p className={styles.lead}>
              {translate({
                id: 'homepage.cta.lead',
                message:
                  'RocketMQ-Rust is developed in the open. Star the repository, join the discussion, or pick up a good first issue.',
              })}
            </p>

            <div className={styles.actions}>
              <Link className={clsx(primitives.button, primitives.buttonPrimary)} to={GITHUB_URL}>
                <GitHubIcon />
                {translate({id: 'homepage.hero.star', message: 'Star on GitHub'})}
                <span className={clsx(primitives.buttonCount, styles.count)}>
                  <StarIcon />
                  {formatCompact(stars)}
                </span>
              </Link>
              <Link className={clsx(primitives.button, primitives.buttonGhost)} to={DISCUSSIONS_URL}>
                <ChatIcon />
                {translate({id: 'homepage.cta.discussions', message: 'Join the discussion'})}
              </Link>
              <Link className={clsx(primitives.button, primitives.buttonGhost)} to="/docs/contributing/overview">
                <BookIcon />
                {translate({id: 'homepage.cta.contribute', message: 'Contributing guide'})}
              </Link>
            </div>

            <Link className={clsx(primitives.textLink, styles.issues)} to={GOOD_FIRST_ISSUES_URL}>
              {translate({id: 'homepage.cta.issues', message: 'Browse good first issues'})}
              <ArrowRightIcon />
            </Link>
          </div>
        </Reveal>

        <p className={styles.disclaimer}>
          {translate({
            id: 'homepage.cta.disclaimer',
            message:
              'RocketMQ-Rust is an independent community project, not an official Apache Software Foundation release. Apache RocketMQ is a trademark of the Apache Software Foundation.',
          })}
        </p>
      </div>
    </section>
  );
}
