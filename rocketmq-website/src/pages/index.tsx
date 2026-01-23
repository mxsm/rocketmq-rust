import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import OrbBackgroundGlobal from '@site/src/components/OrbBackground.global';

import styles from './index.module.css';

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <OrbBackgroundGlobal />
      <div className="container" style={{ position: 'relative', zIndex: 2 }}>
        <div className={styles.heroContent}>
          <div className={styles.heroLeft}>
            <h1 className="hero__title">
              Build <span className={styles.highlight}>optimized</span> messaging systems{' '}
              <span className={styles.highlight2}>quickly</span>
            </h1>
            <p className="hero__subtitle">
              {siteConfig.tagline}. Focus on your business logic while we handle the heavy lifting.
            </p>
            <div className={styles.buttons}>
              <Link
                className={clsx('button', styles.buttonPrimary)}
                to="/docs/introduction"
              >
                Get Started
              </Link>
              <Link
                className={clsx('button', styles.buttonSecondary)}
                to="https://github.com/apache/rocketmq-rust"
              >
                GitHub
              </Link>
            </div>
          </div>
          <div className={styles.heroRight}>
            <div className={styles.heroIllustration}>
              <svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">
                <circle cx="100" cy="100" r="80" fill="#2563eb" opacity="0.1"/>
                <circle cx="100" cy="100" r="60" fill="#2563eb" opacity="0.2"/>
                <path d="M100 40 L100 160 M40 100 L160 100" stroke="#2563eb" strokeWidth="2" opacity="0.3"/>
                <circle cx="100" cy="100" r="30" fill="#2563eb"/>
                <text x="100" y="108" textAnchor="middle" fill="white" fontSize="20" fontWeight="bold">MQ</text>
              </svg>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}

export default function Home(): React.JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout title={`${siteConfig.title}`} description="High-performance messaging middleware built with Rust">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
