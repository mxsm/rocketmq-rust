import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<'svg'>>;
  description: JSX.Element;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Powered by Rust',
    Svg: require('@site/static/img/rust-logo.svg').default,
    description: (
      <>
        Memory-safe and blazing fast performance. Leverages Rust's ownership model and zero-cost abstractions
        for enterprise-grade reliability.
      </>
    ),
  },
  {
    title: 'Async by Design',
    Svg: require('@site/static/img/async.svg').default,
    description: (
      <>
        Fully asynchronous architecture using Tokio runtime. Non-blocking I/O operations ensure optimal resource
        utilization and exceptional concurrency.
      </>
    ),
  },
  {
    title: 'Production Ready',
    Svg: require('@site/static/img/shield.svg').default,
    description: (
      <>
        Built for mission-critical applications. Comprehensive error handling, monitoring support, and battle-tested
        messaging patterns from Apache RocketMQ.
      </>
    ),
  },
];

function Feature({ title, Svg, description }: FeatureItem) {
  return (
    <div className={clsx('col', styles.feature)}>
      <div className={styles.card}>
        <div className={styles.icon}>
          <Svg className={styles.svg} role="img" />
        </div>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): React.JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
