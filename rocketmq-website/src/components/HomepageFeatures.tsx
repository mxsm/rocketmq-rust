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
    title: 'Built with Rust',
    Svg: require('@site/static/img/rust-logo.svg').default,
    description: (
      <>
        Memory-safe and blazing fast performance. RocketMQ-Rust leverages Rust's ownership model and zero-cost abstractions
        for enterprise-grade reliability.
      </>
    ),
  },
  {
    title: 'High Performance',
    Svg: require('@site/static/img/performance.svg').default,
    description: (
      <>
        Designed for high-throughput, low-latency messaging. Async/await based architecture withTokio runtime for
        exceptional concurrency and scalability.
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
  {
    title: 'Async by Design',
    Svg: require('@site/static/img/async.svg').default,
    description: (
      <>
        Fully asynchronous architecture using Tokio. Non-blocking I/O operations ensure optimal resource utilization and
        responsiveness under load.
      </>
    ),
  },
  {
    title: 'Type Safe',
    Svg: require('@site/static/img/types.svg').default,
    description: (
      <>
        Strong typing with Rust's type system prevents entire classes of bugs at compile-time. Self-documenting APIs with
        comprehensive IDE support.
      </>
    ),
  },
  {
    title: 'Cloud Native',
    Svg: require('@site/static/img/cloud.svg').default,
    description: (
      <>
        Designed for modern cloud environments. Container-ready, supports Kubernetes deployment patterns, and integrates
        seamlessly with cloud-native ecosystems.
      </>
    ),
  },
];

function Feature({ title, Svg, description }: FeatureItem) {
  return (
    <div className={clsx('col col--4', styles.feature)}>
      <div className="card">
        <div className="card__header">
          <div className={styles.featureIcon}>
            <Svg className={styles.featureSvg} role="img" />
          </div>
          <h3>{title}</h3>
        </div>
        <div className="card__body">
          <p>{description}</p>
        </div>
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
