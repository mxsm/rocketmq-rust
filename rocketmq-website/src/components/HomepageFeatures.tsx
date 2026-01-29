import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';
import {translate} from '@docusaurus/Translate';

type FeatureItem = {
  title: string;
  Svg: React.ComponentType<React.ComponentProps<'svg'>>;
  description: JSX.Element;
};

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
  const FeatureList: FeatureItem[] = [
    {
      title: translate({
        id: 'homepage.features.rust.title',
        message: 'Rust Language Advantages',
      }),
      Svg: require('@site/static/img/rust-logo.svg').default,
      description: (
        <>
          {translate({
            id: 'homepage.features.rust.description',
            message: 'Memory safety, zero-cost abstractions, and high concurrency performance. Leverages Rust\'s ownership model for enterprise-grade reliability and efficiency.',
          })}
        </>
      ),
    },
    {
      title: translate({
        id: 'homepage.features.async.title',
        message: 'Async Non-blocking Design',
      }),
      Svg: require('@site/static/img/async.svg').default,
      description: (
        <>
          {translate({
            id: 'homepage.features.async.description',
            message: 'Fully asynchronous architecture using Rust\'s async capabilities. Non-blocking design supports high-concurrency message processing with optimal resource utilization.',
          })}
        </>
      ),
    },
    {
      title: translate({
        id: 'homepage.features.crossPlatform.title',
        message: 'Cross-platform Support',
      }),
      Svg: require('@site/static/img/shield.svg').default,
      description: (
        <>
          {translate({
            id: 'homepage.features.crossPlatform.description',
            message: 'Supports multiple platforms including Linux, Windows, and macOS. Convenient deployment across different environments with consistent performance and behavior.',
          })}
        </>
      ),
    },
  ];

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
