import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';
import {translate} from '@docusaurus/Translate';

type FeatureItem = {
  title: string;
  motion: string;
  icon: string;
  description: React.JSX.Element;
  tone: 'rust' | 'async' | 'platform';
};

type MotionItem = {
  title: string;
  detail: string;
  tone: 'particles' | 'core' | 'flow' | 'cta';
};

function Feature({ title, motion, icon, description, tone }: FeatureItem) {
  return (
    <article className={styles.feature}>
      <div className={clsx(styles.card, styles[tone])}>
        <div className={styles.cardTop}>
          <div className={styles.icon}>
            {icon}
          </div>
          <span>{motion}</span>
        </div>
        <div>
          <h3>{title}</h3>
          <p>{description}</p>
        </div>
      </div>
    </article>
  );
}

function MotionSpec({title, detail, tone}: MotionItem) {
  return (
    <article className={clsx(styles.motionCard, styles[tone])}>
      <span className={styles.motionAccent} />
      <h3>{title}</h3>
      <p>{detail}</p>
    </article>
  );
}

export default function HomepageFeatures(): React.JSX.Element {
  const featureList: FeatureItem[] = [
    {
      title: translate({
        id: 'homepage.features.rust.title',
        message: 'Rust Language Advantages',
      }),
      motion: translate({
        id: 'homepage.features.rust.badge',
        message: 'memory safe core',
      }),
      icon: 'R',
      tone: 'rust',
      description: (
        <>
          {translate({
            id: 'homepage.features.rust.description',
            message:
              "RocketMQ-Rust uses Rust's ownership model and zero-cost abstractions to implement brokers, clients, remoting, and storage with predictable performance.",
          })}
        </>
      ),
    },
    {
      title: translate({
        id: 'homepage.features.async.title',
        message: 'Async Non-blocking Design',
      }),
      motion: translate({
        id: 'homepage.features.async.badge',
        message: 'tokio async I/O',
      }),
      icon: 'A',
      tone: 'async',
      description: (
        <>
          {translate({
            id: 'homepage.features.async.description',
            message:
              'Producer, consumer, remoting, and broker services are designed around asynchronous I/O so high-concurrency message traffic does not block worker threads.',
          })}
        </>
      ),
    },
    {
      title: translate({
        id: 'homepage.features.crossPlatform.title',
        message: 'Cross-platform Support',
      }),
      motion: translate({
        id: 'homepage.features.crossPlatform.badge',
        message: 'cargo ready',
      }),
      icon: 'X',
      tone: 'platform',
      description: (
        <>
          {translate({
            id: 'homepage.features.crossPlatform.description',
            message:
              'Run and develop RocketMQ-Rust across Linux, Windows, and macOS with Cargo, examples, and documentation aligned for the Rust ecosystem.',
          })}
        </>
      ),
    },
  ];

  const motionSpecs: MotionItem[] = [
    {
      title: translate({
        id: 'homepage.motion.nameserver.title',
        message: 'NameServer Routing',
      }),
      detail: translate({
        id: 'homepage.motion.nameserver.detail',
        message: 'Producers and consumers resolve broker routes through NameServer before sending or pulling messages.',
      }),
      tone: 'particles',
    },
    {
      title: translate({
        id: 'homepage.motion.broker.title',
        message: 'Broker Storage',
      }),
      detail: translate({
        id: 'homepage.motion.broker.detail',
        message: 'Messages are appended to CommitLog and dispatched to queues for reliable delivery.',
      }),
      tone: 'core',
    },
    {
      title: translate({
        id: 'homepage.motion.producer.title',
        message: 'Producer APIs',
      }),
      detail: translate({
        id: 'homepage.motion.producer.detail',
        message: 'Client APIs support topic messages, queue selection, batch sending, and transactional messaging flows.',
      }),
      tone: 'flow',
    },
    {
      title: translate({
        id: 'homepage.motion.consumer.title',
        message: 'Consumer Delivery',
      }),
      detail: translate({
        id: 'homepage.motion.consumer.detail',
        message: 'Consumers process messages in push or pull mode and keep progress through offsets.',
      }),
      tone: 'cta',
    },
  ];

  return (
    <section className={styles.features}>
      <div className="container">
        <div className={styles.sectionIntro}>
          <span>
            {translate({
              id: 'homepage.features.eyebrow',
              message: 'ROCKETMQ-RUST CORE',
            })}
          </span>
          <h2>
            {translate({
              id: 'homepage.features.title',
              message: "A Rust implementation of Apache RocketMQ's messaging architecture",
            })}
          </h2>
        </div>

        <div className={styles.featureGrid}>
          {featureList.map((props) => (
            <Feature key={props.title} {...props} />
          ))}
        </div>

        <div id="motion-system" className={styles.motionSystem}>
          <h2>
            {translate({
              id: 'homepage.motion.title',
              message: 'RocketMQ message flow in Rust',
            })}
          </h2>
          <div className={styles.motionGrid}>
            {motionSpecs.map((spec) => (
              <MotionSpec key={spec.title} {...spec} />
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
