import React, {useEffect, useRef, useState} from 'react';
import {HtmlClassNameProvider} from '@docusaurus/theme-common';
import {translate} from '@docusaurus/Translate';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import ArchitectureFlow from '../components/home/ArchitectureFlow';
import CapabilityMarquee from '../components/home/CapabilityMarquee';
import CodeShowcase from '../components/home/CodeShowcase';
import CommunityCta from '../components/home/CommunityCta';
import EcosystemOrbit from '../components/home/EcosystemOrbit';
import FeatureBento from '../components/home/FeatureBento';
import GetStarted from '../components/home/GetStarted';
import Hero from '../components/home/Hero';
import StatsBand from '../components/home/StatsBand';
import {MOTION_ATTRIBUTE} from '../components/home/motion';
import {useGitHubStars} from '../components/home/useGitHubStars';
import styles from './index.module.css';

/** Class on <html> that css/custom.css uses to restyle the navbar on this page only. */
const HOME_HTML_CLASS = 'rmq-home';
/** Set on <html> once the page has scrolled, so the navbar can switch from transparent to solid. */
const SCROLLED_ATTRIBUTE = 'data-rmq-scrolled';
const SCROLLED_THRESHOLD_PX = 12;

export default function Home(): React.JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  const stars = useGitHubStars();
  const progressRef = useRef<HTMLDivElement>(null);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    setHydrated(true);

    const root = document.documentElement;
    let frame = 0;
    const update = (): void => {
      frame = 0;
      const scrolled = window.scrollY;
      const scrollable = root.scrollHeight - window.innerHeight;
      root.toggleAttribute(SCROLLED_ATTRIBUTE, scrolled > SCROLLED_THRESHOLD_PX);
      if (progressRef.current) {
        const progress = scrollable > 0 ? Math.min(1, scrolled / scrollable) : 0;
        progressRef.current.style.transform = `scaleX(${progress.toFixed(4)})`;
      }
    };
    const schedule = (): void => {
      if (!frame) {
        frame = window.requestAnimationFrame(update);
      }
    };

    update();
    window.addEventListener('scroll', schedule, {passive: true});
    window.addEventListener('resize', schedule);
    return () => {
      window.cancelAnimationFrame(frame);
      window.removeEventListener('scroll', schedule);
      window.removeEventListener('resize', schedule);
      root.removeAttribute(SCROLLED_ATTRIBUTE);
    };
  }, []);

  return (
    <HtmlClassNameProvider className={HOME_HTML_CLASS}>
      <Layout
        title={siteConfig.title}
        description={translate({
          id: 'homepage.meta.description',
          message:
            'RocketMQ-Rust is a community-built Rust implementation of Apache RocketMQ: NameServer, Broker, Proxy, Controller and async clients with memory safety and non-blocking I/O.',
        })}>
        <div className={styles.home} {...{[MOTION_ATTRIBUTE]: hydrated ? 'on' : undefined}}>
          <div ref={progressRef} className={styles.progress} aria-hidden="true" />
          <Hero stars={stars} />
          <main className={styles.main}>
            <CapabilityMarquee />
            <StatsBand stars={stars} />
            <ArchitectureFlow />
            <FeatureBento />
            <CodeShowcase />
            <EcosystemOrbit />
            <GetStarted />
            <CommunityCta stars={stars} />
          </main>
        </div>
      </Layout>
    </HtmlClassNameProvider>
  );
}
