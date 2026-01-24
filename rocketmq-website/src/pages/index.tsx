import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '../components/HomepageFeatures';
import DeveloperStyleHero from '../components/DeveloperStyleHero';

export default function Home(): React.JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout title={`${siteConfig.title}`} description="High-performance messaging middleware built with Rust">
      <DeveloperStyleHero />
      <main style={{
        background: 'var(--homepage-bg)',
        padding: '0',
      }}>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
