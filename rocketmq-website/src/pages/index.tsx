import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import DeveloperStyleHero from '@site/src/components/DeveloperStyleHero';

export default function Home(): React.JSX.Element {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout title={`${siteConfig.title}`} description="High-performance messaging middleware built with Rust">
      <DeveloperStyleHero />
      <main style={{
        background: '#0f172a',
        padding: '60px 24px',
      }}>
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
