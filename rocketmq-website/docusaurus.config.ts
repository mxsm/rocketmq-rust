import type { Config } from '@docusaurus/types';
import type { Options as PresetOptions } from '@docusaurus/preset-classic';
import { themes as prismThemes } from 'prism-react-renderer';

const config: Config = {
  title: 'RocketMQ-Rust',
  tagline: 'High-performance messaging middleware built with Rust',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://rocketmq-rust.apache.org',
  // Set the /<baseUrl>/ to <repoPath>.
  baseUrl: '/',

  // GitHub pages deployment config.
  organizationName: 'apache',
  projectName: 'rocketmq-rust',
  deploymentBranch: 'gh-pages',

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'warn',

  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'zh-CN'],
    localeConfigs: {
      en: {
        label: 'English',
        htmlLang: 'en-US',
      },
      'zh-CN': {
        label: '简体中文',
        htmlLang: 'zh-CN',
      },
    },
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/apache/rocketmq-rust/tree/main/rocketmq-website/',
          versions: {
            current: {
              label: 'Next',
              badge: false,
            },
          },
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies PresetOptions,
    ],
  ],

  themeConfig: {
    image: 'img/docusaurus-social-card.jpg',
    navbar: {
      title: 'RocketMQ-Rust',
      logo: {
        alt: 'RocketMQ-Rust Logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          href: 'https://github.com/apache/rocketmq-rust',
          label: 'GitHub',
          position: 'right',
        },
        {
          type: 'localeDropdown',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/category/getting-started',
            },
            {
              label: 'Architecture',
              to: '/docs/category/architecture',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/apache/rocketmq-rust',
            },
            {
              label: 'Contributing',
              to: '/docs/category/contributing',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Apache RocketMQ',
              href: 'https://rocketmq.apache.org/',
            },
            {
              label: 'Stack Overflow',
              href: 'https://stackoverflow.com/questions/tagged/rocketmq',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} The Apache Software Foundation. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'java', 'yaml', 'bash', 'toml'],
    },
    algolia: {
      appId: 'YOUR_APP_ID',
      apiKey: 'YOUR_API_KEY',
      indexName: 'rocketmq-rust',
      contextualSearch: true,
    },
    announcementBar: {
      id: 'announcement_bar',
      content:
        '⚠️ RocketMQ-Rust is under active development. APIs may change before the 1.0 release.',
      backgroundColor: '#fafbfc',
      textColor: '#091E42',
      isCloseable: true,
    },
  } satisfies PresetOptions['themeConfig'],

  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          {
            to: '/docs/introduction',
            from: ['/'],
          },
        ],
      },
    ],
  ],

  // Markdown processing
  markdown: {
    mermaid: true,
    parseFrontMatter: async (params) => {
      const result = await params.defaultParseFrontMatter(params);
      return result;
    },
  },

  themes: ['@docusaurus/theme-mermaid'],
};

export default config;
