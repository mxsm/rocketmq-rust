import type {Config} from '@docusaurus/types';
import type {Options as PresetOptions} from '@docusaurus/preset-classic';
import {themes as prismThemes} from 'prism-react-renderer';

const config: Config = {
    title: 'RocketMQ-Rust',
    tagline: 'High-performance messaging middleware built with Rust',
    favicon: 'img/favicon.svg',

    // Set the production url of your site here
    url: 'https://rocketmqrust.com',
    // Set the /<baseUrl>/ to <repoPath>.
    baseUrl: '/',
    trailingSlash: false,

    // GitHub pages deployment config.
    organizationName: 'mxsm',
    projectName: 'rocketmq-rust',
    deploymentBranch: 'gh-pages',

    onBrokenLinks: 'warn',
    onBrokenAnchors: 'warn',

    // Markdown processing
    markdown: {
        mermaid: true,
        hooks: {
            onBrokenMarkdownLinks: 'warn',
        },
        parseFrontMatter: async (params) => {
            const result = await params.defaultParseFrontMatter(params);
            return result;
        },
    },

    i18n: {
        defaultLocale: 'en',
        locales: ['en', 'zh-CN'],
        localeConfigs: {
            en: {
                label: 'English',
                direction: 'ltr',
                htmlLang: 'en-US',
            },
            'zh-CN': {
                label: '简体中文',
                direction: 'ltr',
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
                    editUrl: 'https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/',
                    versions: {
                        current: {
                            label: 'Next',
                            badge: false,
                        },
                    },
                },
                blog: {
                    showReadingTime: true,
                    blogTitle: 'Release Notes',
                    blogDescription: 'RocketMQ-Rust release announcements and changelogs',
                    postsPerPage: 10,
                    blogSidebarTitle: 'All Releases',
                    blogSidebarCount: 'ALL',
                },
                theme: {
                    customCss: './src/css/custom.css',
                },
            } satisfies PresetOptions,
        ],
    ],

    themeConfig: {
        image: 'img/docusaurus-social-card.jpg',
        colorMode: {
            defaultMode: 'dark',
            disableSwitch: false,
            respectPrefersColorScheme: true,
        },
        navbar: {
            title: 'RocketMQ-Rust',
            logo: {
                alt: 'RocketMQ-Rust Logo',
                src: 'img/logo.svg',
            },
            items: [
                {
                    to: '/docs/introduction',
                    position: 'left',
                    label: 'Documentation',
                    activeBaseRegex: '^/docs/(?!contributing|author)',
                },
                {
                    to: '/blog',
                    position: 'left',
                    label: 'Release Notes',
                },
                {
                    to: '/docs/contributing/overview',
                    position: 'left',
                    label: 'Contribute',
                    activeBaseRegex: '^/docs/contributing',
                },
                {
                    to: '/docs/author',
                    position: 'left',
                    label: 'Author',
                    activeBasePath: '/docs/author',
                },
                {
                    href: 'https://github.com/mxsm/rocketmq-rust',
                    label: 'GitHub',
                    position: 'right',
                },
                {
                    type: 'localeDropdown',
                    position: 'right',
                    dropdownItemsAfter: [
                        {
                            type: 'html',
                            value: '<hr style="margin: 0.3rem 0;">',
                        },
                        {
                            href: 'https://github.com/mxsm/rocketmq-rust/issues/new/choose',
                            label: 'Help us translate',
                        },
                    ],
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
                            to: '/docs/getting-started/installation',
                        },
                        {
                            label: 'Architecture',
                            to: '/docs/architecture/overview',
                        },
                        {
                            label: 'Contributing',
                            to: '/docs/contributing/overview',
                        },
                    ],
                },
                {
                    title: 'Community',
                    items: [
                        {
                            label: 'GitHub',
                            href: 'https://github.com/mxsm/rocketmq-rust',
                        },
                        {
                            label: 'Discussions',
                            href: 'https://github.com/mxsm/rocketmq-rust/discussions',
                        },
                        {
                            label: 'Issues',
                            href: 'https://github.com/mxsm/rocketmq-rust/issues',
                        },
                    ],
                },
                {
                    title: 'Resources',
                    items: [
                        {
                            label: 'Apache RocketMQ',
                            href: 'https://rocketmq.apache.org/',
                        },
                        {
                            label: 'Rust Official',
                            href: 'https://www.rust-lang.org/',
                        },
                        {
                            label: 'Stack Overflow',
                            href: 'https://stackoverflow.com/questions/tagged/rocketmq',
                        },
                    ],
                },
                {
                    title: 'Follow Us',
                    items: [
                        {
                            html: `
                <div style="display: flex; flex-direction: column; align-items: flex-start; padding: 0;">
                  <img 
                    src="/img/rocketmq-rustWeChat%20OfficialAccount.jpg" 
                    alt="WeChat Official Account" 
                    style="
                      width: 120px; 
                      height: 120px; 
                      border-radius: 8px; 
                      border: 2px solid rgba(168, 85, 247, 0.3);
                      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
                      margin-bottom: 8px;
                    "
                  />
                  <p style="
                    margin: 0; 
                    font-size: 0.813rem; 
                    color: var(--ifm-footer-link-color);
                    opacity: 0.8;
                  ">
                    WeChat Official Account
                  </p>
                </div>
              `,
                        },
                    ],
                },
            ],
            copyright: `Copyright © ${new Date().getFullYear()} RocketMQ-Rust Community. Built with Docusaurus.`,
        },
        prism: {
            theme: prismThemes.github,
            darkTheme: prismThemes.dracula,
            additionalLanguages: ['rust', 'java', 'yaml', 'bash', 'toml'],
        },
        algolia: {
            appId: 'A7P4XEL1X0',
            apiKey: 'db1ad68e86dd7d47a5988ab78dcc8347',
            indexName: 'crawler_rocketmqrust_index',
            contextualSearch: true,
        },
        // Using custom DevWarningBanner component instead of built-in announcementBar
    } satisfies PresetOptions['themeConfig'],

    themes: ['@docusaurus/theme-mermaid'],
};

export default config;
