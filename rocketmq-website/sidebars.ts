import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'introduction',
    'overview/capability-matrix',
    {
      type: 'category',
      label: 'Getting Started',
      collapsible: true,
      collapsed: false,
      items: [
        'getting-started/installation',
        'getting-started/local-source',
        'getting-started/quick-start',
        'getting-started/basic-concepts',
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      collapsible: true,
      collapsed: false,
      items: [
        'architecture/overview',
        'architecture/module-map',
        'architecture/message-lifecycle',
        'architecture/runtime',
        'architecture/message-model',
        'architecture/storage',
      ],
    },
    {
      type: 'category',
      label: 'Producer',
      collapsible: true,
      collapsed: true,
      items: [
        'producer/overview',
        'producer/sending-messages',
        'producer/transaction-messages',
      ],
    },
    {
      type: 'category',
      label: 'Consumer',
      collapsible: true,
      collapsed: true,
      items: [
        'consumer/overview',
        'consumer/push-consumer',
        'consumer/pull-consumer',
        'consumer/message-filtering',
      ],
    },
    {
      type: 'category',
      label: 'Application Guides',
      collapsible: true,
      collapsed: true,
      items: ['guides/delivery-and-retry'],
    },
    {
      type: 'category',
      label: 'Deployment',
      collapsible: true,
      collapsed: true,
      items: ['deployment/overview'],
    },
    {
      type: 'category',
      label: 'Operations',
      collapsible: true,
      collapsed: true,
      items: ['operations/first-diagnosis'],
    },
    {
      type: 'category',
      label: 'Configuration',
      collapsible: true,
      collapsed: true,
      items: [
        'configuration/broker-config',
        'configuration/client-config',
        'configuration/performance-tuning',
        'configuration/observability',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsible: true,
      collapsed: true,
      items: ['reference/index'],
    },
    {
      type: 'category',
      label: 'Ecosystem',
      collapsible: true,
      collapsed: true,
      items: ['ecosystem/overview'],
    },
    {
      type: 'category',
      label: 'FAQ',
      collapsible: true,
      collapsed: true,
      items: [
        'faq/common-issues',
        'faq/performance',
        'faq/troubleshooting',
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      collapsible: true,
      collapsed: true,
      items: [
        'contributing/overview',
        'contributing/development-guide',
        'contributing/documentation',
        'contributing/coding-standards',
      ],
    },
    'author',
  ],
};

export default sidebars;
