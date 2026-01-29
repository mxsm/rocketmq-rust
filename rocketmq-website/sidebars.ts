import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'introduction',
    {
      type: 'category',
      label: 'Getting Started',
      collapsible: true,
      collapsed: false,
      items: [
        'getting-started/installation',
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
      label: 'Configuration',
      collapsible: true,
      collapsed: true,
      items: [
        'configuration/broker-config',
        'configuration/client-config',
        'configuration/performance-tuning',
      ],
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
        'contributing/coding-standards',
      ],
    },
    'author',
  ],
};

export default sidebars;
