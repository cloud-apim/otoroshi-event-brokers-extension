// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'overview',
    'quickstart',
    'install',
    {
      type: 'category',
      label: 'Connections',
      collapsed: false,
      items: [
        'connections/overview',
        'connections/kafka',
        'connections/rabbitmq',
        'connections/pulsar',
        'connections/redis',
        'connections/mqtt',
        'connections/artemis',
        'connections/tls-and-auth',
      ],
    },
    {
      type: 'category',
      label: 'Routes',
      collapsed: false,
      items: ['routes/plugins', 'routes/publishing', 'routes/consuming', 'routes/sse-and-websockets', 'routes/request-reply'],
    },
    'cloudevents',
    {
      type: 'category',
      label: 'Features',
      collapsed: false,
      items: ['features/webhooks', 'features/try-a-connection', 'features/data-exporter', 'features/workflows'],
    },
    {
      type: 'category',
      label: 'Operations',
      collapsed: false,
      items: ['operations/observability', 'operations/cluster', 'operations/troubleshooting'],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: false,
      items: ['reference/configuration', 'reference/options', 'reference/admin-api', 'reference/events', 'reference/plugins'],
    },
    'faq',
  ],
};

export default sidebars;
