// @ts-check
import { themes as prismThemes } from 'prism-react-renderer';

const repo = 'https://github.com/cloud-apim/otoroshi-event-brokers-extension';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Event Brokers',
  tagline: 'Put Kafka, RabbitMQ, Pulsar, Redis, MQTT and Artemis behind your Otoroshi routes',
  favicon: 'img/favicon.svg',

  url: 'https://cloud-apim.github.io',
  baseUrl: '/otoroshi-event-brokers-extension/',

  organizationName: 'cloud-apim',
  projectName: 'otoroshi-event-brokers-extension',

  onBrokenLinks: 'throw',

  markdown: {
    // .md files are plain commonmark: a `<id>` or a `{"json": true}` in the text stays text
    format: 'detect',
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
      onBrokenMarkdownImages: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: `${repo}/edit/main/documentation/`,
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themes: [
    '@docusaurus/theme-mermaid',
    [
      // offline index: no Algolia account, no third-party request from the docs
      /** @type {import("@easyops-cn/docusaurus-search-local").PluginOptions} */
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
        indexBlog: false,
      },
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        defaultMode: 'dark',
        disableSwitch: false,
        respectPrefersColorScheme: true,
      },
      mermaid: {
        theme: { light: 'neutral', dark: 'dark' },
      },
      navbar: {
        title: 'Event Brokers',
        logo: {
          alt: 'Event Brokers',
          src: 'img/logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Documentation',
          },
          { href: `${repo}/releases/latest`, label: 'Download', position: 'right' },
          { href: repo, label: 'GitHub', position: 'right' },
          { href: 'https://www.cloud-apim.com', label: 'Cloud APIM', position: 'right' },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              { label: 'Overview', to: '/docs/overview' },
              { label: 'Quickstart', to: '/docs/quickstart' },
              { label: 'Connections', to: '/docs/connections/overview' },
              { label: 'Webhooks', to: '/docs/features/webhooks' },
              { label: 'Troubleshooting', to: '/docs/operations/troubleshooting' },
            ],
          },
          {
            title: 'Community',
            items: [
              { label: 'Discord', href: 'https://discord.cloud-apim.com' },
              { label: 'Twitter', href: 'https://twitter.com/cloudapim' },
              { label: 'Youtube', href: 'https://www.youtube.com/@CloudAPIM' },
            ],
          },
          {
            title: 'More',
            items: [
              { label: 'Cloud APIM', href: 'https://www.cloud-apim.com' },
              { label: 'GitHub', href: repo },
              { label: 'Otoroshi', href: 'https://maif.github.io/otoroshi/manual/docs/getting-started' },
              { label: 'CloudEvents', href: 'https://cloudevents.io' },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Cloud APIM. Built with Docusaurus.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        // only what the docs use: prism resolves no dependencies here, so a language that needs
        // another one (scala needs java needs clike) breaks the build
        additionalLanguages: ['bash', 'json', 'json5', 'http'],
      },
    }),
};

export default config;
