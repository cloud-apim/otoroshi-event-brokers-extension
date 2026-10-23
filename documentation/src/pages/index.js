import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import CodeBlock from '@theme/CodeBlock';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './index.module.css';

const brokers = ['Kafka', 'RabbitMQ', 'Pulsar', 'Redis pub/sub', 'Redis streams', 'MQTT 5 and 3.1.1', 'ActiveMQ Artemis'];

const capabilities = [
  {
    kicker: 'Publishing',
    title: 'Publish with a plain HTTP call',
    body: 'One event, a batch, or a stream of lines: the ingester answers one result per event, once the broker took it, with its position. Nothing is lost in silence.',
  },
  {
    kicker: 'Consuming',
    title: 'JSON lines, server-sent events, websockets',
    body: 'Stream events to any HTTP client. Groups, at-least-once delivery, replay from the beginning, a date or an offset, and server-sent events that resume where the client stopped.',
  },
  {
    kicker: 'Standard',
    title: 'Every event is a CloudEvent',
    body: 'With the protocol binding of each broker: what the gateway publishes, any CloudEvents SDK reads, and what another producer published is read as an event too.',
  },
  {
    kicker: 'Each broker at its best',
    title: 'Not the lowest common denominator',
    body: 'Kafka keys and offsets, RabbitMQ confirms, topologies and streams, Pulsar subscriptions, Redis streams groups, MQTT 5 shared subscriptions, Artemis addresses, selectors and message groups.',
  },
  {
    kicker: 'Webhooks',
    title: 'Events pushed to HTTP endpoints',
    body: 'Signed, retried with backoff, dead-lettered when they cannot go, delivered by one node of the cluster at a time.',
  },
  {
    kicker: 'Security',
    title: 'TLS and authentication everywhere',
    body: 'Mutual TLS with the certificates of the Otoroshi store, SASL, tokens, secrets from vaults. And the route decides what its callers may ask: nothing but shaping their own response, by default.',
  },
  {
    kicker: 'Integrations',
    title: 'Exporter, workflows, request reply',
    body: 'Send Otoroshi events to any broker, publish and consume from workflows, or answer an HTTP call with the event that replies to it.',
  },
  {
    kicker: 'Operations',
    title: 'Connections that take care of themselves',
    body: 'Connected in the background, checked, rebuilt, switched to new settings without a dropped request. Their state, their traffic and the lag of their groups in the admin API, the backoffice, and the metrics.',
  },
];

const quickstart = `# publish
curl -X POST 'http://events.oto.tools:8080/' \\
  -H 'Content-Type: application/json' -d '{"order": 42}'

# consume, as server-sent events
curl -N 'http://events-stream.oto.tools:8080/'`;

function Hero() {
  return (
    <header className={styles.hero}>
      <div className="container">
        <img className={styles.heroLogo} src={useBaseUrl('/img/logo.svg')} alt="Event Brokers" />
        <div className={styles.heroEyebrow}>Cloud APIM · Otoroshi extension</div>
        <Heading as="h1" className={styles.heroTitle}>
          Put your <span className={styles.heroAccent}>event brokers</span> behind your routes
        </Heading>
        <p className={styles.heroSubtitle}>
          Publish events with a plain HTTP call, consume them as JSON lines, server-sent events or over a
          websocket, push them to webhooks. The brokers stay private, <a href="https://www.otoroshi.io">Otoroshi</a>{' '}
          decides who reaches them.
        </p>
        <div className={styles.heroButtons}>
          <Link className={styles.buttonPrimary} to="/docs/overview">
            Read the docs
          </Link>
          <Link className={styles.buttonGhost} to="/docs/quickstart">
            Quickstart
          </Link>
          <Link className={styles.buttonGhost} href="https://github.com/cloud-apim/otoroshi-event-brokers-extension">
            GitHub
          </Link>
        </div>
        <div className={styles.brokers}>
          {brokers.map((b) => (
            <span key={b} className={styles.broker}>
              {b}
            </span>
          ))}
        </div>
      </div>
    </header>
  );
}

export default function Home() {
  return (
    <Layout title="Event Brokers for Otoroshi" description="Put Kafka, RabbitMQ, Pulsar, Redis, MQTT and Artemis behind your Otoroshi routes">
      <Hero />
      <main>
        <section className={styles.section}>
          <div className="container">
            <Heading as="h2">What it does</Heading>
            <div className={styles.grid}>
              {capabilities.map((c) => (
                <div key={c.title} className={styles.card}>
                  <div className={styles.cardKicker}>{c.kicker}</div>
                  <Heading as="h3" className={styles.cardTitle}>
                    {c.title}
                  </Heading>
                  <p className={styles.cardBody}>{c.body}</p>
                </div>
              ))}
            </div>
          </div>
        </section>
        <section className={styles.sectionAlt}>
          <div className="container">
            <Heading as="h2">Two routes, and your broker is on the web</Heading>
            <p>
              A connection to the broker, an ingester route, a consumer route. <Link to="/docs/quickstart">The quickstart</Link>{' '}
              takes five minutes with Docker.
            </p>
            <CodeBlock language="bash">{quickstart}</CodeBlock>
          </div>
        </section>
      </main>
    </Layout>
  );
}
