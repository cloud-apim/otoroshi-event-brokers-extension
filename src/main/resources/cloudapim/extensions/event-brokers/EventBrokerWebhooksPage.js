// inlined after EventBrokerConnectionsPage.js, whose `input`, `certificates` and `saved` it uses

const connectionsOptions = {
  valuesFrom: '/bo/api/proxy/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-connections',
  transformer: (a) => ({ value: a.id, label: `${a.name} (${a.broker})` }),
};

class EventBrokerWebhooksPage extends Component {

  formSchema = {
    _loc: {
      type: 'location',
      props: {},
    },
    id: { type: 'string', disabled: true, props: { label: 'Id', placeholder: '---' } },
    name: { type: 'string', props: { label: 'Name', placeholder: 'Orders to billing' } },
    description: { type: 'string', props: { label: 'Description', placeholder: 'Description of the webhook' } },
    metadata: { type: 'object', props: { label: 'Metadata' } },
    tags: { type: 'array', props: { label: 'Tags' } },
    enabled: input('bool', 'Enabled'),
    connection: input('select', 'Connection', connectionsOptions),
    subscription: {
      type: 'jsonobjectcode',
      props: {
        label: 'Subscription',
        help: 'The options of a consumer: {"topic": "orders", "from": "earliest", "group": "...", "selector": "..."}',
      },
    },
    'endpoint.url': input('string', 'URL', { placeholder: 'https://billing.internal/events' }),
    'endpoint.method': input('select', 'Method', { possibleValues: ['POST', 'PUT', 'PATCH'].map((v) => ({ value: v, label: v })) }),
    'endpoint.mode': input('select', 'Mode', {
      possibleValues: [
        { value: 'structured', label: 'Structured (the whole cloudevent as json)' },
        { value: 'binary', label: 'Binary (ce- headers, the data as the body)' },
        { value: 'raw', label: 'Raw (the data alone)' },
      ],
    }),
    'endpoint.headers': input('object', 'Headers'),
    'endpoint.timeout_ms': input('number', 'Timeout (ms)'),
    'endpoint.secret': input('password', 'Signing secret', { help: 'Signs every body: X-Event-Brokers-Signature: sha256=<hmac>' }),
    'endpoint.tls.enabled': input('bool', 'Custom TLS'),
    'endpoint.tls.client_certs': input('array', 'Client certificates', certificates),
    'endpoint.tls.trusted_certs': input('array', 'Trusted certificates', certificates),
    'endpoint.tls.trust_all': input('bool', 'Trust every certificate'),
    'endpoint.tls.hostname_verification': input('bool', 'Hostname verification'),
    'retry.max_attempts': input('number', 'Attempts', { help: 'In all, the first one included' }),
    'retry.initial_backoff_ms': input('number', 'First wait (ms)', { help: 'Twice as long before each next attempt' }),
    'retry.max_backoff_ms': input('number', 'Longest wait (ms)'),
    max_in_flight: input('number', 'Deliveries at once', { help: '1 keeps the order of the events' }),
    'dead_letter.connection': input('select', 'Dead letters connection', connectionsOptions),
    'dead_letter.options': {
      type: 'jsonobjectcode',
      props: { label: 'Dead letters', help: 'Where an event given up on is published: {"topic": "orders-dlq"}' },
    },
  };

  columns = [
    { title: 'Name', filterId: 'name', content: (item) => item.name },
    { title: 'URL', filterId: 'endpoint.url', content: (item) => (item.endpoint || {}).url },
    {
      title: 'Status',
      notFilterable: true,
      notSortable: true,
      content: (item) => {
        const status = item._status;
        if (!item.enabled) return React.createElement('span', { className: 'badge bg-secondary' }, 'disabled');
        if (!status) return React.createElement('span', { className: 'badge bg-secondary' }, 'unknown');
        const label = status.delivering ? `delivering (${status.delivered})` : status.owner ? `on ${status.owner}` : 'waiting';
        const color = status.last_error ? 'bg-warning' : status.delivering || status.owner ? 'bg-success' : 'bg-secondary';
        return React.createElement('span', { className: `badge ${color}`, title: status.last_error ? status.last_error.message : '' }, label);
      },
    },
  ];

  formFlow = [
    '_loc', 'id', 'name', 'description', 'enabled',
    '>>>Metadata and tags', 'tags', 'metadata',
    '<<<Events', 'connection', 'subscription',
    '>>>Endpoint', 'endpoint.url', 'endpoint.method', 'endpoint.mode', 'endpoint.headers', 'endpoint.timeout_ms', 'endpoint.secret',
    '>>>Endpoint TLS', 'endpoint.tls.enabled', 'endpoint.tls.client_certs', 'endpoint.tls.trusted_certs', 'endpoint.tls.trust_all', 'endpoint.tls.hostname_verification',
    '>>>Delivery', 'retry.max_attempts', 'retry.initial_backoff_ms', 'retry.max_backoff_ms', 'max_in_flight',
    '>>>Dead letters', 'dead_letter.connection', 'dead_letter.options',
  ];

  state = { node: null };

  // as seen by the node serving the backoffice: the one holding a webhook delivers it
  fetchStatuses = () => {
    return fetch('/bo/api/proxy/api/extensions/cloud-apim/event-brokers/webhooks/_status', {
      method: 'GET',
      credentials: 'include',
      headers: { Accept: 'application/json' },
    })
      .then((r) => (r.status === 200 ? r.json() : {}))
      .catch(() => ({}));
  };

  fetchItems = () => {
    return Promise.all([this.client.findAll(), this.fetchStatuses()]).then(([items, body]) => {
      this.setState({ node: body.node || null });
      const statuses = _.keyBy(body.webhooks || [], 'id');
      return (items || []).map((item) => ({ ...item, _status: statuses[item.id] }));
    });
  };

  // why this node delivers nothing, when it does not: its badges alone would only say waiting
  nodeWarning = () => {
    const node = this.state.node;
    if (!node) return null;
    if (node.enabled === false) return 'This node delivers no webhook: they are turned off (webhooks.enabled).';
    if (node.locks && node.locks.kind === 'none') return `This node delivers no webhook: ${node.locks.error}.`;
    return null;
  };

  fetchTemplate = () => {
    return fetch('/bo/api/proxy/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-webhooks/_template', {
      method: 'GET',
      credentials: 'include',
      headers: { Accept: 'application/json' },
    }).then((r) => r.json());
  };

  componentDidMount() {
    this.props.setTitle(`Event Broker Webhooks`);
  }

  client = BackOfficeServices.apisClient('event-brokers.extensions.cloud-apim.com', 'v1', 'event-broker-webhooks');

  // a dead letters section left empty means none
  clean = (item) => {
    const webhook = _.omit(item, ['_status']);
    const dead = webhook.dead_letter || {};
    if (!dead.connection && _.isEmpty(dead.options)) webhook.dead_letter = null;
    return webhook;
  };

  render() {
    const warning = this.nodeWarning();
    return React.createElement('div', null, warning ? React.createElement('div', { key: 'w', className: 'alert alert-warning' }, warning) : null, React.createElement(Table, {
      key: 't',
      parentProps: this.props,
      selfUrl: 'extensions/cloud-apim/event-brokers/webhooks',
      defaultTitle: 'All event broker webhooks',
      defaultValue: () => this.fetchTemplate(),
      itemName: 'Event Broker Webhook',
      formSchema: this.formSchema,
      formFlow: this.formFlow,
      columns: this.columns,
      stayAfterSave: true,
      fetchItems: (paginationState) => this.fetchItems(),
      updateItem: (item) => saved(this.client.update(this.clean(item)), 'Webhook'),
      deleteItem: (item) => this.client.delete(this.clean(item)),
      createItem: (item) => saved(this.client.create(this.clean(item)), 'Webhook'),
      navigateTo: (item) => {
        window.location = `/bo/dashboard/extensions/cloud-apim/event-brokers/webhooks/edit/${item.id}`;
      },
      itemUrl: (item) => `/bo/dashboard/extensions/cloud-apim/event-brokers/webhooks/edit/${item.id}`,
      showActions: true,
      showLink: true,
      rowNavigation: true,
      extractKey: (item) => item.id,
      export: true,
      kubernetesKind: 'event-brokers.extensions.cloud-apim.com/EventBrokerWebhook',
    }, null));
  }
}
