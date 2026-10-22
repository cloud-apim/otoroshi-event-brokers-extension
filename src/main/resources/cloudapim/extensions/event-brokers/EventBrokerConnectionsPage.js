
function input(type, label, props) {
  return {
    type: type,
    props: { label: label, ...props },
  };
}

const certificates = {
  valuesFrom: '/bo/api/proxy/api/certificates',
  transformer: (a) => ({ value: a.id, label: a.name }),
};

const connectionStatusColors = {
  connected: 'bg-success',
  connecting: 'bg-info',
  unavailable: 'bg-danger',
  closed: 'bg-secondary',
};

function connectionStatusBadge(status) {
  if (!status || !status.state) {
    return React.createElement('span', { className: 'badge bg-secondary' }, 'unknown');
  }
  const pending = status.pending ? ` (new settings: ${status.pending.state}${status.pending.error ? ' - ' + status.pending.error : ''})` : '';
  return React.createElement(
    'span',
    {
      className: `badge ${connectionStatusColors[status.state] || 'bg-secondary'}`,
      title: (status.error || status.state) + pending,
    },
    status.pending ? `${status.state} *` : status.state
  );
}

class EventBrokerConnectionsPage extends Component {

  formSchema = {
    _loc: {
      type: 'location',
      props: {},
    },
    id: { type: 'string', disabled: true, props: { label: 'Id', placeholder: '---' } },
    name: {
      type: 'string',
      props: { label: 'Name', placeholder: 'My Awesome Provider' },
    },
    description: {
      type: 'string',
      props: { label: 'Description', placeholder: 'Description of the Provider' },
    },
    metadata: {
      type: 'object',
      props: { label: 'Metadata' },
    },
    tags: {
      type: 'array',
      props: { label: 'Tags' },
    },
    'kind': {
      'type': 'select',
      props: { label: 'Kind', possibleValues: [
          { 'label': 'RabbitMQ', value: 'rabbitmq' },
          { 'label': 'Kafka', value: 'kafka' },
          { 'label': 'Pulsar', value: 'pulsar' },
          { 'label': 'MQTT', value: 'mqtt' },
          { 'label': 'Redis', value: 'redis' },
        ] }
    },
    'settings': {
      type: 'jsonobjectcode',
      props: { label: 'Settings' },
    },
    // shared
    'settings.default_topic': input('string', 'Default topic', { help: 'Used when a call names no topic' }),
    'settings.username': input('string', 'Username'),
    'settings.password': input('password', 'Password'),
    'settings.host': input('string', 'Host'),
    'settings.port': input('number', 'Port'),
    'settings.tls.enabled': input('bool', 'TLS'),
    'settings.tls.client_certs': input('array', 'Client certificates', certificates),
    'settings.tls.trusted_certs': input('array', 'Trusted certificates', certificates),
    'settings.tls.trust_all': input('bool', 'Trust every certificate'),
    'settings.tls.hostname_verification': input('bool', 'Hostname verification'),
    // kafka
    'settings.bootstrap_servers': input('array', 'Bootstrap servers', { placeholder: 'host:9092' }),
    'settings.security_protocol': input('select', 'Security protocol', {
      possibleValues: ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'].map((v) => ({ value: v, label: v })),
    }),
    'settings.sasl.mechanism': input('select', 'SASL mechanism', {
      possibleValues: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER', 'GSSAPI'].map((v) => ({ value: v, label: v })),
    }),
    'settings.sasl.username': input('string', 'SASL username'),
    'settings.sasl.password': input('password', 'SASL password'),
    'settings.sasl.jaas_config': input('string', 'JAAS config', { help: 'Instead of a username and a password' }),
    'settings.tls.keystore_path': input('string', 'Keystore file'),
    'settings.tls.truststore_path': input('string', 'Truststore file'),
    'settings.tls.store_password': input('password', 'Keystore and truststore password'),
    'settings.properties': input('object', 'Client properties', { help: 'Raw Kafka client properties, applied last' }),
    // rabbitmq
    'settings.addresses': input('array', 'Addresses', { placeholder: 'host:5672' }),
    'settings.virtual_host': input('string', 'Virtual host'),
    'settings.default_queue': input('string', 'Default queue', { help: 'Used when a call names no queue' }),
    // pulsar
    'settings.service_url': input('string', 'Service URL', { placeholder: 'pulsar://host:6650' }),
    'settings.tenant': input('string', 'Tenant'),
    'settings.namespace': input('string', 'Namespace'),
    'settings.auth.kind': input('select', 'Authentication', {
      possibleValues: [
        { value: 'none', label: 'None' },
        { value: 'token', label: 'Token' },
        { value: 'basic', label: 'Basic' },
      ],
    }),
    'settings.auth.token': input('password', 'Token'),
    'settings.auth.username': input('string', 'Username'),
    'settings.auth.password': input('password', 'Password'),
    'settings.tls.trust_certs_file_path': input('string', 'Trusted certificates file (pem)'),
    // redis
    'settings.uri': input('string', 'URI', { placeholder: 'redis://host:6379, rediss:// for TLS' }),
    'settings.default_channel': input('string', 'Default channel', { help: 'Used when a call names no channel' }),
    'settings.tls.start_tls': input('bool', 'STARTTLS'),
    'settings.tls.verify_peer': input('bool', 'Verify the server certificate'),
    // mqtt
    'settings.client_id_prefix': input('string', 'Client id prefix', { help: 'Every client adds a random suffix' }),
    'settings.default_qos': input('select', 'Default QoS', {
      possibleValues: [0, 1, 2].map((v) => ({ value: v, label: String(v) })),
    }),
    'settings.retain': input('bool', 'Retain published messages'),
    'settings.clean_session': input('bool', 'Clean session'),
    'settings.connection_timeout_seconds': input('number', 'Connection timeout (seconds)'),
    'settings.keep_alive_seconds': input('number', 'Keep alive (seconds)'),
  };

  columns = [
    {
      title: 'Name',
      filterId: 'name',
      content: (item) => item.name,
    },
    { title: 'Kind', filterId: 'kind', content: (item) => item.kind },
    {
      title: 'Status',
      notFilterable: true,
      notSortable: true,
      content: (item) => connectionStatusBadge(item._status),
    },
  ];

  // as seen by the node serving the backoffice: every node has its own clients
  fetchStatuses = () => {
    return fetch('/bo/api/proxy/api/extensions/cloud-apim/event-brokers/connections/_status', {
      method: 'GET',
      credentials: 'include',
      headers: { Accept: 'application/json' },
    })
      .then((r) => (r.status === 200 ? r.json() : { connections: [] }))
      .then((body) => _.keyBy(body.connections || [], 'id'))
      .catch(() => ({}));
  };

  fetchItems = () => {
    return Promise.all([this.client.findAll(), this.fetchStatuses()]).then(([items, statuses]) =>
      (items || []).map((item) => ({ ...item, _status: statuses[item.id] }))
    );
  };

  formFlow = (state) => {
    const head = ['_loc', 'id', 'name', 'description', '>>>Metadata and tags', 'tags', 'metadata', '<<<Connection', 'kind'];
    const tls = ['settings.tls.enabled', 'settings.tls.client_certs', 'settings.tls.trusted_certs', 'settings.tls.trust_all', 'settings.tls.hostname_verification'];
    switch (state.kind) {
      case 'kafka':
        return head.concat([
          'settings.bootstrap_servers',
          'settings.default_topic',
          '>>>Security',
          'settings.security_protocol',
          'settings.sasl.mechanism',
          'settings.sasl.username',
          'settings.sasl.password',
          'settings.sasl.jaas_config',
          '>>>TLS',
        ], tls, [
          'settings.tls.keystore_path',
          'settings.tls.truststore_path',
          'settings.tls.store_password',
          '>>>Client properties',
          'settings.properties',
        ]);
      case 'rabbitmq':
        return head.concat([
          'settings.addresses',
          'settings.virtual_host',
          'settings.default_queue',
          '>>>Authentication',
          'settings.username',
          'settings.password',
        ]);
      case 'pulsar':
        return head.concat([
          'settings.service_url',
          'settings.tenant',
          'settings.namespace',
          'settings.default_topic',
          '>>>Authentication',
          'settings.auth.kind',
        ], (state.settings && state.settings.auth && state.settings.auth.kind === 'token') ? ['settings.auth.token'] : [],
        (state.settings && state.settings.auth && state.settings.auth.kind === 'basic') ? ['settings.auth.username', 'settings.auth.password'] : [],
        ['>>>TLS'], tls, ['settings.tls.trust_certs_file_path']);
      case 'redis':
        return head.concat([
          'settings.uri',
          'settings.default_channel',
          '>>>Authentication',
          'settings.username',
          'settings.password',
          '>>>TLS',
          'settings.tls.start_tls',
          'settings.tls.verify_peer',
        ]);
      case 'mqtt':
        return head.concat([
          'settings.host',
          'settings.port',
          'settings.default_topic',
          'settings.default_qos',
          'settings.retain',
          '>>>Authentication',
          'settings.username',
          'settings.password',
          '>>>Session',
          'settings.client_id_prefix',
          'settings.clean_session',
          'settings.connection_timeout_seconds',
          'settings.keep_alive_seconds',
        ]);
      default:
        return head;
    }
  }

  // the server knows the defaults of every kind: the form starts from its template
  fetchTemplate = (kind) => {
    return fetch(`/bo/api/proxy/apis/event-brokers.extensions.cloud-apim.com/v1/event-broker-connections/_template?kind=${kind}`, {
      method: 'GET',
      credentials: 'include',
      headers: { Accept: 'application/json' },
    }).then((r) => r.json());
  };

  componentDidMount() {
    this.props.setTitle(`Event Broker Connections`);
  }

  client = BackOfficeServices.apisClient('event-brokers.extensions.cloud-apim.com', 'v1', 'event-broker-connections');

  render() {
    return (
      React.createElement(Table, {
        parentProps: this.props,
        selfUrl: "extensions/cloud-apim/event-brokers/connections",
        defaultTitle: "All event broker connections",
        defaultValue: () => this.fetchTemplate('kafka'),
        itemName: "Event Broker Connection",
        formSchema: this.formSchema,
        formFlow: this.formFlow,
        columns: this.columns,
        stayAfterSave: true,
        fetchItems: (paginationState) => this.fetchItems(),
        // the status is only shown, never saved
        updateItem: (item) => this.client.update(_.omit(item, ['_status'])),
        deleteItem: (item) => this.client.delete(_.omit(item, ['_status'])),
        createItem: (item) => this.client.create(_.omit(item, ['_status'])),
        navigateTo: (item) => {
          window.location = `/bo/dashboard/extensions/cloud-apim/event-brokers/connections/edit/${item.id}`
        },
        itemUrl: (item) => `/bo/dashboard/extensions/cloud-apim/event-brokers/connections/edit/${item.id}`,
        showActions: true,
        showLink: true,
        rowNavigation: true,
        extractKey: (item) => item.id,
        export: true,
        kubernetesKind: "event-brokers.extensions.cloud-apim.com/EventBrokerConnection",
        onStateChange: (state, oldState, update) => {
          if (oldState.kind && !_.isEqual(state.kind, oldState.kind)) {
            this.fetchTemplate(state.kind).then((template) => {
              update({ ...state, settings: template.settings });
            });
          }
        }
      }, null)
    );
  }
}