
function input(type, label, props) {
  return {
    type: type,
    props: { label: label, ...props },
  };
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
          { 'label': 'Redis Pub/Sub', value: 'redis' },
        ] }
    },
    'settings': {
      type: 'jsonobjectcode',
      props: { label: 'Settings' },
    },
    'settings.servers': input('array', 'Server URIs'),
    'settings.keyPass': input('password', 'Keystore/Truststore password'),
    'settings.keystore': input('string', 'Keystore path'),
    'settings.truststore': input('string', 'Truststore path'),
    'settings.topic': input('string', 'Topic'),
    'settings.hostValidation': input('bool', 'Hostname validation'),
    'settings.mtlsConfig.mtls': input('bool', 'Custom TLS enabled'),
    'settings.mtlsConfig.loose': input('bool', 'TLS loose'),
    'settings.mtlsConfig.trustAll': input('bool', 'Trust all'),
    'settings.mtlsConfig.certs': input('select', 'Client cert', {
      placeholder: 'Choose a client certificate',
      valuesFrom: '/bo/api/proxy/api/certificates',
      transformer: (a) => ({
        value: a.id,
        label: a.name,
      }),
    }),
    'settings.mtlsConfig.trustedCerts': input('select', 'Trusted certs', {
      placeholder: 'Choose a trusted certificate',
      valuesFrom: '/bo/api/proxy/api/certificates',
      transformer: (a) => ({
        value: a.id,
        label: a.name,
      }),
     }),
    'settings.securityProtocol': input('select', 'Security Protocol', { 
      placeholder: 'Choose a security protocol',
      possibleValues: [
        { value: 'PLAINTEXT', label: "PLAINTEXT" },
        { value: 'SSL', label: "SSL" },
        { value: 'SASL_PLAINTEXT', label: "SASL_PLAINTEXT" },
        { value: 'SASL_SSL', label: "SASL_SSL" },
      ]
    }),
    'settings.saslConfig.username': input('string', 'Username'),
    'settings.saslConfig.password': input('string', 'Password'),
    'settings.saslConfig.mechanism': input('string', 'Auth. Mechanism'),
    'settings.publisher_id': input('string', 'Publisher id'),
    'settings.host': input('string', 'Host'),
    'settings.port': input('number', 'Port'),
    'settings.username': input('string', 'Username'),
    'settings.password': input('string', 'Password'),
    'settings.retained': input('bool', 'Retained'),
    'settings.qos': input('number', 'Message QoS'),
    'settings.uri': input('string', 'Server URI'),
    'settings.tenant': input('string', 'Tenant'),
    'settings.namespace': input('string', 'Namespace'),
    'settings.auth.kind': input('select', 'Authentication kind', { 
      possibleValues: [
        { value: 'basic', label: "basic" },
        { value: 'token', label: "token" },
        { value: 'disabled', label: "disabled" },
      ]
     }),
    'settings.auth.username': input('string', 'Username'),
    'settings.auth.password': input('string', 'Password'),
    'settings.uris': input('string', 'Server URIs'),
    'settings.channel': input('string', 'Channel'),
    'settings.virtual_host': input('string', 'Virtual host'),
    'settings.queue': input('string', 'Queue name'),
    'settings.routing_key': input('string', 'Routing key'),
  };

  columns = [
    {
      title: 'Name',
      filterId: 'name',
      content: (item) => item.name,
    },
    { title: 'Kind', filterId: 'kind', content: (item) => item.kind },
  ];

  formFlow = (state) => {
    if (!state.kind) {
      return [
        '_loc',
        'id',
        'name',
        'description',
        '>>>Metadata and tags',
        'tags',
        'metadata',
        '<<<Connection',
        'kind',
      ]
    }
    if (state.kind === "kafka") {
      return [
        '_loc',
        'id',
        'name',
        'description',
        '>>>Metadata and tags',
        'tags',
        'metadata',
        '<<<Connection options',
        'kind',
        'settings.servers',
        'settings.topic',
        '>>>TLS',
        'settings.keyPass',
        'settings.keystore',
        'settings.truststore',
        'settings.hostValidation',
        'settings.mtlsConfig.mtls',
        'settings.mtlsConfig.loose',
        'settings.mtlsConfig.trustAll',
        'settings.mtlsConfig.certs',
        'settings.mtlsConfig.trustedCerts',
        '>>> Authentication',
        'settings.securityProtocol',
        'settings.saslConfig.username',
        'settings.saslConfig.password',
        'settings.saslConfig.mechanism',
      ];
    }
    if (state.kind === "mqtt") {
      return [
        '_loc',
        'id',
        'name',
        'description',
        '>>>Metadata and tags',
        'tags',
        'metadata',
        '<<<Connection options',
        'kind',
        'settings.publisher_id',
        'settings.host',
        'settings.port',
        'settings.topic',
        'settings.retained',
        'settings.qos',
        '>>>Authentication',
        'settings.username',
        'settings.password',
      ];
    }
    if (state.kind === "pulsar") {
      return [
        '_loc',
        'id',
        'name',
        'description',
        '>>>Metadata and tags',
        'tags',
        'metadata',
        '<<<Connection options',
        'kind',
        'settings.uri',
        'settings.tenant',
        'settings.namespace',
        'settings.topic',
        '>>>Authentication',
        'settings.auth.kind',
        'settings.auth.username',
        'settings.auth.password',
      ];
    }
    if (state.kind === "redis") {
      return [
        '_loc',
        'id',
        'name',
        'description',
        '>>>Metadata and tags',
        'tags',
        'metadata',
        '<<<Connection options',
        'kind',
        'settings.uris',
        'settings.channel',
      ];
    }
    return [
      '_loc',
      'id',
      'name',
      'description',
      '>>>Metadata and tags',
      'tags',
      'metadata',
      '<<<Connection options',
      'kind',
      'settings.servers',
      'settings.virtual_host',
      'settings.queue',
      'settings.routing_key',
      '>>>Authentication',
      'settings.username',
      'settings.password',
    ];
  }

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
        defaultValue: () => {
          return {
            id: 'connection_' + uuid(),
            name: 'RabbitMQ connection',
            description: 'An RabbitMQ connection',
            tags: [],
            metadata: {},
            kind: 'rabbitmq',
            settings: {},
          }
        },
        itemName: "Event Broker Connection",
        formSchema: this.formSchema,
        formFlow: this.formFlow,
        columns: this.columns,
        stayAfterSave: true,
        fetchItems: (paginationState) => this.client.findAll(),
        updateItem: this.client.update,
        deleteItem: this.client.delete,
        createItem: this.client.create,
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
          if (!_.isEqual(state.kind, oldState.kind)) {
            if (state.kind === 'kafka') {
              update({
                id: state.id,
                name: state.name,
                description: state.description,
                tags: state.tags,
                metadata: state.metadata,
                kind: 'kafka',
                settings: {
                  'servers': ["127.0.0.1:9093"],
                  'topic': 'topic',
                  'securityProtocol': 'PLAINTEXT',
                  saslConfig: {
                    username: '',
                    password: '',
                    mechanism: 'PLAIN'
                  }
                },
              });
            } else if (state.kind === 'mqtt') {
              update({
                id: state.id,
                name: state.name,
                description: state.description,
                tags: state.tags,
                metadata: state.metadata,
                kind: 'mqtt',
                settings: {
                  'publisher_id': uuid(),
                  'host': 'localhost',
                  'port': 1883,
                  'topic': 'topic',
                  'username': '',
                  'password': '',
                  'qos': 0,
                  'retained': true,
                },
              });
            } else if (state.kind === 'pulsar') {
              update({
                id: state.id,
                name: state.name,
                description: state.description,
                tags: state.tags,
                metadata: state.metadata,
                kind: 'pulsar',
                settings: {
                  'uri': 'pulsar://localhost:6650',
                  'tenant': 'tenant',
                  'namespace': 'namespace',
                  'topic': 'topic',
                  'auth': {
                    kind: 'basic',
                    'username': '',
                    'password': '',
                  }
                },
              });
            } else if (state.kind === 'redis') {
              update({
                id: state.id,
                name: state.name,
                description: state.description,
                tags: state.tags,
                metadata: state.metadata,
                kind: 'redis',
                settings: {
                  'uris': ['redis://username@password:localhost:6379'],
                  'channel': '/',
                },
              });
            } else {
              update({
                id: state.id,
                name: state.name,
                description: state.description,
                tags: state.tags,
                metadata: state.metadata,
                kind: 'rabbitmq',
                settings: {
                  'servers': ['localhost:5672'],
                  'virtual_host': '/',
                  'queue': 'queue',
                  'routing_key': '',
                  'username': '',
                  'password': '',
                },
              });
            }
          }
        }
      }, null)
    );
  }
}