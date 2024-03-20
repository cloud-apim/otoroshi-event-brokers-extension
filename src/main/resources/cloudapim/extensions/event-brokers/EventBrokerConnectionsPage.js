
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
    'kind': {
      'type': 'select',
      props: { label: 'Description', possibleValues: [
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
      'settings',
    ]
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
        kubernetesKind: "EventBrokerConnection",
        onStateChange: (state, oldState, update) => {
          if (!_.isEqual(state.provider, oldState.provider)) {
            console.log("set default value")
            update({
              id: state.id,
              name: state.name,
              description: state.description,
              tags: state.tags,
              metadata: state.metadata,
              kind: 'rabbitmq',
              settings: {},
            });
          }
        }
      }, null)
    );
  }
}