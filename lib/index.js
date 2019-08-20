const pino = require('pino')
const joi = require('joi')
const Kafka = require('node-rdkafka')
const Package = require('../package.json')

const schema = joi.object().keys({
  host: joi.string(),
  port: joi.number(),
  brokerList: joi.string().allow(''),
  auth: joi.string().valid('SASL', 'NONE').required(),
  username: joi.string().when('auth', { is: 'SASL', then: joi.required(), otherwise: joi.allow('').optional() }),
  password: joi.string().when('auth', { is: 'SASL', then: joi.required(), otherwise: joi.allow('').optional() }),
  debug: joi.boolean().optional(),
  errorListener: joi.func().optional(),
}).xor('host', 'brokerList').with('host', 'port')

const defaults = {
  options: {
    auth: 'NONE',
    debug: true,
  },
  errorListener: (logger, reject) => (err) => {
    logger.error('Error from producer')
    logger.error(err)
    reject(err)
  },
}

const logger = pino({ name: 'hapi-kafka-plugin' })

const connect = (producer, config) => new Promise((resolve, reject) => {
  logger.info(`Connecting producer to broker: ${config['metadata.broker.list']}`)

  producer.connect()

  // Set listeners when the producer is ready o an error has been ocurred
  producer.on('event.error', defaults.errorListener(logger, reject))

  producer.on('ready', () => {
    logger.info('Producer is connected and ready')
    resolve(producer)
  })

  producer.on('connection.failure', (err) => {
    logger.error('An error has been ocurred when tried to connect to the broker')
    logger.error(err)
    reject(err)
  })
})

module.exports = {
  register: async (server, options) => {
    const config = Object.assign({}, defaults.options, options)
    const { error } = joi.validate(config, schema)

    // Validate incoming options for kafka client
    if (error) {
      logger.info('An error has been ocurred when validating options for hapi-kafka-client plugin')

      throw error
    }

    // Init kakfa producer with received options
    const broker = config.brokerList ? config.brokerList : `${config.host}:${config.port}`
    let producerConfig = {
      'client.id': 'kafka',
      'metadata.broker.list': broker,
      'retry.backoff.ms': 500,
      'message.send.max.retries': 10,
      'socket.keepalive.enable': true,
      'queue.buffering.max.messages': 100000,
      'queue.buffering.max.ms': 1000,
      'batch.num.messages': 10000,
      dr_cb: false,
    }

    if (config.auth === 'SASL') {
      producerConfig = {
        ...producerConfig,
        'security.protocol': 'sasl_ssl',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config.username, // your username or key if you are using confluent
        'sasl.password': config.password, // your password or secret if you are using confluent}
      }
    }

    const producer = new Kafka.Producer(producerConfig)

    await connect(producer, producerConfig)

    // Set poll interval to 10 seconds, instead of default value 5 minutes
    producer.setPollInterval(10000)

    // Set producer as a plugin property in the server
    server.expose('producer', producer)
  },
  name: 'kafka',
  version: Package.version,
}
