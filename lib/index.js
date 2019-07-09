const pino = require('pino')
const joi = require('joi')
const Kafka = require('node-rdkafka')
const Package = require('../package.json')

const schema = joi.object().keys({
  host: joi.string().required(),
  port: joi.number().required(),
  username: joi.string().required(),
  password: joi.string().required(),
  debug: joi.boolean().optional(),
  errorListener: joi.func().optional(),
})

const defaults = {
  options: {
    host: 'localhost',
    port: 9092,
    debug: true,
  },
  errorListener: logger => (err) => {
    logger.error('Error from producer')
    logger.error(err)
  },
}

module.exports = {
  register: async (server, options) => {
    const config = Object.assign({}, defaults.options, options)
    const logger = pino()
    const { error } = joi.validate(config, schema)

    // Validate incoming options for kafka client
    if (error) {
      logger.info('An error has been ocurred when validating options for hapi-kafka-client plugin')
    }

    // Init kakfa producer with received options
    const broker = `${config.host}:${config.port}`
    const producer = new Kafka.Producer({
      'metadata.broker.list': broker,
      'queue.buffering.max.kbytes': 1000000,
      'queue.buffering.max.ms': 1000,
      'client.id': 'kafka',
      'message.send.max.retries': 10,
      'socket.keepalive.enable': true,
      'queue.buffering.max.messages': 100000,
      'batch.num.messages': 1000000,
      dr_cb: true,
      'retry.backoff.ms': 500,
      'request.timeout.ms': 20000,
      'security.protocol': 'sasl_ssl',
      'sasl.mechanisms': 'PLAIN',
      'sasl.username': config.username, // your username or key if you are using confluent
      'sasl.password': config.password, // your password or secret if you are using confluent
    })

    // Poll for events every 100 ms
    producer.setPollInterval(100)

    logger.info(`Connecting producer to broker: ${broker}`)

    producer.connect()

    // Set producer as a plugin property in the server
    server.expose('producer', producer)

    // Set listeners when the producer is ready o an error has been ocurred
    producer.on('event.error', config.errorListener || defaults.errorListener(logger))

    producer.on('ready', () => {
      logger.info('Producer is connected and ready')
    })

    producer.on('connection.failure', (err) => {
      logger.error('An error has been ocurred when tried to connect to the broker')
      logger.error(err)
    })
  },
  name: 'kafka',
  version: Package.version,
}
