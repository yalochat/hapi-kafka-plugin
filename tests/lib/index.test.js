const Hapi = require('hapi')
const Kafka = require('node-rdkafka')
const KakfaClientPlugin = require('../../lib')

let server = null

beforeEach(async () => {
  server = Hapi.server({
    host: 'localhost',
  })

  await server.start()
})

afterEach(() => {
  server.stop()
})

describe('hapi kafka client plugin', () => {
  test('should be able to validate options and raise an error', async (done) => {
    try {
      await server.register({
        plugin: KakfaClientPlugin,
        options: {
          port: 'bad-port',
        },
      })
      done()
    } catch (err) {
      expect(err).toBeInstanceOf(Error)
      expect(err.name).toBe('ValidationError')
      done()
    }
  })

  test('should be able to  register a new producer', (done) => {
    server.register({
      plugin: KakfaClientPlugin,
      options: {},
    }).then(() => {
      expect(server.plugins).toHaveProperty('kafka')
      expect(server.plugins.kafka).toHaveProperty('producer')
      expect(server.plugins.kafka.producer).toBeInstanceOf(Kafka.Producer)
      done()
    })
  })
})
