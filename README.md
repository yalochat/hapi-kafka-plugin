# Hapi Kafka Plugin

It's a plugin for Hapi.js with support until the version 16 that implement a Kafka client using [node-rdkafka](https://github.com/Blizzard/node-rdkafka)

## Usage

Install Hapi Kafka Plugin using [npm](https://www.npmjs.com/):

```shell
npm install --save hapi-kafka-plugin
```

To register the plugin you must `require` it

```javascript
const KafkaPlugin = require('hapi-kafka-plugin')
```

## Configuration

The plugin require some paremeteres listed bellow:

| Parameter | Description | Type | Required | Example |
|---|---|---|---|---|
| host | Kafka broker host | string | yes | `localhost` or `kafka` (if you are using [Docker](https://www.docker.com/)) |
| port | Kafka broker port | number | yes | `9092` |
| debug | Flag that allows to the package write logs in console | boolean | no (default value: `true`) | `true` or `false` |
| errorListener | Listener for any error encounter | function | no | `(eror) => { console.error('An error has been ocurred', error) }`|

## How to use it

You have to register the plugin in the hapi.js server, and then your are going to have a property in the plugin's servers that allows you to access to the kafka producer connection.

```javascript
const KafkaPlugin = require('hapi-kafka-plugin')

server.register({
  register: KafkaPlugin,
  options: {
    host: 'localhost',
    port: 9092,
  }
}).then(() => {
  console.log('Kafka has been started')

  // And now is available in the server
  server.plugins.kafka.producer.produce(
    'topic',
    null,
    Buffer.from('hello world'),
    null,
    Date.now(),
  )
})
```

## Tests

Before you run your tests, you have to run a Kafka container configured con Zookeper, you need to install [Docker](https://docker.com) and then run the [docker image of Kakfa](https://github.com/spotify/docker-kafka)

```shell
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`docker-machine ip \`docker-machine active\`` --env ADVERTISED_PORT=9092 spotify/kafka
```

Tests are written in [Jest](https://facebook.github.io/jest) to tests Javascript code.

```shell
npm test
```

---

Created with :heart: by [Yalo](https://github.com/yalochat)
