const fs = require('fs')
const { logLevel } = require('kafkajs')

module.exports = function (RED) {
  const dictLogLevel = {
    warn: logLevel.WARN,
    debug: logLevel.DEBUG,
    info: logLevel.INFO,
    none: logLevel.NOTHING,
    error: logLevel.ERROR,
  }

  function KafkajsClientNode(config) {
    RED.nodes.createNode(this, config)
    const node = this

    const options = {}
    options.brokers = config.brokers.replace(' ', '').split(',')
    options.clientId = config.clientid
    options.logLevel = dictLogLevel[config.logLevel]
    options.connectionTimeout = parseInt(config.connectiontimeout)
    options.requestTimeout = parseInt(config.requesttimeout)

    if (config.advancedretry) {
      options.retry = {}
      options.retry.maxRetryTime = parseInt(config.maxretrytime)
      options.retry.initialRetryTime = parseInt(config.initialretrytime)
      options.retry.factor = parseFloat(config.factor)
      options.retry.multiplier = parseInt(config.multiplier)
      options.retry.retries = parseInt(config.retries)
    }

    if (config.auth === 'tls') {
      options.ssl = {}
      options.ssl.ca = [fs.readFileSync(config.tlscacert, 'utf-8')]
      options.ssl.cert = fs.readFileSync(config.tlsclientcert, 'utf-8')
      options.ssl.key = fs.readFileSync(config.tlsprivatekey, 'utf-8')
      options.ssl.passphrase = config.tlspassphrase
      options.ssl.rejectUnauthorized = config.tlsselfsign
    } else if (config.auth === 'sasl') {
      options.ssl = config.saslssl

      options.sasl = {}
      options.sasl.mechanism = config.saslmechanism || 'plain'
      options.sasl.username = config.saslusername
      options.sasl.password = config.saslpassword

      // ! Disabled node credentials. Use node configuration instead.
      // options.sasl.username = node.credentials.saslusername;
      // options.sasl.password = node.credentials.saslpassword;
    }

    node.options = options
  }

  RED.nodes.registerType('kj-kafka-client', KafkajsClientNode)

  // ! Disabled node credentials. Use node configuration instead.
  // RED.nodes.registerType('kj-kafka-client', KafkajsClientNode, {
  //     credentials: {
  //         saslusername: { type: 'text' },
  //         saslpassword: { type: 'password' }
  //     }
  // });
}
