const { Kafka, CompressionTypes, CompressionCodecs } = require('kafkajs')
const { v4: uuidv4 } = require('uuid')
const SnappyCodec = require('kafkajs-snappy')
const { onError, checkLastMessageTime } = require('../utils/common')

const createConsumerNode = (RED, node, config, isManual) => {
  const client = RED.nodes.getNode(config.client)

  if (!client) return

  const kafka = new Kafka(client.options)
  const consumerOptions = {
    groupId: config.groupid ? config.groupid : 'kj_kafka_' + uuidv4(),
  }

  if (config.useSnappyCompression) {
    CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec
  }

  const subscribeOptions = {
    topic: config.topic,
  }

  const runOptions = {}

  if (config.advancedoptions) {
    consumerOptions.sessionTimeout = parseInt(config.sessiontimeout)
    consumerOptions.rebalanceTimeout = parseInt(config.rebalancetimeout)
    consumerOptions.heartbeatInterval = parseInt(config.heartbeatinterval)
    consumerOptions.metadataMaxAge = parseInt(config.metadatamaxage)
    consumerOptions.maxBytesPerPartition = parseInt(config.maxbytesperpartition)
    consumerOptions.minBytes = parseInt(config.minbytes)
    consumerOptions.maxBytes = parseInt(config.maxbytes)
    consumerOptions.maxWaitTimeInMs = parseInt(config.maxwaittimeinms)
    consumerOptions.allowAutoTopicCreation = config.allowautotopiccreation

    subscribeOptions.fromBeginning = config.frombeginning

    runOptions.autoCommitInterval = parseInt(config.autocommitinterval)
    runOptions.autoCommitThreshold = parseInt(config.autocommitthreshold)
  }

  const init = async () => {
    node.running = true
    if (config.advancedoptions && config.clearoffsets) {
      node.status({ fill: 'yellow', shape: 'ring', text: 'Clearing Offset' })
      const admin = kafka.admin()
      await admin.connect()
      await admin.resetOffsets({ groupId: config.groupid, topic: config.topic })
      await admin.disconnect()
    }

    node.consumer = kafka.consumer(consumerOptions)
    node.status({ fill: 'yellow', shape: 'ring', text: 'Initializing' })

    const onConnect = () => {
      node.ready = true
      node.lastMessageTime = new Date().getTime()
      node.status({ fill: 'green', shape: 'ring', text: 'Ready' })
    }

    const onDisconnect = () => {
      node.status({ fill: 'red', shape: 'ring', text: 'Offline' })
    }

    const onRequestTimeout = () => {
      node.status({ fill: 'red', shape: 'ring', text: 'Timeout' })
    }

    const onHeartbeat = (event) => {
      try {
        heartbeatCount++

        const logHeartbeat = (message) => {
          console.log(
            `[${new Date().toISOString()}] ${message}: { groupId: ${
              event.payload.groupId
            }, memberId: ${event.payload.memberId} }`,
          )
        }

        if (!firstHeartbeatLogged) {
          logHeartbeat('First heartbeat received')
          firstHeartbeatLogged = true
        } else if (heartbeatCount % HEARTBEAT_INTERVAL === 0) {
          logHeartbeat(`Heartbeat is working (Total: ${heartbeatCount})`)
        }
      } catch (error) {
        console.error(`Error processing heartbeat event: ${error.message}`)
      }
    }

    const onCrash = () => {
      node.status({ fill: 'red', shape: 'ring', text: `${isManual ? 'Manual ' : ''}Consumer has crashed` })
    }

    const onMessage = (topic, partition, message) => {
      node.lastMessageTime = new Date().getTime()
      const payload = {
        topic,
        partition,
        payload: {
          ...message,
          key: message.key ? message.key.toString() : null,
          value: message.value.toString(),
          headers: Object.fromEntries(Object.entries(message.headers).map(([key, value]) => [key, value.toString()])),
        },
      }

      node.send(payload)
      node.status({ fill: 'blue', shape: 'ring', text: 'Reading' })
    }

    node.interval = setInterval(() => checkLastMessageTime(node), 1000)

    await node.consumer.connect()

    let firstHeartbeatLogged = false
    let heartbeatCount = 0
    const HEARTBEAT_INTERVAL = 50

    node.consumer.on(node.consumer.events.CONNECT, onConnect)
    node.consumer.on(node.consumer.events.DISCONNECT, onDisconnect)
    node.consumer.on(node.consumer.events.REQUEST_TIMEOUT, onRequestTimeout)
    node.consumer.on(node.consumer.events.HEARTBEAT, onHeartbeat)
    node.consumer.on(node.consumer.events.CRASH, onCrash)

    await node.consumer.subscribe(subscribeOptions)

    runOptions.eachMessage = async ({ topic, partition, message }) => {
      onMessage(topic, partition, message)
    }

    await node.consumer.run(runOptions)
  }

  node.on('close', (done) => {
    node.consumer
      .disconnect()
      .then(() => {
        node.ready = false
        node.running = false
        node.status({
          fill: 'red',
          shape: 'ring',
          text: `Kafka ${isManual ? 'Manual' : ''} Consumer Closed`,
        })
        clearInterval(node.interval)
        done()
      })
      .catch((e) => {
        node.ready = false
        node.running = false
        onError(node, `Kafka ${isManual ? 'Manual' : ''} Consumer Close Error`, null, e)
        done()
      })
  })

  if (!isManual) {
    init().catch((e) => {
      node.ready = false
      node.running = false
      onError(node, 'Kafka Consumer Init Error', null, e)
    })
  } else {
    node.on('input', (msg) => {
      let enabledValue = config.consumerEnabled
      try {
        enabledValue = RED.util.evaluateNodeProperty(config.consumerEnabled, config.consumerEnabledType, node, msg)
      } catch (e) {
        node.status({ fill: 'red', shape: 'ring', text: 'Enabled not set' })
        node.ready = false
        node.running = false
        onError(node, 'Kafka Manual Consumer Enabled Error', null, e)
        return
      }

      if (!enabledValue) {
        if (!node.running || !node.ready) {
          node.status({ fill: 'red', shape: 'ring', text: 'Already Disabled' })
          return
        }
        if (node.running || node.ready) {
          node.consumer
            .disconnect()
            .then(() => {
              node.running = false
              node.ready = false
              node.status({
                fill: 'red',
                shape: 'ring',
                text: `Kafka ${isManual ? 'Manual' : ''} Consumer Closed`,
              })
              clearInterval(node.interval)
            })
            .catch((e) => {
              node.ready = false
              node.running = false
              onError(node, `Kafka ${isManual ? 'Manual' : ''} Consumer Close Error`, null, e)
            })
          node.status({ fill: 'red', shape: 'ring', text: 'Disabled' })
          return
        }
      }

      if (enabledValue) {
        if (node.running || node.ready) {
          node.status({ fill: 'yellow', shape: 'ring', text: 'Already Running' })
          return
        }
        init().catch((e) => {
          node.ready = false
          node.running = false
          onError(node, 'Kafka Manual Consumer Input Error', null, e)
        })
      }
    })
  }
}

module.exports = { createConsumerNode }
