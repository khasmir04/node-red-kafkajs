const { Kafka } = require('kafkajs')
const { onError, checkLastMessageTime } = require('../utils/common')

module.exports = function (RED) {
  const acksDict = {
    all: -1,
    none: 0,
    leader: 1,
  }

  function KafkajsProducerNode(config) {
    RED.nodes.createNode(this, config)
    const node = this
    node.ready = false
    node.successCount = 0
    node.failureCount = 0

    const client = RED.nodes.getNode(config.client)

    if (!client) {
      return
    }

    const kafka = new Kafka(client.options)

    const producerOptions = {
      metadataMaxAge: parseInt(config.metadatamaxage),
      allowAutoTopicCreation: config.allowautotopiccreation,
      transactionTimeout: parseInt(config.transactiontimeout),
    }

    const sendOptions = {
      topic: config.topic || null,
      partition: parseInt(config.partition) || null,
      key: config.key || null,
      headers: config.headeritems || {},
      acks: acksDict[config.acknowledge],
      timeout: parseInt(config.responsetimeout),
    }

    node.sendOptions = sendOptions

    const init = async () => {
      const producer = kafka.producer()
      node.producer = producer

      node.status({ fill: 'yellow', shape: 'ring', text: 'Initializing' })

      const onConnect = () => {
        node.ready = true
        node.lastMessageTime = new Date().getTime()
        node.status({ fill: 'green', shape: 'ring', text: 'Ready' })
      }

      const onDisconnect = () => {
        node.ready = false
        node.status({ fill: 'red', shape: 'ring', text: 'Offline' })
      }

      const onRequestTimeout = () => {
        node.status({ fill: 'red', shape: 'ring', text: 'Timeout' })
      }

      producer.on(producer.events.CONNECT, onConnect)
      producer.on(producer.events.DISCONNECT, onDisconnect)
      producer.on(producer.events.REQUEST_TIMEOUT, onRequestTimeout)

      try {
        await producer.connect()
      } catch (e) {
        onError(node, 'Kafka Producer Connect Error', null, e)
      }
    }

    init()

    node.interval = setInterval(() => checkLastMessageTime(node), 1000)

    node.on('input', (msg) => {
      if (node.ready && msg.payload) {
        const sendOptions = {
          ...node.sendOptions,
          topic: node.sendOptions.topic || msg.topic || null,
          messages: [
            {
              key: node.sendOptions.key || msg.key || null,
              headers: Object.keys(node.sendOptions.headers).length === 0 ? msg.headers : node.sendOptions.headers,
              partition: node.sendOptions.partition || msg.partition || null,
              value: msg.payload,
            },
          ],
        }

        node.producer
          .send(sendOptions)
          .then((result) => {
            node.successCount++
            node.log(`Kafka Producer Success (${node.successCount})`, result)
            node.status({
              fill: 'green',
              shape: 'dot',
              text: `Message Sent (${node.successCount})`,
            })

            const successData = {
              ...result[0],
              topic: sendOptions.topic,
              payload: sendOptions.messages[0].value,
            }

            delete successData.topicName

            if (config.enableOutputs) {
              node.send([successData, null])
            }
          })
          .catch((e) => {
            node.failureCount++
            onError(node, 'Kafka Producer Send Error', node.failureCount, e)

            const failureData = {
              topic: sendOptions.topic,
              payload: sendOptions.messages[0].value,
            }

            if (config.enableOutputs) {
              node.send([null, { error: e, failureData }])
            }
          })

        node.lastMessageTime = new Date().getTime()
        node.status({ fill: 'blue', shape: 'ring', text: 'Sending' })
      }
    })

    node.on('close', (done) => {
      node.producer
        .disconnect()
        .then(() => {
          node.status({})
          clearInterval(node.interval)
          done()
        })
        .catch((e) => {
          onError(node, 'Kafka Producer Close Error', null, e)
        })
    })
  }

  RED.nodes.registerType('kj-kafka-producer', KafkajsProducerNode, {
    outputs: 2,
  })
}
