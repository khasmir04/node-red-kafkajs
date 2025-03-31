const { createConsumerNode } = require('./create-consumer')

module.exports = function (RED) {
  function KafkajsConsumerNode(config) {
    RED.nodes.createNode(this, config)
    const node = this
    createConsumerNode(RED, node, config, false)
  }
  RED.nodes.registerType('kj-kafka-consumer', KafkajsConsumerNode)
}
