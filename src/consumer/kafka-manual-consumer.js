const { createConsumerNode } = require('./create-consumer')

module.exports = function (RED) {
  function KafkajsConsumerNode(config) {
    RED.nodes.createNode(this, config)
    const node = this
    createConsumerNode(RED, node, config, true)
  }
  RED.nodes.registerType('kj-kafka-manual-consumer', KafkajsConsumerNode)
}
