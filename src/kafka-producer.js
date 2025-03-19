module.exports = function (RED) {
    const { Kafka } = require('kafkajs');

    const acksDict = {
        'all': -1,
        'none': 0,
        'leader': 1
    };

    function KafkajsProducerNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        node.ready = false;
        node.successCount = 0;
        node.failureCount = 0;

        let client = RED.nodes.getNode(config.client);

        let kafka = new Kafka(client.options);

        if (!client) {
            return;
        }

        var producerOptions = new Object();
        var sendOptions = new Object();
        sendOptions.topic = config.topic || null;

        producerOptions.metadataMaxAge = parseInt(config.metadatamaxage);
        producerOptions.allowAutoTopicCreation = config.allowautotopiccreation;
        producerOptions.transactionTimeout = parseInt(config.transactiontimeout);

        sendOptions.partition = parseInt(config.partition) || null;
        sendOptions.key = config.key || null;
        sendOptions.headers = config.headeritems || {};

        sendOptions.acks = acksDict[config.acknowledge];
        sendOptions.timeout = parseInt(config.responsetimeout);

        node.sendOptions = sendOptions;

        node.init = async function init() {
            // TODO: Add producer options in the argument
            const producer = kafka.producer();
            node.producer = producer;

            node.status({ fill: 'yellow', shape: 'ring', text: 'Initializing' });

            node.onConnect = function () {
                node.ready = true;
                node.lastMessageTime = new Date().getTime();
                node.status({ fill: 'green', shape: 'ring', text: 'Ready' });
            };

            node.onDisconnect = function () {
                node.ready = false;
                node.status({ fill: 'red', shape: 'ring', text: 'Offline' });
            };

            node.onRequestTimeout = function () {
                node.status({ fill: 'red', shape: 'ring', text: 'Timeout' });
            };

            producer.on(producer.events.CONNECT, node.onConnect);
            producer.on(producer.events.DISCONNECT, node.onDisconnect);
            producer.on(producer.events.REQUEST_TIMEOUT, node.onRequestTimeout);

            try {
                await producer.connect();
            } catch (err) {
                node.error('Kafka Producer Connect Error', err);
            }
        };

        node.init();

        function checkLastMessageTime() {
            if (node.lastMessageTime != null) {
                timeDiff = new Date().getTime() - node.lastMessageTime;
                if (timeDiff > 5000) {
                    node.status({ fill: 'yellow', shape: 'ring', text: 'Idle' });
                }
            }
        }

        node.interval = setInterval(checkLastMessageTime, 1000);

        node.on('input', function (msg) {
            if (node.ready) {
                if (msg.payload) {
                    var sendOptions = new Object();

                    sendOptions.topic = node.sendOptions.topic || msg.topic || null;
                    sendOptions.acks = node.sendOptions.acks || null;
                    sendOptions.timeout = node.sendOptions.timeout || null;

                    sendOptions.messages = [];
                    var message = new Object();

                    message.key = node.sendOptions.key || msg.key || null;

                    message.headers = node.sendOptions.headers;
                    message.headers =
                        Object.keys(message.headers).length === 0 ? msg.headers : message.headers;

                    message.partition = node.sendOptions.partition || msg.partition || null;

                    message.value = msg.payload;

                    sendOptions.messages.push(message);

                    // node.producer.send(sendOptions).catch((e) => {
                    //     node.error('Kafka Producer Error', e);
                    //     node.status({ fill: 'red', shape: 'ring', text: 'Error' });
                    // });

                    node.producer.send(sendOptions)
                        .then((result) => {
                            node.successCount++;
                            node.log(`Kafka Producer Success (${node.successCount})`, result);
                            node.status({ fill: 'green', shape: 'dot', text: `Message Sent (${node.successCount})` });

                            const successData = {
                              ...result[0],
                              topic: sendOptions.topic,
                              payload: sendOptions.messages[0].value,
                            }

                            delete successData.topicName;

                            if (config.enableOutputs) {
                              node.send([successData, null]);
                            }
                        })
                        .catch((e) => {
                            node.failureCount++;
                            node.error(`Kafka Producer Error (${node.failureCount})`, e);
                            node.status({ fill: 'red', shape: 'ring', text: `Error (${node.failureCount})` });

                            const failureData = {
                              topic: sendOptions.topic,
                              payload: sendOptions.messages[0].value,
                            }

                            if (config.enableOutputs) {
                              node.send([null, { error: e, failureData }]);
                            }
                        });

                    node.lastMessageTime = new Date().getTime();
                    node.status({ fill: 'blue', shape: 'ring', text: 'Sending' });
                }
            }
        });

        node.on('close', function (done) {
            node.producer
                .disconnect()
                .then(() => {
                    node.status({});
                    clearInterval(node.interval);
                    done();
                })
                .catch((e) => {
                    node.onError(e);
                });
        });
    }
    RED.nodes.registerType('kj-kafka-producer', KafkajsProducerNode, {
      outputs: 2,
    });
};
