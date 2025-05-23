module.exports = function (RED) {
    const SnappyCodec = require('kafkajs-snappy');
    const { Kafka, CompressionTypes, CompressionCodecs } = require('kafkajs');
    const { v4: uuidv4 } = require('uuid');

    function KafkajsConsumerNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;

        let client = RED.nodes.getNode(config.client);

        if (!client) {
            return;
        }

        const kafka = new Kafka(client.options);

        let consumerOptions = new Object();
        consumerOptions.groupId = config.groupid ? config.groupid : 'kj-kafka-' + uuidv4();

        if (config.useSnappyCompression) {
          CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;
        }

        let subscribeOptions = new Object();
        subscribeOptions.topic = config.topic;

        let runOptions = new Object();

        if (config.advancedoptions) {
            consumerOptions.sessionTimeout = parseInt(config.sessiontimeout);
            consumerOptions.rebalanceTimeout = parseInt(config.rebalancetimeout);
            consumerOptions.heartbeatInterval = parseInt(config.heartbeatinterval);
            consumerOptions.metadataMaxAge = parseInt(config.metadatamaxage);
            consumerOptions.maxBytesPerPartition = parseInt(config.maxbytesperpartition);
            consumerOptions.minBytes = parseInt(config.minbytes);
            consumerOptions.maxBytes = parseInt(config.maxbytes);
            consumerOptions.maxWaitTimeInMs = parseInt(config.maxwaittimeinms);
            consumerOptions.allowAutoTopicCreation = config.allowautotopiccreation;

            subscribeOptions.fromBeginning = config.frombeginning;

            runOptions.autoCommitInterval = parseInt(config.autocommitinterval);
            runOptions.autoCommitThreshold = parseInt(config.autocommitthreshold);
        }

        node.init = async function init() {
            if (config.advancedoptions && config.clearoffsets) {
                node.status({ fill: 'yellow', shape: 'ring', text: 'Clearing Offset' });
                var admin = kafka.admin();
                await admin.connect();
                await admin.resetOffsets({ groupId: config.groupid, topic: config.topic });
                await admin.disconnect();
            }

            node.consumer = kafka.consumer(consumerOptions);
            node.status({ fill: 'yellow', shape: 'ring', text: 'Initializing' });

            node.onConnect = function () {
                node.lastMessageTime = new Date().getTime();
                node.status({ fill: 'green', shape: 'ring', text: 'Ready' });
            };

            node.onDisconnect = function () {
                node.status({ fill: 'red', shape: 'ring', text: 'Offline' });
            };

            node.onRequestTimeout = function () {
                node.status({ fill: 'red', shape: 'ring', text: 'Timeout' });
            };

            node.onError = function (e) {
                node.error('Kafka Consumer Error', e.message);
                node.status({ fill: 'red', shape: 'ring', text: 'Error' });
            };

            node.onMessage = function (topic, partition, message) {
                node.lastMessageTime = new Date().getTime();
                var payload = new Object();
                payload.topic = topic;
                payload.partition = partition;

                payload.payload = new Object();
                payload.payload = message;

                payload.payload.key = message.key ? message.key.toString() : null;
                payload.payload.value = message.value.toString();

                for (const [key, value] of Object.entries(payload.payload.headers)) {
                    payload.payload.headers[key] = value.toString();
                }

                node.send(payload);
                node.status({ fill: 'blue', shape: 'ring', text: 'Reading' });
            };

            function checkLastMessageTime() {
                if (node.lastMessageTime != null) {
                    timeDiff = new Date().getTime() - node.lastMessageTime;
                    if (timeDiff > 5000) {
                        node.status({ fill: 'yellow', shape: 'ring', text: 'Idle' });
                    }
                }
            }

            node.interval = setInterval(checkLastMessageTime, 1000);

            node.consumer.on(node.consumer.events.CONNECT, node.onConnect);
            node.consumer.on(node.consumer.events.DISCONNECT, node.onDisconnect);
            node.consumer.on(node.consumer.events.REQUEST_TIMEOUT, node.onRequestTimeout);

            await node.consumer.connect();
            await node.consumer.subscribe(subscribeOptions);

            runOptions.eachMessage = async ({ topic, partition, message }) => {
                node.onMessage(topic, partition, message);
            };

            await node.consumer.run(runOptions);
        };

        node.init().catch((e) => {
            node.onError(e);
        });

        node.on('close', function (done) {
            node.consumer
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
    RED.nodes.registerType('kj-kafka-consumer', KafkajsConsumerNode);
};
