// var ponte = require('ponte');
var ascoltatori_mqtt = require('ascoltatori');
var ascoltatori_kafka = require('ascoltatori');
var arg_mqtt = null;
var arg_kafka = null;
const conf_mqtt = {
    type: 'mqtt',
    json: false,
    mqtt: require('mqtt'),
    url: 'http://localhost:1883'
};

const conf_kafka = {
    type: 'kafka',
    json: false,
    kafka: require('kafka-node'),
    connectString: "localhost:2181",
    clientId: "ascoltatorigroup",
    groupId: "test-consumer-group",
    defaultEncoding: "utf8",
    encodings: {
        image: "utf8"
    }
};
ascoltatori_mqtt.build(conf_mqtt, function(err, broker) {
    broker.subscribe("test", function() {
        console.log("mqtt_subscribe...\n" + "value: " + arguments[1] + ".....\n");
        arg_mqtt = arguments[1];
    });
    broker.publish("messages", "on", function() {
        console.log("mqtt_publish...\n" + "value: " + arg_kafka + "\n");
        arg_kafka = null;
    });
});
ascoltatori_kafka.build(conf_kafka, function(err, broker) {
    broker.subscribe("messages", function() {
        console.log("kafka_subscribe...\n" + "value: " + arguments[1] + "\n");
        arg_kafka = arguments[1];
    });
    broker.publish("test", arg_mqtt, function() {
        console.log("kafka_publish...\n" + "value: " + arg_mqtt + ".....\n");
        arg_mqtt = null;
    });
});
