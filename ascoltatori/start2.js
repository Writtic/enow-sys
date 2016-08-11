// var ponte = require('ponte');
var ascoltatori_mqtt = require('ascoltatori');
var ascoltatori_kafka = require('ascoltatori');
var sleep = require('sleep');
var mqtt = require('mqtt');
var arg_mqtt = null;
var arrrg_kafka = null;
var signal = 0;
var signal2=0;
var settings = {
  type: 'mqtt',
  json: false,
  mqtt: require('mqtt'),
  url: 'http://localhost:1883'
};

var settings_kafka = {
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

ascoltatori_mqtt.build(settings, function (err_mqtt, ascoltatori_mqtt) {

      ascoltatori_mqtt.subscribe("test", function() {
        if(arguments[1] == "on"){
          console.log("mqtt_subscribe...\n" + "value: " + arguments[1] + ".....\n");
          arg_mqtt = arguments[1];
          signal = 1;
        }
      });
      ascoltatori_mqtt.publish("messages", "on", function(){
        console.log("mqtt_publish...\n" + "value: " + arrrg_kafka + "\n");
        arrrg_kafka = null;
        signal, signal2 = 0;
      });
    // }
});
ascoltatori_kafka.build(settings_kafka, function (err_kafka, ascoltatore_kafka){

      ascoltatore_kafka.subscribe("messages", function() {
          // if(arguments[1] == "on"){
            console.log("kafka_subscribe...\n" + "value: " + arguments[1] + "\n");
            arrrg_kafka = arguments[1];
          // }
      });
    // }
    // if(signal == 1){
      ascoltatore_kafka.publish("test", arg_mqtt, function(){
          console.log("kafka_publish...\n" + "value: " + arg_mqtt + ".....\n");
          arg_mqtt = null;
          signal2= 1;
      });
    // }
});
