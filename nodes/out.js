const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttOutNode(config) {
    RED.nodes.createNode(this, config);
    this.topic = config.topic;
    this.name = config.name;
    this.create = config.create;

    this.db = MqttDb.instance(RED);

    const node = this;
    this.on("input", (msg) => {
      const topic = node.topic || msg.topic;
      if (topic) {
        node.db.update(topic, msg.payload, node.create, false);
      }
    });
  }

  RED.nodes.registerType("mqtt-db-out", MqttOutNode);
};
