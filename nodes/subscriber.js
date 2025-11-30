const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttDbSubscriber(config) {
    RED.nodes.createNode(this, config);
    this.db = MqttDb.instance(RED);
    const node = this;

    const topics_to_ignore = new Set();
    this.db.sub_all = (topic, val, acked) => {
      if (!acked) {
        if (topic.startsWith("stat.")) {
          topic = topic.replace("stat.", "cmnd.");
        }
        node.send({ topic: topic.replaceAll('.', '/'), payload: val });
        topics_to_ignore.add(topic);
      }
    };

    this.on("input", (msg) => {
      if (!topics_to_ignore.delete(msg.topic)) {
        node.db.update(msg.topic, msg.payload, true, true);
      }
    });

    this.on("close", () => {
      node.db.sub_all = null;
      node.db.dump();
    });
  }
  RED.nodes.registerType("mqtt-db-subscriber", MqttDbSubscriber);
};
