const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttInNode(config) {
    RED.nodes.createNode(this, config);
    this.topic = config.topic;
    this.name = config.name;
    this.ack = config.ack;

    const node = this;
    if (this.topic) {
      this.db = MqttDb.instance(RED);
      this.cb = this.db.subscribe(this.topic, (topic, val, ack) => {
        if (node.ack == "all" || (node.ack === "updates") == ack) {
          node.send({ topic, payload: MqttDb.process_resp(val), ack, ts: Date.now() });
        }
      });
    }

    this.on("close", function () {
      if (node.cb) {
        node.db.unsubscribe(node.topic, node.cb);
      }
    });
  }

  RED.nodes.registerType("mqtt-db-in", MqttInNode);
};
