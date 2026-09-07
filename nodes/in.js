const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttInNode(config) {
    RED.nodes.createNode(this, config);
    let unsubscribe;
    try {
      this.db = MqttDb.instance(RED);
      unsubscribe = this.db.subscribe(config.topic, (topic, payload, deleted) => {
        this.send({ topic, payload, ...(deleted ? { deleted: true } : {}) });
      });
    } catch (err) {
      this.error(err);
      this.status({ fill: "red", shape: "dot", text: "invalid subscription" });
    }
    this.on("close", () => unsubscribe?.());
  }
  RED.nodes.registerType("mqtt-db-in", MqttInNode);
};
