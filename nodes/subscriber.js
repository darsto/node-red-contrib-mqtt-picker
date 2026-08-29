const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttDbSubscriber(config) {
    RED.nodes.createNode(this, config);
    this.db = MqttDb.instance(RED);
    const node = this;

    const outgoing = (topic, val, acked) => {
      if (acked) {
        return;
      }
      topic = MqttDb.normalize_topic(topic);
      if (!topic || MqttDb.is_discovery_topic(topic)) {
        return;
      }
      const parts = node.db.split_key(topic);
      const version = parts.length > 1
        ? node.db.get(`${parts[0]}.INFO1.Version`)
        : undefined;
      const is_tasmota = typeof version === "string" &&
        version.toLowerCase().includes("tasmota");
      const mqtt_topic = (parts.length > 1 && is_tasmota ? "cmnd." : "") + topic;
      const msg = { topic: mqtt_topic.replaceAll(".", "/") };
      if (val !== undefined) {
        msg.payload = val;
      }
      node.send(msg);
    };
    this.db.subs.cb.push(outgoing);

    this.on("input", (msg) => {
      if (MqttDb.is_discovery_topic(msg.topic)) {
        return;
      }
      const topic = MqttDb.normalize_topic(msg.topic);
      const payload = MqttDb.collapse_info_payload(topic, msg.payload);
      node.db.update(topic, payload, true, true);
    });

    this.on("close", () => {
      const index = node.db.subs.cb.indexOf(outgoing);
      if (index >= 0) {
        node.db.subs.cb.splice(index, 1);
      }
      node.db.dump();
    });
  }
  RED.nodes.registerType("mqtt-db-subscriber", MqttDbSubscriber);
};
