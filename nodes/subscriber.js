const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttDbSubscriber(config) {
    RED.nodes.createNode(this, config);
    let initError;
    try {
      this.db = MqttDb.instance(RED);
      this.db.init((msg) => this.send(msg),
        !config.fullTopics || config.fullTopics === "%prefix%/%topic%/" ? undefined : config.fullTopics);
    } catch (err) {
      initError = err;
      this.error(err);
      this.status({ fill: "red", shape: "dot", text: "configuration error" });
    }
    this.on("input", (msg, _send, done) => {
      try {
        if (initError) throw initError;
        if (typeof msg.topic !== "string") throw new Error("MQTT topic must be a string");
        this.db.receive(msg.topic, msg.payload);
        if (done) done();
      } catch (err) {
        if (done) done(err); else this.error(err, msg);
      }
    });
    this.on("close", (_removed, done) => {
      try { if (!initError) this.db?.deinit(); }
      catch (err) { this.error(err); }
      if (done) done();
    });
  }
  RED.nodes.registerType("mqtt-db-subscriber", MqttDbSubscriber);
};
