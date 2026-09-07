const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttOutNode(config) {
    RED.nodes.createNode(this, config);
    let initError;
    try { this.db = MqttDb.instance(RED); }
    catch (err) { initError = err; this.error(err); }
    this.on("input", (msg, _send, done) => {
      try {
        if (initError) throw initError;
        this.db.publish(config.topic || msg.topic, msg.payload, config.fullTopic || undefined);
        if (done) done();
      } catch (err) {
        if (done) done(err); else this.error(err, msg);
      }
    });
  }
  RED.nodes.registerType("mqtt-db-out", MqttOutNode);
};
