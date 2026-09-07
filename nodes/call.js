const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttCallNode(config) {
    RED.nodes.createNode(this, config);
    let initError;
    try { this.db = MqttDb.instance(RED); }
    catch (err) { initError = err; this.error(err); }
    const controllers = new Set();
    let closed = false;
    this.on("input", async (msg, send, done) => {
      const controller = new AbortController();
      controllers.add(controller);
      try {
        if (initError) throw initError;
        if (closed) throw new Error("Node is closed");
        const topic = config.topic || msg.topic;
        const requestLatest = config.requestlatest === true || config.requestlatest === "true";
        const value = requestLatest
          ? await this.db.query(topic, {
            timeout: Number(config.timeout === "" || config.timeout == null ? 5000 : config.timeout),
            signal: controller.signal,
          })
          : this.db.get(topic);
        if (closed) return;
        if (value === undefined && config.noexist === "error") throw new Error("Device property does not exist: " + topic);
        if (value === undefined && (!config.noexist || config.noexist === "nothing")) return;
        msg.topic = topic;
        // An attribute is a literal top-level message key, never a prototype setter.
        Object.defineProperty(msg, config.attr || "payload", {
          value, enumerable: true, configurable: true, writable: true,
        });
        (send || ((message) => this.send(message)))(msg);
      } catch (err) {
        if (done) { done(err); done = null; }
        else if (!closed) this.error(err, msg);
      } finally {
        controllers.delete(controller);
        if (done) done();
      }
    });
    this.on("close", () => {
      closed = true;
      for (const controller of controllers) controller.abort();
      controllers.clear();
    });
  }
  RED.nodes.registerType("mqtt-db-call", MqttCallNode);
};
