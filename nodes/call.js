module.exports = function (RED) {
  function StringPickerNode(config) {
    RED.nodes.createNode(this, config);
    this.topic = config.topic;
    this.payload = config.payload;
    this.name = config.name;
    this.attr = config.attr;
    this.noexist = config.noexist;

    const node = this;
    this.on("input", (msg) => {
      const topic = msg.topic || node.topic;
      if (topic) {
        const value = node.db.get(topic);
        if (!value) {
          if (node.noexist == "error") {
            throw new Error("MQTT Topic '" + topic + "' doesn't exist");
          } else if (node.noexist == "nothing") {
            return;
          }
        }
        msg.topic = topic;
        msg[this.attr || "payload"] = value;
        this.send(msg);
      }
    });
  }

  RED.nodes.registerType("mqtt-db-call", StringPickerNode);
};
