const MqttDb = require("./db");

module.exports = function (RED) {
  function StringPickerNode(config) {
    RED.nodes.createNode(this, config);
    this.topic = config.topic;
    this.payload = config.payload;
    this.name = config.name;
    this.attr = config.attr;
    this.noexist = config.noexist;

    this.db = MqttDb.instance(RED);
    const node = this;
    this.on("input", async (msg) => {
      const topic = node.topic || msg.topic;
      if (topic) {
        // TODO query by sending cmnd/topic/var
        // and subscribe to cmnd/topic/RESULT
        // timeout after 5s
        let value;
        const parts = node.db.split_key(topic);
        if (parts[0] == "stat") {
          node.db.query("cmnd." + topic.substring(5), "");
          let cb;
          const cmnd = "stat." + parts[1] + ".RESULT";
          value = await new Promise((resolve) => {
            cb = node.db.subscribe(cmnd, (topic, val, acked) => {
              if (!acked) {
                return;
              }

              const topic_end = topic.substring(cmnd.length + 1);
              if (topic_end == "Command") {
                resolve(undefined);
              } else if (topic_end == parts[2].toUpperCase() ||
                  (topic_end.length == parts[2].length + 1 &&
                   topic_end[topic_end.length - 1] == '1')
              ) {
                // cmnd/X/POWER can result in stat/X/POWER1 (...and stat/X/POWER2)
                resolve(val);
              } else {
                // race condition; resp to a different cmnd
              }
            });
          });
          node.db.unsubscribe(cmnd, cb);
        } else {
          value = node.db.get(topic);
        }
        if (value === undefined) {
          if (node.noexist == "error") {
            throw new Error("MQTT Topic '" + topic + "' doesn't exist");
          } else if (node.noexist == "nothing") {
            return;
          }
        }
        msg.topic = topic;
        msg[this.attr || "payload"] = MqttDb.process_resp(value);
        this.send(msg);
      }
    });
  }

  RED.nodes.registerType("mqtt-db-call", StringPickerNode);
};
