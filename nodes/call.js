const MqttDb = require("./db");

module.exports = function (RED) {
  function MqttCallNode(config) {
    RED.nodes.createNode(this, config);
    this.topic = config.topic;
    this.payload = config.payload;
    this.name = config.name;
    this.attr = config.attr;
    this.noexist = config.noexist;
    this.requestlatest = config.requestlatest === true || config.requestlatest === "true";

    this.db = MqttDb.instance(RED);
    const node = this;
    this.on("input", async (msg) => {
      const topic = node.topic || msg.topic;
      if (topic) {
        let value;
        const parts = node.db.split_key(topic);
        if (parts.length > 1 && node.requestlatest) {
          const commandTopic = parts.join(".");
          const resultTopic = parts[0];
          let cb = null;
          let timer = null;

          value = await new Promise((resolve) => {
            const finish = (value) => {
              clearTimeout(timer);
              resolve(value);
            };

            cb = node.db.subscribe(resultTopic, (topic, val, acked) => {
              if (!acked) {
                return;
              }

              const topic_end = topic.substring(resultTopic.length + 1)
                .toUpperCase();
              const command = parts[1]?.toUpperCase();
              if (topic_end == "COMMAND") {
                finish(undefined);
              } else if (
                topic_end == command || topic_end == command + "1"
              ) {
                finish(val);
              } else {
                // race condition; resp to a different cmnd
              }
            });

            timer = setTimeout(() => finish(undefined), 5000);
            node.db.query(commandTopic);
          });
          node.db.unsubscribe(resultTopic, cb);
        } else {
          node.db.query(topic);
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

  RED.nodes.registerType("mqtt-db-call", MqttCallNode);
};
