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
        if (parts[0] == "stat" && node.requestlatest) {
          const commandTopic = "cmnd." + parts.slice(1).join(".");
          const resultTopic = "stat." + parts[1] + ".RESULT";
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

              const topic_end = topic.substring(resultTopic.length + 1);
              if (topic_end == "Command") {
                finish(undefined);
              } else if (topic_end == parts[2]?.toUpperCase() ||
                  (topic_end.length == parts[2]?.length + 1 &&
                   topic_end[topic_end.length - 1] == '1')
              ) {
                // cmnd/X/POWER can result in stat/X/POWER1 (...and stat/X/POWER2)
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
