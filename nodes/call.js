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
        if (node.requestlatest) {
          const requestedTopic = parts.join(".");
          const rootTopic = parts[0];
          const relativeTopic = requestedTopic.substring(rootTopic.length + 1)
          const subscriptions = [];
          let timer = null;

          value = await new Promise((resolve) => {
            const finish = (value) => {
              clearTimeout(timer);
              resolve(value);
            };

            subscriptions.push([
              requestedTopic,
              node.db.subscribe(requestedTopic, (topic, val, acked) => {
                if (acked && topic === requestedTopic) {
                  finish(
                    relativeTopic.toUpperCase() === "COMMAND" ? undefined : val,
                  );
                }
              }),
            ]);
            // Requesting i.e. Power sometimes results in Power1 topic
            if (relativeTopic) {
              const numberedTopic = requestedTopic + '1';
              subscriptions.push([
                numberedTopic,
                node.db.subscribe(numberedTopic, (topic, val, acked) => {
                  if (acked && topic === numberedTopic) {
                    finish(val);
                  }
                }),
              ]);
            }
            // Invalid requests result in `<device>.Command` topic which
            // lets us finish immediately without waiting the 5s timeout
            if (
              relativeTopic && relativeTopic.toUpperCase() !== "COMMAND"
            ) {
              const unknownTopic = `${rootTopic}.Command`;
              subscriptions.push([
                unknownTopic,
                node.db.subscribe(unknownTopic, (topic, val, acked) => {
                  if (acked && topic === unknownTopic) {
                    finish(undefined);
                  }
                }),
              ]);
            }

            timer = setTimeout(() => finish(undefined), 5000);
            node.db.query(requestedTopic);
          });
          for (const [responseTopic, cb] of subscriptions) {
            node.db.unsubscribe(responseTopic, cb);
          }
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

  RED.nodes.registerType("mqtt-db-call", MqttCallNode);
};
