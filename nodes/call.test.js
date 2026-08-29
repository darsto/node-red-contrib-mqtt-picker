const assert = require("node:assert/strict");
const test = require("node:test");

const MqttDb = require("./db");
const registerSubscriber = require("./subscriber");
const registerCall = require("./call");

const setup = (callConfig) => {
  const types = {};
  const RED = {
    nodes: {
      createNode(node) {
        node.handlers = {};
        node.on = (event, cb) => {
          node.handlers[event] = cb;
        };
        node.sent = [];
        node.send = (msg) => node.sent.push(msg);
      },
      registerType(name, ctor) {
        types[name] = ctor;
      },
    },
  };
  MqttDb.inst = new MqttDb();
  registerSubscriber(RED);
  registerCall(RED);
  const subscriber = new types["mqtt-db-subscriber"]({});
  const call = new types["mqtt-db-call"](callConfig);
  return { db: MqttDb.inst, subscriber, call };
};

test("request latest publishes the prefixless command and matches X.RESULT", async () => {
  const { db, subscriber, call } = setup({
    topic: "plug.POWER",
    requestlatest: true,
    attr: "payload",
  });
  db.update("plug.INFO1.Version", "13.2.0 tasmota");

  const pending = call.handlers.input({ source: "test" });
  assert.deepEqual(subscriber.sent, [
    { topic: "cmnd/plug/POWER", payload: "" },
  ]);

  subscriber.handlers.input({
    topic: "stat/plug/RESULT",
    payload: { POWER: "ON" },
  });
  await pending;

  assert.deepEqual(call.sent, [{
    source: "test",
    topic: "plug.POWER",
    payload: true,
  }]);
});

test("request latest ignores unrelated results and preserves POWER1 matching", async () => {
  const { subscriber, call } = setup({
    topic: "plug.POWER",
    requestlatest: true,
    attr: "result",
  });

  const pending = call.handlers.input({});
  assert.deepEqual(subscriber.sent, [{ topic: "plug/POWER", payload: "" }]);
  subscriber.handlers.input({
    topic: "stat/plug/RESULT",
    payload: { Dimmer: 25 },
  });
  subscriber.handlers.input({
    topic: "stat/plug/RESULT",
    payload: { POWER1: "OFF" },
  });
  await pending;

  assert.deepEqual(call.sent, [{
    topic: "plug.POWER",
    result: false,
  }]);
});
