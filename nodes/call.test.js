const assert = require("node:assert/strict");
const test = require("node:test");

const MqttDb = require("./db");
const registerSubscriber = require("./subscriber");
const registerCall = require("./call");
const registerIn = require("./in");

const setup = (callConfig, inputConfig) => {
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
  registerIn(RED);
  const subscriber = new types["mqtt-db-subscriber"]({});
  const call = new types["mqtt-db-call"](callConfig);
  const input = inputConfig
    ? new types["mqtt-db-in"](inputConfig)
    : undefined;
  return { db: MqttDb.inst, subscriber, call, input };
};

test("an injected cached call does not trigger mqtt-db-in", async () => {
  const topic = "sonoff-zb-bridge.SENSOR.t2.Temperature";
  const { db, subscriber, call, input } = setup({
    topic,
    requestlatest: "false",
    attr: "payload",
  }, {
    topic,
    ack: "updates",
  });
  subscriber.handlers.input({
    topic: "stat/sonoff-zb-bridge/SENSOR",
    payload: { t2: { Temperature: 25.58 } },
  });
  input.sent = [];

  await call.handlers.input({ _msgid: "inject", payload: "" });

  assert.equal(db.get(topic), 25.58);
  assert.deepEqual(subscriber.sent, []);
  assert.deepEqual(input.sent, []);
  assert.deepEqual(call.sent, [{
    _msgid: "inject",
    topic,
    payload: 25.58,
  }]);
});

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

  assert.equal(db.get("plug.RESULT"), undefined);
  assert.equal(db.get("plug.POWER"), "ON");
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
    payload: { TIMER1: 30 },
  });
  assert.deepEqual(call.sent, []);
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

test("request latest matches responses case-insensitively", async () => {
  const { subscriber, call } = setup({
    topic: "plug.Dimmer",
    requestlatest: true,
    attr: "payload",
  });

  const pending = call.handlers.input({});
  subscriber.handlers.input({
    topic: "stat/plug/RESULT",
    payload: { Dimmer: 42 },
  });
  await pending;

  assert.deepEqual(call.sent, [{
    topic: "plug.Dimmer",
    payload: 42,
  }]);
});
