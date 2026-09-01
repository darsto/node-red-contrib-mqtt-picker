const assert = require("node:assert/strict");
const test = require("node:test");

const MqttDb = require("./db");
const registerSubscriber = require("./subscriber");

const setup = () => {
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
  const node = new types["mqtt-db-subscriber"]({});
  return { db: MqttDb.inst, node };
};

test("normalizes prefixed and prefixless MQTT input", () => {
  const { db, node } = setup();

  node.handlers.input({ topic: "stat/plug/POWER", payload: "ON" });
  node.handlers.input({ topic: "tele/plug/STATE", payload: { Uptime: 10 } });
  node.handlers.input({ topic: "cmnd/plug/Dimmer", payload: 50 });
  node.handlers.input({ topic: "sensor/Temperature", payload: 21.5 });

  assert.deepEqual(db.data, {
    plug: {
      POWER: "ON",
      STATE: { Uptime: 10 },
      Dimmer: 50,
      _update_ts: db.data.plug._update_ts,
    },
    sensor: {
      Temperature: 21.5,
      _update_ts: db.data.sensor._update_ts,
    },
  });
  assert.deepEqual(node.sent, []);
});

test("collapses received RESULT fields without changing existing data", () => {
  const { db, node } = setup();

  db.update("plug.RESULT.Legacy", "kept");
  node.handlers.input({
    topic: "stat/plug/RESULT",
    payload: { POWER: "ON", Switch1: "OFF" },
  });
  node.handlers.input({
    topic: "stat/plug/RESULT/Dimmer",
    payload: 25,
  });

  assert.deepEqual(db.data.plug, {
    RESULT: { Legacy: "kept" },
    POWER: "ON",
    Switch1: "OFF",
    Dimmer: 25,
    _update_ts: db.data.plug._update_ts,
  });
});

test("drops discovery input and outbound queries", () => {
  const { db, node } = setup();
  const updates = [];
  db.subs.cb.push((...args) => updates.push(args));

  node.handlers.input({
    topic: "tasmota/discovery/device/config",
    payload: { ignored: true },
  });
  node.handlers.input({
    topic: "tasmota.discovery.device.sensors",
    payload: { ignored: true },
  });
  db.query("tasmota.discovery.device.command");

  assert.deepEqual(db.data, {});
  assert.deepEqual(node.sent, []);
  assert.deepEqual(updates, []);
});

test("collapses INFO and STATUS topic and payload wrappers", () => {
  const { db, node } = setup();

  node.handlers.input({
    topic: "tele/plug/INFO1",
    payload: { Info1: { Version: "13.0.0(tasmota)", Module: "Generic" } },
  });
  node.handlers.input({
    topic: "tele/plug/INFO2/Info2/FriendlyName",
    payload: "Desk plug",
  });
  node.handlers.input({
    topic: "tele/plug/INFO3/Info2",
    payload: "not collapsed",
  });
  node.handlers.input({
    topic: "tele/plug/STATUS",
    payload: { Status: { Module: 1, DeviceName: "Desk plug" } },
  });

  assert.deepEqual(db.data, {
    plug: {
      INFO1: { Version: "13.0.0(tasmota)", Module: "Generic" },
      INFO2: { FriendlyName: "Desk plug" },
      INFO3: { Info2: "not collapsed" },
      STATUS: { Module: 1, DeviceName: "Desk plug" },
      _update_ts: db.data.plug._update_ts,
    },
  });
});

test("routes descendants according to the current INFO1 Version", () => {
  const { db, node } = setup();

  db.update("plug.INFO1.Version", "13.0.0(TaSmOtA)");
  db.query("plug.POWER");
  db.update("sensor.INFO1.Version", "custom firmware");
  db.query("sensor.POWER");
  db.query("unknown.POWER");
  db.query("single");

  assert.deepEqual(node.sent, [
    { topic: "cmnd/plug/POWER", payload: "" },
    { topic: "sensor/POWER", payload: "" },
    { topic: "unknown/POWER", payload: "" },
    { topic: "single", payload: "" },
  ]);
});

test("records command echoes as acknowledged updates without a loop", () => {
  const { db, node } = setup();
  const updates = [];
  db.subscribe("plug.POWER", (topic, value, acked) => {
    updates.push({ topic, value, acked });
  });

  node.handlers.input({ topic: "cmnd/plug/POWER", payload: "ON" });

  assert.equal(db.get("plug.POWER"), "ON");
  assert.deepEqual(updates, [
    { topic: "plug.POWER", value: "ON", acked: true },
  ]);
  assert.deepEqual(node.sent, []);
});
