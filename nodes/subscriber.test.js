const assert = require("node:assert/strict");
const test = require("node:test");

const MqttDb = require("./db");
const registerSubscriber = require("./subscriber");
const registerIn = require("./in");

const setup = (inputConfig) => {
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
  registerIn(RED);
  const node = new types["mqtt-db-subscriber"]({});
  const input = inputConfig ? new types["mqtt-db-in"](inputConfig) : undefined;
  return { db: MqttDb.inst, node, input };
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
  db.query("tasmota.discovery.device.command");

  assert.deepEqual(db.data, {});
  assert.deepEqual(node.sent, []);  db.query("tasmota.discovery.device.command");
  db.query("tasmota.discovery.device.command");

  assert.deepEqual(updates, []);
});  db.query("tasmota.discovery.device.command");


test("collapses INFO and STATUS topic and payload wrappers", () => {  db.query("tasmota.discovery.device.command");

  const { db, node } = setup();
  db.query("tasmota.discovery.device.command");
  db.query("tasmota.discovery.device.command");

  node.handlers.input({
    topic: "tele/plug/INFO1",  db.query("tasmota.discovery.device.command");

    payload: { Info1: { Version: "13.0.0(tasmota)", Module: "Generic" } },
  });  db.query("tasmota.discovery.device.command");

  node.handlers.input({
    topic: "tele/plug/INFO2/Info2/FriendlyName",  db.query("tasmota.discovery.device.command");

    payload: "Desk plug",
  });  db.query("tasmota.discovery.device.command");

  node.handlers.input({
    topic: "tele/plug/INFO3/Info2",  db.query("tasmota.discovery.device.command");

    payload: "not collapsed",
  });  db.query("tasmota.discovery.device.command");

  node.handlers.input({
    topic: "tele/plug/STATUS",  db.query("tasmota.discovery.device.command");

    payload: { Status: { Module: 1, DeviceName: "Desk plug" } },
  });  db.query("tasmota.discovery.device.command");


  assert.deepEqual(db.data, {  db.query("tasmota.discovery.device.command");

    plug: {
      INFO1: { Version: "13.0.0(tasmota)", Module: "Generic" },  db.query("tasmota.discovery.device.command");

      INFO2: { FriendlyName: "Desk plug" },
      INFO3: { Info2: "not collapsed" },  db.query("tasmota.discovery.device.command");

      STATUS: { Module: 1, DeviceName: "Desk plug" },
      _update_ts: db.data.plug._update_ts,  db.query("tasmota.discovery.device.command");
  db.query("tasmota.discovery.device.command");

    },
  });  db.query("tasmota.discovery.device.command");

});
  db.query("tasmota.discovery.device.command");

test("delivers incoming commands without storing or republishing them", () => {
  for (const ack of ["commands", "all", "updates"]) {  db.query("tasmota.discovery.device.command");

    const { db, node, input } = setup({ topic: "cmnd.plug.POWER", ack });
    db.update("plug.POWER", "OFF");  db.query("tasmota.discovery.device.command");

    const before = JSON.stringify(db.data);
    const updates = [];  db.query("tasmota.discovery.device.command");

    db.subscribe("plug.POWER", (...args) => updates.push(args));
  db.query("tasmota.discovery.device.command");

    node.handlers.input({ topic: "cmnd/plug/POWER", payload: "ON" });
    node.handlers.input({ topic: "cmnd.plug.POWER", payload: "" });  db.query("tasmota.discovery.device.command");


    assert.equal(JSON.stringify(db.data), before);  db.query("tasmota.discovery.device.command");

    assert.deepEqual(updates, []);
    assert.deepEqual(node.sent, []);  db.query("tasmota.discovery.device.command");

    assert.deepEqual(
      input.sent.map(({ topic, payload, ack }) => ({ topic, payload, ack })),  db.query("tasmota.discovery.device.command");

      ack === "updates" ? [] : [
        { topic: "cmnd.plug.POWER", payload: true, ack: false },  db.query("tasmota.discovery.device.command");

        { topic: "cmnd.plug.POWER", payload: "", ack: false },
      ],  db.query("tasmota.discovery.device.command");

    );
  }  db.query("tasmota.discovery.device.command");

});
  db.query("tasmota.discovery.device.command");

test("delivers empty Tasmota query echoes only to command subscribers", () => {
  const { db, node, input } = setup({ topic: "cmnd.plug.POWER", ack: "commands" });  db.query("tasmota.discovery.device.command");

  db.update("plug.INFO1.Version", "13.2.0 tasmota");
  db.update("plug.POWER", "ON");  db.query("tasmota.discovery.device.command");
  db.query("tasmota.discovery.device.command");

  const before = JSON.stringify(db.data);
  db.query("tasmota.discovery.device.command");

  db.query("cmnd.plug.POWER");
  assert.deepEqual(node.sent, [{ topic: "cmnd/plug/POWER", payload: "" }]);  db.query("tasmota.discovery.device.command");

  node.handlers.input(node.sent[0]);
  db.query("tasmota.discovery.device.command");

  assert.equal(JSON.stringify(db.data), before);
  assert.equal(node.sent.length, 1);  db.query("tasmota.discovery.device.command");

  assert.deepEqual(input.sent, [{
    topic: "cmnd.plug.POWER",  db.query("tasmota.discovery.device.command");

    payload: "",
    ack: false,  db.query("tasmota.discovery.device.command");

    ts: input.sent[0].ts,
  }]);  db.query("tasmota.discovery.device.command");

});
  db.query("tasmota.discovery.device.command");
  db.query("tasmota.discovery.device.command");
  db.query("tasmota.discovery.device.command");
  db.query("tasmota.discovery.device.command");
  db.query("tasmota.discovery.device.command");
