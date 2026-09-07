const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { EventEmitter } = require("node:events");
const test = require("node:test");
const MqttDb = require("../nodes/db");
const MqttTopicParser = require("../resources/mqtt-topic-parser");

function setup(patterns = ["%prefix%/%topic%/"]) {
  const db = new MqttDb();
  const sent = [];
  db.init((msg) => sent.push(msg), patterns);
  return { db, sent };
}
function events(db, selector) {
  const calls = [];
  const unsubscribe = db.subscribe(
    selector,
    (topic, value, deleted) => calls.push({ topic, value, deleted }),
  );
  return { calls, unsubscribe };
}
function subscriptionCount(db) {
  let count = 0;
  const nodes = Object.values(db.tree);
  for (const node of nodes) {
    count += node.subscribers.size;
    nodes.push(...Object.values(node.children));
  }
  return count;
}
function temporary(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mqtt-picker-test-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  return path.join(directory, "mqttdb.json");
}

test("Full Topic round trips with the prefix first, middle and last", () => {
  for (
    const [pattern, incoming, key, outgoing, routePath] of [
      [
        "%prefix%/%topic%/",
        "stat/desk/HVAC/Temperature",
        "desk.HVAC.Temperature",
        "cmnd/desk/Hvac/Temperature",
        ["desk"],
      ],
      [
        "tasmota/%topic%/%prefix%/",
        "tasmota/desk/tele/HVAC/Temperature",
        "tasmota.desk.HVAC.Temperature",
        "tasmota/desk/cmnd/Hvac/Temperature",
        ["tasmota", "desk"],
      ],
      [
        "%prefix%/home/cellar/%topic%/",
        "stat/home/cellar/desk/HVAC/Temperature",
        "home.cellar.desk.HVAC.Temperature",
        "cmnd/home/cellar/desk/Hvac/Temperature",
        ["home", "cellar", "desk"],
      ],
    ]
  ) {
    const { db, sent } = setup([pattern]);
    db.receive(incoming, "ON");
    assert.equal(db.get(key), 1);
    db.publish(key, "OFF");
    assert.deepEqual(sent, [{ topic: outgoing, payload: "OFF" }]);
    assert.equal(db.get(key), 1);
    assert.equal(
      routePath.reduce((node, part) => node.children[part], {
        children: db.tree,
      }).fullTopic,
      pattern,
    );
  }
});

test("default patterns support Tasmota and generic topic trees", () => {
  const db = new MqttDb();
  db.receive("stat/desk/POWER", "ON");
  db.receive("zb/b2", { Device: "0x1DA8", Power: 2 });
  assert.deepEqual(db.data(), {
    desk: { Power: 1, _update_ts: db.get("desk._update_ts") },
    zb: { b2: { Device: "0x1DA8", Power: 2 }, _update_ts: db.get("zb._update_ts") },
  });
});

test("incoming topics and JSON keys are case-insensitive and normalize uppercase names", () => {
  const db = new MqttDb();
  db.receive("stat/dev/something", 1);
  db.receive("STAT/Dev/SOMETHING", 2);
  db.receive("tele/DEV/RESULT", { POWER1: 1, SWITCH1: 0 });
  db.receive("stat/dev/result", { power1: 4 });
  db.receive("stat/OTHER/POWER1", 3);
  assert.equal(db.get("DEV.Something"), 2);
  assert.equal(db.get("dev.POWER1"), 4);
  assert.deepEqual(Object.keys(db.data().dev).sort(), ["Power1", "Switch1", "_update_ts", "something"].sort());
  assert.equal(db.data().Other.Power1, 3);
});

test("generic root messages and suffix messages merge without Tasmota interpretation", () => {
  const { db, sent } = setup(["devices/%topic%/"]);
  db.receive("devices/fan", { state: { speed: 1 }, RESULT: { POWER: "ON" } });
  db.receive("devices/fan/state", { mode: "OFF" });
  db.publish("devices.fan.set", { speed: 2 });
  assert.deepEqual(db.get("devices.fan"), {
    state: { speed: 1, mode: "OFF" },
    Result: { Power: "ON" },
    _update_ts: db.get("devices.fan._update_ts"),
  });
  assert.deepEqual(sent, [{ topic: "devices/fan/set", payload: { speed: 2 } }]);
  db.publish("devices.fan", { speed: 3 });
  assert.equal(sent[1].topic, "devices/fan");
});

test("object routes expose, update, and persist their update timestamp", (t) => {
  const file = temporary(t);
  const db = new MqttDb();
  const originalNow = Date.now;
  try {
    Date.now = () => 1700000000000;
    db.receive("stat/desk/POWER", 1);
    assert.equal(db.get("desk._update_ts"), 1700000000000);
    Date.now = () => 1700000000123;
    db.receive("stat/desk/RESULT", { POWER: 2, _update_ts: 0 });
    assert.equal(db.get("desk._update_ts"), 1700000000123);
    db.receive("weather", 21);
    assert.equal(db.get("weather._update_ts"), undefined);
    db.dump(file);
  } finally {
    Date.now = originalNow;
  }
  const loaded = new MqttDb();
  loaded.load(file);
  assert.equal(loaded.get("desk._update_ts"), 1700000000123);
});

test("specific patterns win; ties reject independently of order", () => {
  const { db } = setup(["%prefix%/%topic%/", "%prefix%/home/cellar/%topic%/"]);
  db.receive("stat/home/cellar/pump/POWER", "on");
  assert.equal(db.get("home.cellar.pump.POWER"), 1);
  const patterns = ["%prefix%/home/%topic%/", "%prefix%/%topic%/state/"];
  for (const order of [patterns, [...patterns].reverse()]) {
    const { db: ambiguous } = setup(order);
    assert.throws(() => ambiguous.receive("stat/home/state/x", 1), /Ambiguous/);
    assert.deepEqual(ambiguous.data(), {});
  }
});

test("literal generic stat base is preferred to a variable prefix", () => {
  const { db } = setup(["%prefix%/%topic%/", "stat/%topic%/"]);
  db.receive("stat/desk/POWER", "OFF");
  assert.equal(db.get("stat.desk.POWER"), "OFF");
  assert.equal(db.tree.stat.children.desk.fullTopic, "stat/%topic%/");
});

test("the same device under different literal bases has separate routes", () => {
  const { db } = setup(["%prefix%/%topic%/", "tasmota/%topic%/%prefix%/"]);
  db.receive("stat/desk/POWER", 1);
  db.receive("tasmota/desk/stat/POWER", 0);
  assert.equal(db.get("desk.POWER"), 1);
  assert.equal(db.get("tasmota.desk.POWER"), 0);
});

test("conflicting routes report both MQTT Topics", () => {
  const { db } = setup(["%prefix%/%topic%/", "%topic%/%prefix%/"]);
  db.receive("stat/desk/POWER", 1);
  assert.throws(
    () => db.receive("desk/tele/POWER", 0),
    /Conflicting MQTT Topics for device desk: %prefix%\/%topic%\/ and %topic%\/%prefix%\//,
  );
});

test("patterns and generated MQTT names are validated", () => {
  for (
    const pattern of [
      "%topic%",
      "%topic%/%topic%/",
      "%prefix%/",
      "%topic%//",
      "x%topic%/",
      "%topic%/%bad%/",
      "%topic%/#/",
    ]
  ) {
    assert.throws(() => MqttDb.pattern(pattern), undefined, pattern);
  }
  const { db } = setup();
  assert.throws(() => db.receive("bad//topic", 1), /Invalid/);
  assert.throws(() => db.publish("bad+name.value", 1, "%topic%/"), /Invalid/);
  assert.equal(db.receive("unmatched/a/x", 1), true);
  assert.equal(db.get("unmatched.a.x"), 1);
  assert.equal(MqttDb.patterns('["%topic%/"]').length, 1);
  assert.equal(MqttDb.patterns("%topic%/\nsensors/%topic%/").length, 2);
});

test("subscription placeholders stay hidden until values arrive", () => {
  const { db } = setup();
  const sub = events(db, "future.POWER");
  assert.deepEqual(db.data(), {});
  assert.equal(JSON.stringify(db.tree), "{}");
  db.receive("stat/future/RESULT", { POWER: 1, nil: null });
  assert.equal(sub.calls[0].value, 1);
  assert.deepEqual(db.data(), { future: { Power: 1, nil: null, _update_ts: db.get("future._update_ts") } });
  db.remove("future.POWER");
  db.receive("stat/future/POWER", 2);
  assert.equal(sub.calls[1].value, 2);
  sub.unsubscribe();
  assert.equal(subscriptionCount(db), 0);
});

test("topics outside configured patterns become pickable generic data", () => {
  const { db } = setup("tasmota/%topic%/%prefix%/");
  assert.equal(db.receive("stat/zb/X", 1), true);
  assert.equal(db.receive("weather", 21), true);
  assert.deepEqual(db.data(), { stat: { zb: { X: 1 }, _update_ts: db.get("stat._update_ts") }, weather: 21 });
  assert.equal(db.remove("stat.zb.X"), true);
  assert.equal(db.remove("stat"), true);
  assert.equal(db.remove("weather"), true);
  assert.deepEqual(db.data(), {});

  assert.throws(
    () => db.receive("broken/topic", Buffer.from("bad")),
    /decoded JSON/,
  );
  assert.deepEqual(db.data(), {});
});

test("ignore command echoes", () => {
  const { db, sent } = setup(["%prefix%/%topic%/", "%topic%/"]);
  db.receive("stat/desk/POWER", 1);
  const sub = events(db, "desk");
  assert.equal(db.receive("cmnd/desk/POWER", "OFF"), false);
  assert.equal(db.get("desk.POWER"), 1);
  assert.equal(sub.calls.length, 0);
  assert.equal(sent.length, 0);
});

test("prefix-bearing values convert recursively while source values remain unchanged", () => {
  const input = {
    ON: "OfF",
    nested: {
      a: "FALSE",
      b: "oN",
      c: "True",
      online: "Online",
      spaces: " on ",
    },
    array: ["off", true, false, null, 2],
  };
  const { db } = setup(["%prefix%/%topic%/", "sensors/%topic%/"]);
  db.receive("stat/desk/RESULT", input);
  db.receive("sensors/generic", input);
  assert.deepEqual(db.get("desk"), {
    On: 0,
    nested: { a: 0, b: 1, c: 1, online: "Online", spaces: " on " },
    array: [0, 1, 0, null, 2],
    _update_ts: db.get("desk._update_ts"),
  });
  assert.deepEqual(db.get("sensors.generic"), {
    On: "OfF",
    nested: input.nested,
    array: input.array,
    _update_ts: db.get("sensors.generic._update_ts"),
  });
  assert.equal(input.ON, "OfF");
});

test("Tasmota RESULT/SENSOR and sole INFO/STATUS wrappers, without flattening siblings", () => {
  const { db } = setup();
  db.receive("stat/desk/RESULT", { POWER: "on" });
  db.receive("stat/desk/RESULT/HVAC/Temperature", 21);
  db.receive("tele/desk/SENSOR", { Humidity: 46 });
  db.receive("tele/desk/INFO1", { Info1: { Version: "example" } });
  db.receive("tele/desk/INFO2", { Info2: { value: 1 }, value: 2 });
  db.receive("stat/desk/STATUS", { Status: { Module: 1 } });
  db.receive("tele/desk/STATE", { POWER: "off" });
  assert.deepEqual(db.get("desk"), {
    Power: 1,
    Hvac: { Temperature: 21 },
    Humidity: 46,
    Info1: { Version: "example" },
    Info2: { Info2: { value: 1 }, value: 2 },
    Status: { Module: 1 },
    State: { Power: 0 },
    _update_ts: db.get("desk._update_ts"),
  });
  assert.throws(() => db.receive("stat/desk/RESULT", "invalid"), /JSON object/);
  assert.equal(db.get("desk.POWER"), 1);
});

test("MQTT levels and literal JSON dots/backslashes/hash round trip", () => {
  const { db } = setup(["devices/%topic%/"]);
  db.receive("devices/a.b/state/temperature", 20);
  assert.equal(db.get("devices.a\\.b.state.temperature"), 20);
  db.receive("devices/a.b/state", {
    temperature: 21,
    "sensor.v1": { "\\#": "yes" },
    "": 0,
  });
  assert.equal(db.get("devices.a\\.b.state.temperature"), 21);
  const selector = MqttTopicParser.format([
    "devices",
    "a.b",
    "state",
    "sensor.v1",
    "\\#",
  ]);
  assert.equal(db.get(selector), "yes");
  assert.equal(db.get("devices.a\\.b.state."), 0);
  db.publish("devices.a\\.b.control.set", { x: 1 });
});

test("reject non-JSON values atomically", () => {
  const { db } = setup();
  db.receive("stat/desk/POWER", 1);
  const cyclic = {};
  cyclic.self = cyclic;
  for (
    const value of [
      undefined,
      NaN,
      Infinity,
      Buffer.from("ON"),
      new Date(),
      new Map(),
      cyclic,
      { a: undefined },
      [undefined],
    ]
  ) {
    assert.throws(() => db.receive("stat/desk/RESULT", value));
    assert.deepEqual(db.get("desk"), { Power: 1, _update_ts: db.get("desk._update_ts") });
  }
});

test("plain objects from Node-RED Function-style VM contexts are valid JSON", () => {
  const vm = require("node:vm");
  const { db } = setup();
  const payload = vm.runInNewContext(
    '({POWER:"ON", nested:{flag:false}, list:["OFF"]})',
  );
  db.receive("stat/desk/RESULT", payload);
  assert.deepEqual(db.get("desk"), {
    Power: 1,
    nested: { flag: 0 },
    list: [0],
    _update_ts: db.get("desk._update_ts"),
  });
});

test("deep payload rejection cannot leave an unreadable cache", () => {
  const { db } = setup();
  db.receive("stat/desk/POWER", 1);
  let payload = 1;
  for (let i = 0; i < 101; i++) payload = { child: payload };
  assert.throws(() => db.receive("tele/desk/SENSOR", payload), /deeply nested/);
  const updateTs = db.get("desk._update_ts");
  assert.deepEqual(db.get("desk"), { Power: 1, _update_ts: updateTs });
  assert.deepEqual(db.data(), { desk: { Power: 1, _update_ts: updateTs } });
});

test("prototype-sensitive device names and JSON keys are ordinary own data", () => {
  const { db } = setup();
  const payload = JSON.parse(
    '{"__proto__":{"polluted":"on"},"constructor":{"prototype":{"polluted":"off"}},"toString":7}',
  );
  db.receive("stat/__proto__/RESULT", payload);
  assert.equal(db.get("__proto__.__proto__.polluted"), 1);
  assert.equal(db.get("__proto__.constructor.prototype.polluted"), 0);
  assert.equal(db.get("__proto__.toString"), 7);
  assert.equal({}.polluted, undefined);
  assert.equal(db.get("__proto__.hasOwnProperty"), undefined);
  const sub = events(db, "__proto__.#");
  db.receive("stat/__proto__/RESULT", { toString: 8 });
  assert.deepEqual(sub.calls.map((event) => event.topic), ["__proto__.toString", "__proto__._update_ts"]);
  db.remove("__proto__.__proto__");
  assert.equal({}.polluted, undefined);
});

test("whole device and exact object subscribers receive one completed snapshot", () => {
  const { db } = setup();
  db.receive("stat/desk/RESULT", { POWER: "OFF" });
  const root = events(db, "desk");
  const temperature = events(db, "desk.Temperature");
  const all = events(db, "desk.#");
  const power = events(db, "desk.POWER");
  db.receive("tele/desk/SENSOR", { Temperature: 22, Humidity: 46 });
  const updateTs = db.get("desk._update_ts");
  assert.deepEqual(root.calls, [{
    topic: "desk",
    value: { Power: 0, Temperature: 22, Humidity: 46, _update_ts: updateTs },
    deleted: false,
  }]);
  assert.equal(temperature.calls.length, 1);
  assert.deepEqual(all.calls.map((e) => e.topic), [
    "desk.Temperature",
    "desk.Humidity",
    "desk._update_ts",
  ]);
  assert.equal(power.calls.length, 0);
  assert.equal(db.get("desk.SENSOR"), undefined);
});

test("object merging preserves omitted fields; repeated updates still emit; arrays are atomic", () => {
  const { db } = setup();
  db.receive("stat/desk/RESULT", { obj: { a: 1, b: 2 }, list: [1, 2] });
  const all = events(db, "desk.#");
  db.receive("stat/desk/RESULT", { obj: { a: 1 }, list: [3] });
  assert.deepEqual(db.get("desk.obj"), { a: 1, b: 2 });
  assert.deepEqual(all.calls.map((e) => e.topic), ["desk.obj.a", "desk.list", "desk._update_ts"]);
  assert.deepEqual(db.get("desk.list"), [3]);
  assert.equal(db.get("desk.list.0"), undefined);
  const count = all.calls.length;
  db.receive("stat/desk/RESULT", { obj: {} });
  assert.equal(all.calls.length, count + 1);
  db.receive("stat/desk/RESULT", { empty: {} });
  assert.deepEqual(all.calls.find((event) => event.topic === "desk.empty"), {
    topic: "desk.empty",
    value: {},
    deleted: false,
  });
});

test("manual property/object/device removal is silent for every subscription", () => {
  const { db } = setup();
  db.receive("stat/desk/RESULT", { obj: { a: 1, b: 2 }, POWER: 1 });
  const watched = ["desk", "desk.#", "desk.obj", "desk.obj.#", "desk.obj.a"]
    .map((s) => events(db, s));
  db.remove("desk.obj.a");
  db.remove("desk.obj");
  assert.deepEqual(db.get("desk"), { Power: 1, _update_ts: db.get("desk._update_ts") });
  db.remove("desk");
  assert.equal(db.get("desk"), undefined);
  assert.ok(watched.every((s) => s.calls.length === 0));
});

test("incoming object replacement reconciles descendants but emits one final parent value", () => {
  const { db } = setup();
  db.receive("stat/desk/RESULT", { obj: { a: 1, b: 2 } });
  const all = events(db, "desk.#");
  const parent = events(db, "desk.obj");
  const child = events(db, "desk.obj.a");
  db.receive("stat/desk/RESULT", { obj: null });
  assert.deepEqual(all.calls, [
    { topic: "desk.obj.a", value: null, deleted: true },
    { topic: "desk.obj.b", value: null, deleted: true },
    { topic: "desk.obj", value: null, deleted: false },
    { topic: "desk._update_ts", value: db.get("desk._update_ts"), deleted: false },
  ]);
  assert.deepEqual(parent.calls, [{
    topic: "desk.obj",
    value: null,
    deleted: false,
  }]);
  assert.equal(child.calls[0].deleted, true);
});

test("callbacks are isolated, detached snapshots; unsubscribe removes listener", () => {
  const { db } = setup();
  const errors = [];
  db.on_error = (err) => errors.push(err);
  db.subscribe("desk", (_t, value) => {
    value.Power = 55;
    throw new Error("listener");
  });
  const second = events(db, "desk");
  db.receive("stat/desk/POWER", "ON");
  assert.equal(errors.length, 1);
  assert.equal(second.calls[0].value.Power, 1);
  const view = db.data();
  view.desk.Power = 66;
  const value = db.get("desk");
  value.Power = 77;
  assert.equal(db.get("desk.POWER"), 1);
  second.unsubscribe();
  db.receive("stat/desk/POWER", "OFF");
  assert.equal(second.calls.length, 1);
});

test("outgoing JSON is one publication and never creates reported state or notifies", () => {
  const { db, sent } = setup(["devices/%topic%/"]);
  db.receive("devices/fan/state", { speed: 1 });
  const root = events(db, "devices.fan");
  db.publish("devices.fan.set", { nested: { speed: 2 }, list: [1, 2] });
  assert.equal(sent.length, 1);
  assert.equal(sent[0].topic, "devices/fan/set");
  assert.deepEqual(sent[0].payload, { nested: { speed: 2 }, list: [1, 2] });
  assert.deepEqual(db.get("devices.fan"), { state: { speed: 1 }, _update_ts: db.get("devices.fan._update_ts") });
  assert.equal(root.calls.length, 0);
  db.receive("devices/fan/state", { mode: "auto" });
  db.publish("devices.fan.state", 3);
  assert.deepEqual(sent[1], { topic: "devices/fan/state", payload: 3 });
  assert.throws(() => db.publish("fan.#", 3), /subscriptions/);
});

test("unrouted paths publish directly and explicit patterns are one-shot", () => {
  const { db, sent } = setup();
  db.publish("new.POWER", 1);
  assert.deepEqual(sent[0], { topic: "new/Power", payload: 1 });
  assert.equal(db.get("new"), undefined);
  db.publish("new.POWER", 1, "tasmota/%topic%/%prefix%/");
  assert.equal(sent[1].topic, "tasmota/new/cmnd/Power");
  assert.equal(db.get("new"), undefined);
  db.publish("new.POWER", 0, "%prefix%/%topic%/");
  assert.equal(sent[2].topic, "cmnd/new/Power");
  db.publish("new.POWER.level", 0, "%prefix%/%topic%/");
  assert.equal(sent[3].topic, "cmnd/new/Power/level");
  db.publish("new", {}, "%prefix%/%topic%/");
  assert.equal(sent[4].topic, "cmnd/new");
});

test("duplicate initialization fails and deinit disconnects publishing", () => {
  const db = new MqttDb();
  const sent = [];
  db.init((msg) => sent.push(msg));
  assert.throws(() => db.init(() => {}), /Only one/);
  db.publish("desk.POWER", 1, "%prefix%/%topic%/");
  assert.equal(sent.length, 1);
  db.deinit();
  assert.equal(db.publisher, null);
  assert.throws(() => db.publish("desk.POWER", 0), /No mqttdb subscriber/);
});

test("v3 persistence round trips tree data and converts prefixed values", (t) => {
  const file = temporary(t);
  fs.writeFileSync(
    file,
    JSON.stringify({
      version: 3,
      tree: {
        stat: {
          route: "%prefix%/%topic%/",
          value: {},
          children: { POWER: { value: "OFF" } },
        },
        generic: {
          route: "devices/%topic%/",
          value: {},
          children: {
            POWER: { value: "OFF" },
            bool: { value: true },
            nested: {
              route: "%prefix%/%topic%/",
              value: {},
              children: { POWER: { value: "OFF" } },
            },
          },
        },
      },
    }),
  );
  const db = new MqttDb();
  db.full_topics = MqttDb.patterns(["%prefix%/%topic%/"]);
  db.load(file);
  assert.equal(db.get("stat.POWER"), 0);
  assert.equal(db.get("generic.POWER"), "OFF");
  assert.equal(db.get("generic.bool"), true);
  assert.equal(db.get("generic.nested.POWER"), 0);
  assert.equal(db.receive("other/zb/X", 1), true);
  assert.equal(db.receive("weather", 21), true);
  db.dump(file);
  const target = new MqttDb();
  target.load(file);
  assert.deepEqual(target.data(), db.data());
  assert.equal(target.get("other.zb.X"), 1);
  assert.equal(target.get("weather"), 21);
  assert.equal(fs.existsSync(file + ".tmp"), false);
  assert.equal(JSON.parse(fs.readFileSync(file)).version, 3);
});

test("older database formats are archived and discarded", (t) => {
  const file = temporary(t);
  const source = { version: 2, devices: { desk: { POWER: 1 } } };
  fs.writeFileSync(file, JSON.stringify(source));
  fs.writeFileSync(file + ".v2", "replace me");
  const db = new MqttDb();
  db.receive("stat/current/POWER", 1);
  db.load(file);
  assert.equal(fs.existsSync(file), false);
  assert.deepEqual(JSON.parse(fs.readFileSync(file + ".v2")), source);
  assert.deepEqual(db.data(), {});
});

test("invalid database formats are rejected", (t) => {
  const file = temporary(t);
  const db = new MqttDb();
  for (const source of [{ version: 3, tree: { desk: { POWER: "OFF" } } }, { desk: { POWER: "OFF" } }]) {
    fs.writeFileSync(file, JSON.stringify(source));
    assert.throws(() => db.load(file), /Unsupported|Invalid MQTT tree node/);
  }
});

test("malformed persistence leaves existing memory and original file intact", (t) => {
  const file = temporary(t);
  const { db } = setup();
  db.receive("stat/desk/POWER", 1);
  fs.writeFileSync(file, "{invalid");
  assert.throws(() => db.load(file));
  assert.equal(fs.readFileSync(file, "utf8"), "{invalid");
  assert.equal(db.get("desk.POWER"), 1);
});

function streamPair(body) {
  const req = new EventEmitter();
  req.body = body;
  const res = new EventEmitter();
  res.headers = {};
  res.lines = [];
  res.destroyed = false;
  res.setHeader = (k, v) => {
    res.headers[k] = v;
  };
  res.write = (line) => {
    res.lines.push(JSON.parse(line));
    return true;
  };
  res.end = (body) => {
    res.body = body;
    res.writableEnded = true;
    res.emit("finish");
  };
  res.destroy = () => {
    res.destroyed = true;
    res.emit("close");
  };
  return { req, res };
}

test("HTTP uses the same whole/leaf semantics, deduplicates selectors and cleans up", async () => {
  const { db } = setup();
  const { req, res } = streamPair({ topics: ["desk", "desk.#", "desk"] });
  await db.stream(req, res);
  assert.equal(subscriptionCount(db), 2);
  db.receive("stat/desk/RESULT", { POWER: "ON", nested: { flag: "FALSE" } });
  const updateTs = db.get("desk._update_ts");
  assert.deepEqual(res.lines, [
    { topic: "desk", value: { Power: 1, nested: { flag: 0 }, _update_ts: updateTs }, acked: true },
    { topic: "desk.Power", value: 1, acked: true },
    { topic: "desk.nested.flag", value: 0, acked: true },
    { topic: "desk._update_ts", value: updateTs, acked: true },
  ]);
  db.remove("desk");
  assert.equal(res.lines.length, 4);
  res.emit("close");
  assert.equal(subscriptionCount(db), 0);
});

test("HTTP rejects invalid selectors before installing any subscription", async () => {
  const { db } = setup();
  for (
    const body of [{ topics: ["desk", "desk.#.a"] }, [], "not JSON", {
      topics: [""],
    }]
  ) {
    const { req, res } = streamPair(body);
    await db.stream(req, res);
    assert.equal(res.statusCode, 400);
    assert.equal(subscriptionCount(db), 0);
  }
});

test("HTTP pauses writes on backpressure, resumes on drain and bounds the queue", async () => {
  const { db } = setup();
  const { req, res } = streamPair(["desk.POWER"]);
  let calls = 0;
  res.write = (line) => {
    calls++;
    res.lines.push(JSON.parse(line));
    return false;
  };
  await db.stream(req, res);
  db.receive("stat/desk/POWER", 1);
  db.receive("stat/desk/POWER", 2);
  assert.equal(calls, 1);
  res.emit("drain");
  assert.equal(calls, 2);
  for (let i = 0; i < 258; i++) db.receive("stat/desk/POWER", i);
  assert.equal(res.destroyed, true);
  assert.equal(subscriptionCount(db), 0);
});
