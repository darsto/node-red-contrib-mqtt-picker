const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const MqttDb = require("./db");

const collect = () => {
  const calls = [];
  return {
    calls,
    cb: (topic, value, acked) => calls.push({ topic, value, acked }),
  };
};

test("basic", () => {
  const db = new MqttDb();

  db.update("aa.bb", "val1");
  db.update("aa/cc", "val2");
  db.update("aa.dd.11", "val3");

  assert.equal(db.get("aa.bb"), "val1");
  assert.equal(db.get("aa.cc"), "val2");
  assert.equal(db.get("aa.dd.11"), "val3");
  assert.deepEqual(db.data, {
    aa: {
      bb: "val1",
      cc: "val2",
      dd: { 11: "val3" },
      _update_ts: db.data.aa._update_ts,
    },
  });
});

test("basic 2", () => {
  const db = new MqttDb();

  db.update("aa.bb", "ignored", false);
  assert.equal(db.get("aa.bb"), undefined);

  db.update("aa.bb", "created");
  db.update("aa.cc", "ignored", false);
  db.update("aa.bb", "changed", false);

  assert.equal(db.get("aa.cc"), undefined);
  assert.equal(db.get("aa.bb"), "changed");
});

test("tracks acknowledged updates on object-valued roots", () => {
  const db = new MqttDb();

  db.update("aa.bb", "first", true, false);
  assert.equal(db.data.aa._update_ts, undefined);

  db.update("aa.bb", "second", true, true);
  assert.equal(typeof db.data.aa._update_ts, "number");

  db.data.aa._update_ts = 1;
  db.update("aa.cc", "third", true, true);
  assert.ok(db.data.aa._update_ts > 1);

  const timestamp = db.data.aa._update_ts;
  db.update("aa.dd", "fourth", true, false);
  assert.equal(db.data.aa._update_ts, timestamp);

  db.update("single", "value", true, true);
  assert.equal(db.data.single, "value");
});

test("merge updates", () => {
  const db = new MqttDb();

  db.update("aa.dd.44", { a: 43, nested: { one: 1 } });
  db.update("aa.dd.44", { b: 45, nested: { two: 2 } });

  assert.deepEqual(db.get("aa.dd.44"), {
    a: 43,
    b: 45,
    nested: { one: 1, two: 2 },
  });
});

test("object updates replace incompatible values and preserve arrays and null", () => {
  const db = new MqttDb();

  db.update("aa.value", "old");
  db.update("aa.value", { nested: true });
  db.update("aa.list", [1, 2]);
  db.update("aa.empty", null);

  assert.deepEqual(db.data, {
    aa: {
      value: { nested: true },
      list: [1, 2],
      empty: null,
      _update_ts: db.data.aa._update_ts,
    },
  });
});

test("remove", () => {
  const db = new MqttDb();

  db.update("aa.bb.cc", "val");
  db.remove("aa.bb.cc");
  db.remove("aa.missing.cc");

  assert.equal(db.get("aa.bb.cc"), undefined);
  assert.deepEqual(db.get("aa.bb"), {});
});

test("subscribe", () => {
  const db = new MqttDb();
  const sub = collect();

  const cb = db.subscribe("aa.bb", sub.cb);
  db.update("aa.bb", "val", true, true);

  assert.equal(cb, sub.cb);
  assert.deepEqual(sub.calls, [
    { topic: "aa.bb", value: "val", acked: true },
  ]);
});

test("subscribe parent", () => {
  const db = new MqttDb();
  const sub = collect();

  db.subscribe("aa.dd", sub.cb);
  db.update("aa.dd.44", "val", true, false);

  assert.deepEqual(sub.calls, [
    { topic: "aa.dd.44", value: "val", acked: false },
  ]);
});

test("subscribe both leaf + parent", () => {
  const db = new MqttDb();
  const parent = collect();
  const child = collect();

  db.subscribe("aa.dd.44", parent.cb);
  db.subscribe("aa.dd.44.b", child.cb);
  db.update("aa.dd.44", { b: 45, c: 46 }, true, true);

  assert.deepEqual(parent.calls, [
    { topic: "aa.dd.44.b", value: 45, acked: true },
    { topic: "aa.dd.44.c", value: 46, acked: true },
  ]);
  assert.deepEqual(child.calls, [
    { topic: "aa.dd.44.b", value: 45, acked: true },
  ]);
});

test("subscribe missing fields", () => {
  const db = new MqttDb();
  const parent = collect();
  const nested = collect();

  db.subscribe("aa", parent.cb);
  db.subscribe("aa.obj.inner", nested.cb);
  db.update("aa.obj", { inner: { leaf: "value" }, other: "x" }, true, true);

  assert.deepEqual(parent.calls, [
    { topic: "aa.obj.inner", value: { leaf: "value" }, acked: true },
    { topic: "aa.obj.inner.leaf", value: "value", acked: true },
    { topic: "aa.obj.other", value: "x", acked: true },
  ]);
  assert.deepEqual(nested.calls, [
    { topic: "aa.obj.inner", value: { leaf: "value" }, acked: true },
    { topic: "aa.obj.inner.leaf", value: "value", acked: true },
  ]);
});

test("unsubscribe", () => {
  const db = new MqttDb();
  const first = collect();
  const second = collect();

  db.subscribe("aa.bb", first.cb);
  db.subscribe("aa.bb", second.cb);
  db.unsubscribe("aa.bb", first.cb);
  db.update("aa.bb", "val");

  assert.deepEqual(first.calls, []);
  assert.deepEqual(second.calls, [
    { topic: "aa.bb", value: "val", acked: true },
  ]);
});

test("subscribe + query", () => {
  const db = new MqttDb();
  const root = collect();
  const topic = collect();

  db.subs.cb.push(root.cb);
  db.subscribe("aa.bb", topic.cb);
  db.query("aa.bb");

  assert.deepEqual(root.calls, [
    { topic: "aa.bb", value: "", acked: false },
  ]);
  assert.deepEqual(topic.calls, []);
});

test("normalizes MQTT prefixes and separators", () => {
  assert.equal(MqttDb.normalize_topic("stat/device/POWER"), "device.POWER");
  assert.equal(MqttDb.normalize_topic("tele.device.STATE"), "device.STATE");
  assert.equal(MqttDb.normalize_topic("cmnd/device/POWER"), "device.POWER");
  assert.equal(MqttDb.normalize_topic("device/POWER"), "device.POWER");
  assert.equal(MqttDb.normalize_topic("stat/stat/device"), "stat.device");
});

test("identifies only the exact tasmota discovery namespace", () => {
  assert.equal(MqttDb.is_discovery_topic("tasmota/discovery"), true);
  assert.equal(MqttDb.is_discovery_topic("tasmota/discovery/device/config"), true);
  assert.equal(MqttDb.is_discovery_topic("tasmota.discovery.device.sensors"), true);
  assert.equal(MqttDb.is_discovery_topic("Tasmota/discovery/device"), false);
  assert.equal(MqttDb.is_discovery_topic("other/tasmota/discovery"), false);
  assert.equal(MqttDb.is_discovery_topic("tasmota/discovery2"), false);
});

test("collapses matching numbered INFO wrappers", () => {
  assert.equal(
    MqttDb.normalize_topic("tele/device/INFO1/Info1/Version"),
    "device.INFO1.Version",
  );
  assert.equal(
    MqttDb.normalize_topic("tele/device/INFO1/Info2/Version"),
    "device.INFO1.Info2.Version",
  );
  assert.equal(
    MqttDb.normalize_topic("device/STATE/State/value"),
    "device.STATE.State.value",
  );
  assert.deepEqual(
    MqttDb.collapse_info_payload("device.INFO1", {
      Info1: { Version: "1.0 tasmota" },
    }),
    { Version: "1.0 tasmota" },
  );
  assert.deepEqual(
    MqttDb.collapse_info_payload("device.INFO1", {
      Info2: { Version: "unchanged" },
    }),
    { Info2: { Version: "unchanged" } },
  );
});

test("migrates legacy roots with defined collision precedence", () => {
  const normalized = MqttDb.normalize_data({
    cmnd: {
      plug: { POWER: "cmnd", onlyCmnd: 1, nested: { cmnd: true } },
    },
    tele: {
      plug: { POWER: "tele", onlyTele: 2, nested: { tele: true } },
    },
    stat: {
      plug: { POWER: "stat", onlyStat: 3, nested: { stat: true } },
    },
    plug: {
      POWER: "prefixless",
      onlyDirect: 4,
      nested: { direct: true },
      INFO1: { Info1: { Version: "13.0.0(tasmota)" } },
    },
    tasmota: {
      discovery: { device: { config: "ignored" } },
      unrelated: "preserved",
    },
    other: { value: 5 },
  });

  assert.deepEqual(normalized, {
    plug: {
      POWER: "prefixless",
      onlyCmnd: 1,
      onlyTele: 2,
      onlyStat: 3,
      onlyDirect: 4,
      nested: { cmnd: true, tele: true, stat: true, direct: true },
      INFO1: { Version: "13.0.0(tasmota)" },
    },
    tasmota: { unrelated: "preserved" },
    other: { value: 5 },
  });
});

test("migration preserves mismatched INFO wrappers and removes empty discovery", () => {
  assert.deepEqual(
    MqttDb.normalize_data({
      tele: { sensor: { INFO1: { Info2: { Version: "kept" } } } },
      tasmota: { discovery: { nested: true } },
    }),
    { sensor: { INFO1: { Info2: { Version: "kept" } } } },
  );
});

test("dump and load preserve last update timestamps", (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mqtt-db-test-"));
  const file = path.join(directory, "mqttdb.json");
  t.after(() => fs.rmSync(directory, { recursive: true }));

  const source = new MqttDb();
  source.update("plug.POWER", "ON", true, true);
  source.dump(file);

  const target = new MqttDb();
  target.load(file);

  assert.deepEqual(target.data, source.data);
});

test("load applies legacy migration", (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "mqtt-db-test-"));
  const file = path.join(directory, "mqttdb.json");
  t.after(() => fs.rmSync(directory, { recursive: true }));
  fs.writeFileSync(file, JSON.stringify({
    cmnd: { plug: { POWER: "command" } },
    stat: { plug: { POWER: "state" } },
  }));

  const db = new MqttDb();
  db.load(file);

  assert.deepEqual(db.data, { plug: { POWER: "state" } });
});
