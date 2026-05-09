const assert = require("node:assert/strict");
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
