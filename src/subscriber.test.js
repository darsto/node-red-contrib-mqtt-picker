const assert = require("node:assert/strict");
const test = require("node:test");
const setup = require("./test-helper");

test("bridge/input/output work together with a custom Full Topic", () => {
  const { db, bridge, make } = setup("tasmota/%topic%/%prefix%/");
  const input = make("in", { topic: "tasmota.desk" });
  const leaves = make("in", { topic: "tasmota.desk.#" });
  bridge.handlers.input({ topic: "tasmota/desk/stat/RESULT", payload: { POWER: "off", nested: { active: "TRUE" } } });
  const updateTs = db.get("tasmota.desk._update_ts");
  assert.deepEqual(input.sent, [{ topic: "tasmota.desk", payload: { Power: 0, nested: { active: 1 }, _update_ts: updateTs } }]);
  assert.deepEqual(leaves.sent, [
    { topic: "tasmota.desk.Power", payload: 0 }, { topic: "tasmota.desk.nested.active", payload: 1 },
    { topic: "tasmota.desk._update_ts", payload: updateTs },
  ]);
  const output = make("out", { topic: "tasmota.desk.POWER" });
  const completed = [];
  output.handlers.input({ payload: "ON" }, undefined, (err) => completed.push(err));
  assert.deepEqual(bridge.sent, [{ topic: "tasmota/desk/cmnd/Power", payload: "ON" }]);
  bridge.handlers.input(bridge.sent[0]);
  assert.equal(db.get("tasmota.desk.POWER"), 0);
  assert.equal(input.sent.length, 1);
  assert.deepEqual(completed, [undefined]);
});

test("the legacy default also accepts generic topic trees", () => {
  const { db, bridge } = setup("%prefix%/%topic%/");
  bridge.handlers.input({ topic: "zb/b2", payload: { Device: "0x1DA8", Power: 2 } });
  assert.deepEqual(db.get("zb.b2"), { Device: "0x1DA8", Power: 2 });
});

test("generic wildcard inputs match case-insensitive topics", () => {
  const { db, bridge, make } = setup();
  const input = make("in", { topic: "zb.#" });
  bridge.handlers.input({ topic: "ZB/p1", payload: { POWER: 1 } });
  const updateTs = db.get("zb._update_ts");
  assert.deepEqual(input.sent, [
    { topic: "zb.p1.Power", payload: 1 },
    { topic: "zb._update_ts", payload: updateTs },
  ]);
});

test("generic JSON output uses an explicit pattern without changing the database", () => {
  const { db, bridge, make } = setup("devices/%topic%/");
  const output = make("out", { topic: "fan.set", fullTopic: "devices/%topic%/" });
  output.handlers.input({ payload: { speed: 2, mode: "auto" } });
  assert.deepEqual(bridge.sent, [{ topic: "devices/fan/set", payload: { speed: 2, mode: "auto" } }]);
  assert.equal(db.get("fan"), undefined);
  const input = make("in", { topic: "devices.fan" });
  bridge.handlers.input({ topic: "devices/fan/state", payload: { power: "ON", speed: 2 } });
  assert.equal(input.sent[0].payload.state.power, "ON");
});

test("duplicate bridge error does not cause duplicate sends or detach the original", () => {
  const { bridge, make } = setup();
  const duplicate = make("subscriber", {});
  assert.match(duplicate.errors[0].message, /Only one/);
  duplicate.handlers.close(false, () => {});
  make("out", { topic: "desk.POWER", fullTopic: "%prefix%/%topic%/" }).handlers.input({ payload: 1 });
  assert.equal(bridge.sent.length, 1);
  assert.equal(duplicate.sent.length, 0);
});

test("nested generic publishes and invalid input configuration reach Node-RED", () => {
  const { bridge, make } = setup("%topic%/");
  bridge.handlers.input({ topic: "desk/state", payload: { power: 1 } });
  const output = make("out", { topic: "desk.state.power" });
  const errors = [];
  output.handlers.input({ payload: 0 }, undefined, (err) => errors.push(err));
  assert.deepEqual(errors, [undefined]);
  assert.deepEqual(bridge.sent, [{ topic: "desk/state/power", payload: 0 }]);
  const invalid = make("in", { topic: "desk.#.power" });
  assert.equal(invalid.errors.length, 1);
});

test("manual removal is silent through Node-RED and closed inputs unsubscribe", () => {
  const { db, bridge, make } = setup();
  const input = make("in", { topic: "desk" });
  const leaf = make("in", { topic: "desk.#" });
  bridge.handlers.input({ topic: "stat/desk/RESULT", payload: { POWER: 1, obj: { a: 2 } } });
  db.remove("desk.obj");
  db.remove("desk.POWER");
  assert.equal(input.sent.length, 1);
  assert.equal(leaf.sent.length, 3);
  input.handlers.close();
  leaf.handlers.close();
});

test("subscriber rejects a non-string MQTT topic", () => {
  const { bridge } = setup();
  const completed = [];
  bridge.handlers.input({ topic: 42, payload: 1 }, undefined, (err) => completed.push(err));
  assert.match(completed[0].message, /topic must be a string/);
});

test("invalid payload produces a diagnostic without modifying received state", () => {
  const { db, bridge } = setup();
  bridge.handlers.input({ topic: "stat/desk/POWER", payload: 1 });
  const completed = [];
  bridge.handlers.input({ topic: "stat/desk/RESULT", payload: Buffer.from("bad") }, undefined, (err) => completed.push(err));
  assert.match(completed[0].message, /decoded JSON/);
  assert.equal(db.get("desk.POWER"), 1);
});
