const assert = require("node:assert/strict");
const test = require("node:test");
const setup = require("./test-helper");

function subscriptionCount(db) {
  let count = 0;
  const nodes = Object.values(db.tree);
  for (const node of nodes) {
    count += node.subscribers.size;
    nodes.push(...Object.values(node.children));
  }
  return count;
}

test("cached call returns a detached whole object without publication or input notifications", async () => {
  const { db, bridge, make } = setup();
  bridge.handlers.input({ topic: "stat/desk/RESULT", payload: { POWER: "OFF", SENSOR: { Temperature: 21 } } });
  const input = make("in", { topic: "desk" });
  const call = make("call", { topic: "desk" });
  await call.handlers.input({ _msgid: "test" });
  const updateTs = db.get("desk._update_ts");
  assert.deepEqual(call.sent, [{ _msgid: "test", topic: "desk", payload: { Power: 0, Sensor: { Temperature: 21 }, _update_ts: updateTs } }]);
  assert.equal(input.sent.length, 0);
  assert.equal(bridge.sent.length, 0);
  call.sent[0].payload.Power = 55;
  assert.equal(db.get("desk.POWER"), 0);
});

test("active query subscribes before a synchronous response and preserves normalized values", async () => {
  const { db, bridge, make } = setup("tasmota/%topic%/%prefix%/");
  bridge.handlers.input({ topic: "tasmota/desk/stat/POWER", payload: "OFF" });
  bridge.forward = (msg) => {
    bridge.handlers.input(msg); // Echo must not complete or mutate anything.
    assert.equal(db.get("tasmota.desk.POWER"), 0);
    bridge.handlers.input({ topic: "tasmota/desk/stat/RESULT", payload: { POWER: "ON" } });
  };
  const call = make("call", { topic: "tasmota.desk.POWER", requestlatest: true });
  await call.handlers.input({});
  assert.deepEqual(bridge.sent, [{ topic: "tasmota/desk/cmnd/Power", payload: "" }]);
  assert.deepEqual(call.sent, [{ topic: "tasmota.desk.POWER", payload: 1 }]);
  assert.equal(subscriptionCount(db), 0);
});

test("active query supports multi-level command suffixes", async () => {
  const { bridge, make } = setup("%prefix%/%topic%/");
  bridge.handlers.input({ topic: "stat/mydev/HVAC/Temperature", payload: 20 });
  bridge.forward = () => bridge.handlers.input({ topic: "stat/mydev/HVAC/Temperature", payload: 21 });
  const call = make("call", { topic: "mydev.HVAC.Temperature", requestlatest: true });
  await call.handlers.input({});
  assert.deepEqual(bridge.sent, [{ topic: "cmnd/mydev/Hvac/Temperature", payload: "" }]);
  assert.equal(call.sent[0].payload, 21);
});

test("active query ignores unrelated fields and returns a whole requested object", async () => {
  const { db, bridge, make } = setup();
  bridge.handlers.input({ topic: "tele/desk/SENSOR", payload: { Climate: { a: 1 } } });
  const call = make("call", { topic: "desk.Climate", requestlatest: true });
  const pending = call.handlers.input({});
  bridge.handlers.input({ topic: "stat/desk/RESULT", payload: { POWER: 0 } });
  await Promise.resolve();
  assert.equal(call.sent.length, 0);
  bridge.handlers.input({ topic: "tele/desk/SENSOR", payload: { Climate: { b: 2 } } });
  await pending;
  assert.deepEqual(call.sent[0].payload, { a: 1, b: 2 });
});

test("queries without prefix-bearing routes fail without publishing", async () => {
  const { db, bridge, make } = setup(["%prefix%/%topic%/", "sensors/%topic%/"]);
  bridge.handlers.input({ topic: "stat/desk/POWER", payload: 1 });
  bridge.handlers.input({ topic: "sensors/weather/temp", payload: 22 });
  for (const topic of ["weather.temp", "unknown.POWER"]) {
    const call = make("call", { topic, requestlatest: true });
    const errors = [];
    await call.handlers.input({}, undefined, (err) => errors.push(err));
    assert.equal(errors.length, 1);
    assert.match(errors[0].message, /Active queries/);
  }
  assert.equal(bridge.sent.length, 0);
});

test("concurrent queries for one path share the same reply", async () => {
  const { bridge, make } = setup();
  bridge.handlers.input({ topic: "stat/desk/POWER", payload: 0 });
  const first = make("call", { topic: "desk.POWER", requestlatest: true });
  const second = make("call", { topic: "desk.POWER", requestlatest: true });
  const pending = [first.handlers.input({}), second.handlers.input({})];
  assert.equal(bridge.sent.length, 2);
  bridge.handlers.input({ topic: "stat/desk/RESULT", payload: { POWER: 1 } });
  await Promise.all(pending);
  assert.equal(first.sent[0].payload, 1);
  assert.equal(second.sent[0].payload, 1);
});

test("timeout cleans up without inventing numbered-field aliases or changing state", async () => {
  const { db, bridge, make } = setup();
  bridge.handlers.input({ topic: "stat/desk/Temperature", payload: 20 });
  const call = make("call", { topic: "desk.Temperature", requestlatest: true, timeout: 10 });
  const pending = call.handlers.input({});
  bridge.handlers.input({ topic: "stat/desk/Temperature1", payload: 30 });
  await pending;
  assert.match(call.errors[0].message, /timed out/);
  assert.equal(call.sent.length, 0);
  assert.equal(db.get("desk.Temperature"), 20);
  assert.equal(subscriptionCount(db), 0);
});

test("node close cancels pending calls and prevents output after close", async () => {
  const { db, bridge, make } = setup();
  bridge.handlers.input({ topic: "stat/desk/POWER", payload: 0 });
  const call = make("call", { topic: "desk.POWER", requestlatest: true });
  const completed = [];
  const pending = call.handlers.input({}, undefined, (err) => completed.push(err));
  call.handlers.close();
  await pending;
  assert.equal(call.sent.length, 0);
  assert.match(completed[0].message, /cancelled/);
  assert.equal(completed.length, 1);
  assert.equal(subscriptionCount(db), 0);
});

test("flow shutdown aborts pending calls and deinitializes publishing", async () => {
  const { db, bridge, make } = setup();
  bridge.handlers.input({ topic: "stat/desk/POWER", payload: 0 });
  const call = make("call", { topic: "desk.POWER", requestlatest: true });
  const pending = call.handlers.input({});
  bridge.handlers.close(false, () => {});
  call.handlers.close();
  await pending;
  assert.equal(db.publisher, null);
  assert.equal(subscriptionCount(db), 0);
});

test("publish failure also cleans up the query subscription and timer", async () => {
  const { db, bridge, make } = setup();
  bridge.handlers.input({ topic: "stat/desk/POWER", payload: 0 });
  db.publisher = () => { throw new Error("send failed"); };
  const call = make("call", { topic: "desk.POWER", requestlatest: true });
  await call.handlers.input({});
  assert.match(call.errors[0].message, /send failed/);
  assert.equal(subscriptionCount(db), 0);
});

test("invalid timeout is reported instead of silently replaced", async () => {
  const { bridge, make } = setup();
  bridge.handlers.input({ topic: "stat/desk/POWER", payload: 0 });
  const call = make("call", { topic: "desk.POWER", requestlatest: true, timeout: 0 });
  await call.handlers.input({});
  assert.match(call.errors[0].message, /timeout must be positive/);
  assert.equal(bridge.sent.length, 0);
});

test("missing cached values follow noexist behavior and attributes are safe", async () => {
  const { make } = setup();
  const nothing = make("call", { topic: "missing", noexist: "nothing" });
  await nothing.handlers.input({});
  assert.equal(nothing.sent.length, 0);
  const error = make("call", { topic: "missing", noexist: "error" });
  await error.handlers.input({});
  assert.match(error.errors[0].message, /does not exist/);
  const undef = make("call", { topic: "missing", noexist: "undef", attr: "__proto__" });
  await undef.handlers.input({});
  assert.equal(undef.sent.length, 1);
  assert.equal(Object.getPrototypeOf(undef.sent[0]), Object.prototype);
  assert.equal(Object.hasOwn(undef.sent[0], "__proto__"), true);
});
