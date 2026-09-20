const assert = require("node:assert/strict");
const test = require("node:test");
const MqttTree = require("../resources/mqtt-tree");
const MqttTopicParser = require("../resources/mqtt-topic-parser");

test("shared paths round trip unusual device/property names and terminal wildcards", () => {
  for (const parts of [["desk", "SENSOR", "Temperature"], ["a.b", "sensor.v1", "a/b", "\\#", ""], ["__proto__", "constructor"]]) {
    const path = MqttTopicParser.format(parts);
    assert.deepEqual(MqttTopicParser.parse(path), { parts, wildcard: false });
    assert.deepEqual(MqttTopicParser.parse(path + ".#", true), { parts, wildcard: true });
  }
  assert.deepEqual(MqttTopicParser.parse("#", true), { parts: [], wildcard: true });
  assert.equal(MqttTopicParser.format([], true), "#");
  assert.throws(() => MqttTopicParser.parse("#"));
  for (const invalid of ["", "desk.#.x", "desk.a#b", "desk\\x", "desk\\"]) {
    assert.throws(() => MqttTopicParser.parse(invalid, true));
  }
});

test("picker uses opaque IDs and shared escaped paths instead of raw selector text", () => {
  const values = { 'a.b"[]': { "sensor.v1": { "#": 1 }, "state/temp": 20 } };
  const tree = MqttTree.new(values);
  const leaf = MqttTree.find(tree, MqttTopicParser.format(['a.b"[]', "sensor.v1", "#"]));
  assert.equal(leaf.value, 1);
  assert.match(leaf.id, /^mqtt-\d+$/);
  assert.equal(leaf.label, "#");
  assert.equal(MqttTree.find(tree, MqttTopicParser.format(['a.b"[]', "state/temp"])).value, 20);
});

test("picker displays JSON types distinctly and keeps arrays atomic", () => {
  const tree = MqttTree.new({ desk: { nil: null, zero: 0, off: false, empty: "", list: [1, 2], obj: {} } });
  const expected = { nil: "null", zero: "0", off: "false", empty: '""', list: "[1,2]", obj: "{}" };
  for (const [key, value] of Object.entries(expected)) assert.equal(MqttTree.display(MqttTree.find(tree, "desk." + key)), value);
  assert.equal(MqttTree.find(tree, "desk.list").children.length, 0);
  assert.equal(MqttTree.find(tree, "desk.obj").isObject, true);
});

test("picker allows nested generic topic destinations", () => {
  const tree = MqttTree.new({ zb: { b2: { Power: 2 } } });
  assert.equal(MqttTree.find(tree, "zb").selectable, true);
  assert.equal(MqttTree.find(tree, "zb.b2").selectable, true);
  assert.equal(MqttTree.find(tree, "zb.b2.Power").selectable, true);
  assert.deepEqual(MqttTree.new(null), []);
});
