const assert = require("node:assert/strict");
const fs = require("node:fs");
const test = require("node:test");

eval(fs.readFileSync("resources/mqtt-tree.js") + "");

const ids = (nodes) => nodes.map((node) => node.id);

const find = (nodes, id) => {
  const stack = [...nodes];
  while (stack.length) {
    const node = stack.shift();
    if (node.id === id) {
      return node;
    }
    stack.push(...node.children);
  }
  return null;
};

test("displays the normalized database directly", () => {
  const tree = MqttTree.new({
    plug: {
      POWER: "ON",
      SENSOR: { Temperature: 21.5 },
      list: [1, 2],
      empty: null,
      off: false,
    },
    other: { leaf: "value" },
  });

  assert.deepEqual(ids(tree), ["plug", "other"]);
  assert.deepEqual(ids(find(tree, "plug").children), [
    "plug.POWER",
    "plug.SENSOR",
    "plug.list",
    "plug.empty",
    "plug.off",
  ]);
  assert.equal(find(tree, "plug.POWER").value, "ON");
  assert.equal(find(tree, "plug.SENSOR.Temperature").value, 21.5);
  assert.deepEqual(find(tree, "plug.list").value, [1, 2]);
  assert.equal(find(tree, "plug.empty").value, null);
  assert.equal(find(tree, "plug.off").value, false);
  assert.equal(find(tree, "other.leaf").value, "value");
});

test("does not add prefix branches or writable markers", () => {
  const tree = MqttTree.new({ device: { POWER: "OFF" } });

  assert.deepEqual(ids(tree), ["device"]);
  assert.equal(find(tree, "device.POWER").writable, undefined);
  assert.equal(find(tree, "stat"), null);
  assert.equal(find(tree, "tele"), null);
  assert.equal(find(tree, "cmnd"), null);
});

test("handles non-object input as an empty tree", () => {
  assert.deepEqual(MqttTree.new(null), []);
  assert.deepEqual(MqttTree.new("invalid"), []);
});
